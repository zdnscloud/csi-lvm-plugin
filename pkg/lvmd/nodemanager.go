package lvmd

import (
	"context"
	"errors"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"
	"github.com/zdnscloud/gok8s/controller"
	"github.com/zdnscloud/gok8s/event"
	"github.com/zdnscloud/gok8s/handler"
	"github.com/zdnscloud/gok8s/predicate"
)

var (
	ErrUnknownNode          = errors.New("unknown node")
	ErrNoEnoughFreeSpace    = errors.New("no node with enough size")
	syncLVMFreeSizeInterval = 30 * time.Second
	lvmdPort                = "1736"
	ConnectTimeout          = 3 * time.Second
)

const (
	ZkeStorageLabel     = "node-role.kubernetes.io/storage"
	ZkeInternalIPAnnKey = "zdnscloud.cn/internal-ip"
)

type Node struct {
	Name     string
	Addr     string
	Size     uint64
	FreeSize uint64
}

type NodeManager struct {
	stopCh chan struct{}
	cache  cache.Cache
	vgName string

	lock  sync.Mutex
	nodes []*Node
}

func NewNodeManager(c cache.Cache, vgName string) *NodeManager {
	ctrl := controller.New("lvmNodeManager", c, scheme.Scheme)
	ctrl.Watch(&corev1.Node{})

	stopCh := make(chan struct{})
	a := &NodeManager{
		stopCh: stopCh,
		cache:  c,
		vgName: vgName,
	}
	a.initNodes()
	go ctrl.Start(stopCh, a, predicate.NewIgnoreUnchangedUpdate())
	//go a.syncLVMFreeSize()
	return a
}

func (m *NodeManager) initNodes() {
	var nl corev1.NodeList
	err := m.cache.List(context.TODO(), &client.ListOptions{}, &nl)
	if err != nil {
		log.Fatalf("get all the statefulsets failed:%s", err.Error())
	}

	for _, n := range nl.Items {
		m.addNode(&n, false)
	}
}

func (m *NodeManager) Allocate(name string, size uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	n := m.getNode(name)
	if n == nil {
		return ErrUnknownNode
	} else if n.FreeSize < size {
		return ErrNoEnoughFreeSpace
	}

	n.FreeSize -= size
	return nil
}

func (m *NodeManager) GetNodes() []Node {
	m.lock.Lock()
	defer m.lock.Unlock()

	var nodes []Node
	for _, n := range m.nodes {
		nodes = append(nodes, *n)
	}
	return nodes
}

func (m *NodeManager) getNode(name string) *Node {
	for _, n := range m.nodes {
		if n.Name == name {
			return n
		}
	}
	return nil
}

func (m *NodeManager) GetNodesHasFreeSize(size uint64) []string {
	var candidates []string
	for _, node := range m.nodes {
		if node.FreeSize > size {
			candidates = append(candidates, node.Name)
		}
	}
	return candidates
}

func (m *NodeManager) Release(name string, size uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	n := m.getNode(name)
	if n == nil {
		return ErrUnknownNode
	}

	n.FreeSize += size
	return nil
}

func (m *NodeManager) OnCreate(e event.CreateEvent) (handler.Result, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.addNode(e.Object.(*corev1.Node), true)
	return handler.Result{}, nil
}

func (m *NodeManager) addNode(n *corev1.Node, checkExists bool) {
	v, ok := n.Labels[ZkeStorageLabel]
	if ok && v == "true" {
		addr := n.Annotations[ZkeInternalIPAnnKey]
		if checkExists {
			old := m.getNode(n.Name)
			if old != nil {
				log.Warnf("node %s add more than once", n.Name)
				return
			}
		}

		size, freeSize, err := m.getSizeInVG(addr + ":" + lvmdPort)
		if err == nil {
			log.Debugf("add node %s with cap %v", n.Name, freeSize)
			m.nodes = append(m.nodes, &Node{
				Name:     n.Name,
				Addr:     addr,
				Size:     size,
				FreeSize: freeSize,
			})
		}
	}
}

func (m *NodeManager) OnUpdate(e event.UpdateEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (m *NodeManager) OnDelete(e event.DeleteEvent) (handler.Result, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	node := e.Object.(*corev1.Node)
	v, ok := node.Labels[ZkeStorageLabel]
	if ok && v == "true" {
		m.deleteNode(node.Name)
	}
	return handler.Result{}, nil
}

func (m *NodeManager) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (m *NodeManager) deleteNode(name string) {
	for i, n := range m.nodes {
		if n.Name == name {
			m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
			return
		}
	}

	log.Warnf("deleted storage node %s is unknown", name)
}

func (m *NodeManager) getSizeInVG(addr string) (uint64, uint64, error) {
	conn, err := NewLVMConnection(addr, ConnectTimeout)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	return conn.GetSizeOfVG(context.TODO(), m.vgName)
}
