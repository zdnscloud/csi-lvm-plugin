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
	ZkeStorageLabel      = "storage.zcloud.cn/storagetype"
	ZkeStorageLabelValue = "Lvm"
	ZkeInternalIPAnnKey  = "zdnscloud.cn/internal-ip"
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
		if isStorageNode(&n) {
			m.addNode(&n, false)
		}
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
	m.lock.Lock()
	defer m.lock.Unlock()

	var candidates []string
	for _, n := range m.nodes {
		if n.FreeSize > size {
			candidates = append(candidates, n.Name)
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
	n := e.Object.(*corev1.Node)
	if isStorageNode(n) && isNodeReady(n) {
		m.lock.Lock()
		m.addNode(n, true)
		m.lock.Unlock()
	}
	return handler.Result{}, nil
}

func (m *NodeManager) addNode(n *corev1.Node, checkExists bool) {
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
	} else {
		log.Errorf("get storage size of node %s(%s) failed:%s", n.Name, addr, err.Error())
	}
}

func (m *NodeManager) OnUpdate(e event.UpdateEvent) (handler.Result, error) {
	old := e.ObjectOld.(*corev1.Node)
	newNode := e.ObjectNew.(*corev1.Node)
	if isStorageNode(newNode) {
		isOldReady := isNodeReady(old)
		isNewReady := isNodeReady(newNode)
		if isOldReady != isNewReady {
			m.lock.Lock()
			if isNewReady {
				log.Debugf("detected node %s restore to ready", newNode.Name)
				m.addNode(newNode, true)
			} else {
				log.Debugf("detected node %s became unready", newNode.Name)
				m.deleteNode(newNode.Name)
			}
			m.lock.Unlock()
		}
	}

	return handler.Result{}, nil
}

func (m *NodeManager) OnDelete(e event.DeleteEvent) (handler.Result, error) {
	n := e.Object.(*corev1.Node)
	if isStorageNode(n) {
		m.lock.Lock()
		m.deleteNode(n.Name)
		m.lock.Unlock()
	}
	return handler.Result{}, nil
}

func (m *NodeManager) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (m *NodeManager) deleteNode(name string) {
	for i, n := range m.nodes {
		if n.Name == name {
			log.Warnf("deleted node %s ", name)
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

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady &&
			cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isStorageNode(n *corev1.Node) bool {
	v, ok := n.Labels[ZkeStorageLabel]
	return ok && v == ZkeStorageLabelValue
}
