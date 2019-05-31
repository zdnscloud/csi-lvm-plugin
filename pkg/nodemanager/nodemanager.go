package nodemanager

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
	lvmdclient "github.com/zdnscloud/lvmd/client"
	pb "github.com/zdnscloud/lvmd/proto"
)

var (
	ErrVGNotExist           = errors.New("vg doesn't exist")
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

type VGSizeGetter func(string, string) (uint64, uint64, error)

type NodeManager struct {
	stopCh chan struct{}
	cache  cache.Cache
	vgName string

	lock         sync.Mutex
	nodes        []*Node
	vgSizeGetter VGSizeGetter //for test
}

func New(c cache.Cache, vgName string) *NodeManager {
	ctrl := controller.New("lvmNodeManager", c, scheme.Scheme)
	ctrl.Watch(&corev1.Node{})

	stopCh := make(chan struct{})
	a := &NodeManager{
		stopCh:       stopCh,
		cache:        c,
		vgName:       vgName,
		vgSizeGetter: getSizeInVG,
	}
	a.initNodes()
	go ctrl.Start(stopCh, a, predicate.NewIgnoreUnchangedUpdate())
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
			m.AddNode(&n)
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
		if n.Size == 0 {
			m.fetchNodeInfo(n)
		}
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
		if n.Size == 0 {
			m.fetchNodeInfo(n)
		}

		if n.FreeSize >= size {
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
		m.AddNode(n)
	}
	return handler.Result{}, nil
}

func (m *NodeManager) fetchNodeInfo(n *Node) {
	size, freeSize, err := m.vgSizeGetter(n.Addr+":"+lvmdPort, m.vgName)
	if err == nil {
		n.Size = size
		n.FreeSize = freeSize
	} else {
		log.Errorf("get node %s cap failed:%s", n.Name, err.Error())
	}
}

func (m *NodeManager) AddNode(k8snode *corev1.Node) {
	m.lock.Lock()
	defer m.lock.Unlock()

	node := &Node{
		Name: k8snode.Name,
		Addr: k8snode.Annotations[ZkeInternalIPAnnKey],
	}
	if old := m.getNode(node.Name); old != nil {
		return
	}

	m.fetchNodeInfo(node)
	m.nodes = append(m.nodes, node)
	log.Debugf("add node %s with cap %v", node.Name, node.FreeSize)
}

func (m *NodeManager) OnUpdate(e event.UpdateEvent) (handler.Result, error) {
	old := e.ObjectOld.(*corev1.Node)
	newNode := e.ObjectNew.(*corev1.Node)
	if isStorageNode(newNode) {
		isOldReady := isNodeReady(old)
		isNewReady := isNodeReady(newNode)
		if isOldReady != isNewReady {
			if isNewReady {
				log.Debugf("detected node %s restore to ready", newNode.Name)
				m.AddNode(newNode)
			} else {
				log.Debugf("detected node %s became unready", newNode.Name)
				m.DeleteNode(newNode.Name)
			}
		}
	}

	return handler.Result{}, nil
}

func (m *NodeManager) OnDelete(e event.DeleteEvent) (handler.Result, error) {
	n := e.Object.(*corev1.Node)
	if isStorageNode(n) {
		m.DeleteNode(n.Name)
	}
	return handler.Result{}, nil
}

func (m *NodeManager) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (m *NodeManager) DeleteNode(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for i, n := range m.nodes {
		if n.Name == name {
			log.Warnf("deleted node %s ", name)
			m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
			return
		}
	}

	log.Warnf("deleted storage node %s is unknown", name)
}

func getSizeInVG(addr, vgName string) (uint64, uint64, error) {
	conn, err := lvmdclient.New(addr, ConnectTimeout)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	req := pb.ListVGRequest{}
	rsp, err := conn.ListVG(context.TODO(), &req)
	if err != nil {
		return 0, 0, err
	}

	for _, vg := range rsp.VolumeGroups {
		if vg.Name == vgName {
			return vg.Size, vg.FreeSize, nil
		}
	}
	return 0, 0, ErrVGNotExist
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
