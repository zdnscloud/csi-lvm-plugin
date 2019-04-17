package lvm

import (
	"context"
	"errors"
	"sync"

	"github.com/zdnscloud/csi-lvm-plugin/logger"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/lvmd"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/controller"
	"github.com/zdnscloud/gok8s/event"
	"github.com/zdnscloud/gok8s/handler"
	"github.com/zdnscloud/gok8s/predicate"
)

var (
	ErrNoEnoughFreeSpace = errors.New("no node with enough size")
)

const (
	ZkeStorageLabel     = "node-role.kubernetes.io/storage"
	ZkeInternalIPAnnKey = "zdnscloud.cn/internal-ip"
)

type Node struct {
	Id       string
	Addr     string
	FreeSize uint64
}

type NodeAllocator struct {
	stopCh chan struct{}
	cache  cache.Cache
	vgName string

	lock  sync.Mutex
	nodes []*Node
	index int
}

func NewNodeAllocator(c cache.Cache, vgName string) *NodeAllocator {
	ctrl := controller.New("nodeAllocator", c, scheme.Scheme)
	ctrl.Watch(&corev1.Node{})

	stopCh := make(chan struct{})
	sc := &NodeAllocator{
		stopCh: stopCh,
		cache:  c,
		vgName: vgName,
		index:  0,
	}

	go ctrl.Start(stopCh, sc, predicate.NewIgnoreUnchangedUpdate())
	return sc
}

func (a *NodeAllocator) AllocateNodeForRequest(size uint64) (node string, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	nodeCount := len(a.nodes)
	for i := 0; i < nodeCount; i++ {
		n := a.nodes[a.index]
		a.index = (a.index + 1) % nodeCount
		if n.FreeSize > size {
			n.FreeSize = n.FreeSize - size
			node = n.Id
			break
		}
	}

	if node == "" {
		err = ErrNoEnoughFreeSpace
	}
	return
}

func (a *NodeAllocator) Release(size uint64, id string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	for _, n := range a.nodes {
		if n.Id == id {
			n.FreeSize += size
			return
		}
	}

	logger.Warn("unknown node %s to release volume", id)
}

func (a *NodeAllocator) OnCreate(e event.CreateEvent) (handler.Result, error) {
	n := e.Object.(*corev1.Node)
	v, ok := n.Labels[ZkeStorageLabel]
	if ok && v == "true" {
		addr := n.Annotations[ZkeInternalIPAnnKey]
		freesize, err := a.getFreeSize(addr + ":" + lvmdPort)
		if err == nil {
			a.lock.Lock()
			a.addNode(&Node{
				Id:       n.Name,
				Addr:     addr,
				FreeSize: freesize,
			})
			a.lock.Unlock()
		}
	}
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnUpdate(e event.UpdateEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnDelete(e event.DeleteEvent) (handler.Result, error) {
	n := e.Object.(*corev1.Node)
	v, ok := n.Labels[ZkeStorageLabel]
	if ok && v == "true" {
		a.lock.Lock()
		a.deleteNode(n.Name)
		a.lock.Unlock()
	}
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (a *NodeAllocator) addNode(n *Node) {
	isKnownNode := false
	for _, o := range a.nodes {
		if o.Id == n.Id {
			logger.Warn("node %s add more than once", n.Id)
			*o = *n
			isKnownNode = true
			break
		}
	}

	if isKnownNode == false {
		logger.Debug("add node %s with cap %v", n.Id, n.FreeSize)
		a.nodes = append(a.nodes, n)
	}
}

func (a *NodeAllocator) deleteNode(id string) {
	for i, n := range a.nodes {
		if n.Id == id {
			a.nodes = append(a.nodes[:i], a.nodes[i+1:]...)
			if a.index == len(a.nodes) {
				a.index = 0
			}
			return
		}
	}

	logger.Warn("deleted storage node %s is unknown", id)
}

func (a *NodeAllocator) getFreeSize(addr string) (uint64, error) {
	conn, err := lvmd.NewLVMConnection(addr, ConnectTimeout)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.GetFreeSizeOfVG(context.TODO(), a.vgName)
}
