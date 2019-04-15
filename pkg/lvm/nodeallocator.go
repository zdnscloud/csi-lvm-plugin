package lvm

import (
	"context"
	"errors"
	"sort"
	"sync"

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

type NodeByFreeSize []*Node

func (n NodeByFreeSize) Len() int           { return len(n) }
func (n NodeByFreeSize) Less(i, j int) bool { return n[i].FreeSize < n[j].FreeSize }
func (n NodeByFreeSize) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

type NodeAllocator struct {
	stopCh chan struct{}
	cache  cache.Cache
	vgName string

	lock  sync.Mutex
	nodes NodeByFreeSize
}

func NewNodeAllocator(c cache.Cache, vgName string) *NodeAllocator {
	ctrl := controller.New("nodeAllocator", c, scheme.Scheme)
	ctrl.Watch(&corev1.Node{})

	stopCh := make(chan struct{})
	sc := &NodeAllocator{
		stopCh: stopCh,
		cache:  c,
		vgName: vgName,
	}

	go ctrl.Start(stopCh, sc, predicate.NewIgnoreUnchangedUpdate())
	return sc
}

func (a *NodeAllocator) AllocateNodeForRequest(size uint64) (node string, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	for _, n := range a.nodes {
		if n.FreeSize > size {
			n.FreeSize = n.FreeSize - size
			node = n.Id
			break
		}
	}

	if node != "" {
		sort.Sort(a.nodes)
	} else {
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
			sort.Sort(a.nodes)
			return
		}
	}
}

func (a *NodeAllocator) OnCreate(e event.CreateEvent) (handler.Result, error) {
	n := e.Object.(*corev1.Node)
	v, ok := n.Labels[ZkeStorageLabel]
	if ok && v == "true" {
		addr := n.Annotations[ZkeInternalIPAnnKey]
		freesize, err := a.getFreeSize(addr)
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
			*o = *n
			isKnownNode = true
			break
		}
	}

	if isKnownNode == false {
		a.nodes = append(a.nodes, n)
	}

	sort.Sort(a.nodes)
}

func (a *NodeAllocator) deleteNode(id string) {
	for i, n := range a.nodes {
		if n.Id == id {
			a.nodes = append(a.nodes[:i], a.nodes[i+1:]...)
			return
		}
	}
}

func (a *NodeAllocator) getFreeSize(addr string) (uint64, error) {
	conn, err := lvmd.NewLVMConnection(addr, connectTimeout)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.GetFreeSizeOfVG(context.TODO(), a.vgName)
}
