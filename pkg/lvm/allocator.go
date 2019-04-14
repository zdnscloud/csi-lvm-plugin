package lvm

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/controller"
	"github.com/zdnscloud/gok8s/event"
	"github.com/zdnscloud/gok8s/handler"
	"github.com/zdnscloud/gok8s/predicate"
)

type NodeAllocator struct {
	stopCh chan struct{}
	cache  cache.Cache
}

func NewNodeAllocator(c cache.Cache) (*NodeAllocator, error) {
	ctrl := controller.New("nodeAllocator", c, scheme.Scheme)
	ctrl.Watch(&corev1.Node{})

	stopCh := make(chan struct{})
	sc := &NodeAllocator{
		stopCh: stopCh,
		cache:  c,
	}

	go ctrl.Start(stopCh, sc, predicate.NewIgnoreUnchangedUpdate())
	return sc, nil
}

func (a *NodeAllocator) AllocateNodeForRequest(size uint32) (string, error) {
	return "", nil
}

func (a *NodeAllocator) OnCreate(e event.CreateEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnUpdate(e event.UpdateEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnDelete(e event.DeleteEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}
