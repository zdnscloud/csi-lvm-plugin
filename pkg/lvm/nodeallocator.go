package lvm

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/zdnscloud/csi-lvm-plugin/pkg/lvmd"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
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
	ErrNoEnoughFreeSpace    = errors.New("no node with enough size")
	syncLVMFreeSizeInterval = 30 * time.Second
)

const (
	ZkeStorageLabel     = "node-role.kubernetes.io/storage"
	ZkeInternalIPAnnKey = "zdnscloud.cn/internal-ip"
)

type StatefulSet struct {
	Name          string
	Nodes         map[string]struct{}
	LVMVolumeName string
}

type StatefulSetList []*StatefulSet

type Node struct {
	Id       string
	Addr     string
	FreeSize uint64
}

type NodeAllocator struct {
	stopCh chan struct{}
	cache  cache.Cache
	vgName string

	lock         sync.Mutex
	nodes        []*Node
	statefulsets map[string]StatefulSetList
	knownPVC     map[k8stypes.UID]*StatefulSet
}

func NewNodeAllocator(c cache.Cache, vgName string) *NodeAllocator {
	rand.Seed(time.Now().Unix())

	ctrl := controller.New("nodeAllocator", c, scheme.Scheme)
	ctrl.Watch(&corev1.Node{})

	stopCh := make(chan struct{})
	a := &NodeAllocator{
		stopCh:       stopCh,
		cache:        c,
		vgName:       vgName,
		statefulsets: make(map[string]StatefulSetList),
		knownPVC:     make(map[k8stypes.UID]*StatefulSet),
	}
	a.initStatefulSet()
	a.initNodes()
	go ctrl.Start(stopCh, a, predicate.NewIgnoreUnchangedUpdate())
	//go a.syncLVMFreeSize()
	return a
}

func (a *NodeAllocator) initStatefulSet() {
	var sl appsv1.StatefulSetList
	err := a.cache.List(context.TODO(), &client.ListOptions{}, &sl)
	if err != nil {
		log.Fatalf("get all the statefulsets failed:%s", err.Error())
	}

	for _, ss := range sl.Items {
		a.addStatefulSet(&ss, false)
	}
}

func (a *NodeAllocator) initNodes() {
	var nl corev1.NodeList
	err := a.cache.List(context.TODO(), &client.ListOptions{}, &nl)
	if err != nil {
		log.Fatalf("get all the statefulsets failed:%s", err.Error())
	}

	for _, n := range nl.Items {
		a.addNode(&n, false)
	}
}

func (a *NodeAllocator) AllocateNodeForRequest(pvcUID string, size uint64) (node string, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	var candidate []string
	for _, node := range a.nodes {
		if node.FreeSize > size {
			candidate = append(candidate, node.Id)
		}
	}
	if len(candidate) == 0 {
		return "", ErrNoEnoughFreeSpace
	}

	ss, ok := a.knownPVC[k8stypes.UID(pvcUID)]
	if ok {
		for _, c := range candidate {
			if _, ok := ss.Nodes[c]; ok == false {
				return c, nil
			}
		}
	}

	return candidate[rand.Intn(len(candidate))], nil
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

	log.Warnf("unknown node %s to release volume", id)
}

func (a *NodeAllocator) OnCreate(e event.CreateEvent) (handler.Result, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	switch obj := e.Object.(type) {
	case *corev1.Node:
		a.addNode(obj, true)
	case *appsv1.StatefulSet:
		a.addStatefulSet(obj, true)
	case *corev1.Pod:
		if obj.Spec.NodeName != "" {
			a.onPodScheduledToNode(obj)
		}
	case *corev1.PersistentVolumeClaim:
		sl, ok := a.statefulsets[obj.Namespace]
		if ok && obj.UID != "" {
			for _, ss := range sl {
				if strings.HasPrefix(obj.Name, ss.LVMVolumeName+"-"+ss.Name) {
					a.knownPVC[obj.UID] = ss
					break
				}
			}
		}
	}

	return handler.Result{}, nil
}

func (a *NodeAllocator) syncLVMFreeSize() {
	ticker := time.NewTicker(syncLVMFreeSizeInterval)
	for {
		<-ticker.C
		a.lock.Lock()
		for _, n := range a.nodes {
			freeSize, err := a.getFreeSize(n.Addr + ":" + lvmdPort)
			if err != nil {
				log.Errorf("get node %s lvm info failed %s", n.Id, err.Error)
			} else {
				if freeSize != n.FreeSize {
					log.Warnf("node %s free size isn't synced with os lvm", n.Id)
					n.FreeSize = freeSize
				}
			}
		}
		a.lock.Unlock()
	}
}

func (a *NodeAllocator) addNode(n *corev1.Node, checkExists bool) {
	v, ok := n.Labels[ZkeStorageLabel]
	if ok && v == "true" {
		addr := n.Annotations[ZkeInternalIPAnnKey]
		if checkExists {
			for _, o := range a.nodes {
				if o.Id == n.Name {
					log.Warnf("node %s add more than once", n.Name)
					return
				}
			}
		}

		freeSize, err := a.getFreeSize(addr + ":" + lvmdPort)
		if err == nil {
			log.Debugf("add node %s with cap %v", n.Name, freeSize)
			a.nodes = append(a.nodes, &Node{
				Id:       n.Name,
				Addr:     addr,
				FreeSize: freeSize,
			})
		}
	}
}

func (a *NodeAllocator) addStatefulSet(ss *appsv1.StatefulSet, checkExists bool) {
	for _, vct := range ss.Spec.VolumeClaimTemplates {
		sc := vct.Spec.StorageClassName
		if sc != nil && *sc == "lvm" {
			sl, _ := a.statefulsets[ss.Namespace]
			isNew := true
			if checkExists {
				for _, ss_ := range sl {
					if ss_.Name == ss.Name {
						isNew = false
						break
					}
				}
			}
			if isNew {
				sl = append(sl, &StatefulSet{
					Name:          ss.Name,
					Nodes:         make(map[string]struct{}),
					LVMVolumeName: vct.Name,
				})
				a.statefulsets[ss.Namespace] = sl
			}
			break
		}
	}
}

func (a *NodeAllocator) OnUpdate(e event.UpdateEvent) (handler.Result, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	switch newObj := e.ObjectNew.(type) {
	case *corev1.Pod:
		oldPod := e.ObjectOld.(*corev1.Pod)
		if oldPod.Spec.NodeName != newObj.Spec.NodeName && newObj.Spec.NodeName != "" {
			a.onPodScheduledToNode(newObj)
		}
	}

	return handler.Result{}, nil
}

func (a *NodeAllocator) onPodScheduledToNode(pod *corev1.Pod) {
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != "StatefulSet" {
		return
	}

	ownerName := pod.OwnerReferences[0].Name
	sl, ok := a.statefulsets[pod.Namespace]
	if ok {
		for _, ss := range sl {
			if ss.Name == ownerName {
				ss.Nodes[pod.Spec.NodeName] = struct{}{}
				break
			}
		}
	}
}

func (a *NodeAllocator) OnDelete(e event.DeleteEvent) (handler.Result, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	switch obj := e.Object.(type) {
	case *appsv1.StatefulSet:
		sl, ok := a.statefulsets[obj.Namespace]
		if ok {
			for i, ss := range sl {
				if ss.Name == obj.Name {
					a.statefulsets[obj.Namespace] = append(sl[:i], sl[i+1:]...)
					break
				}
			}
		}
	case *corev1.PersistentVolumeClaim:
		delete(a.knownPVC, obj.UID)
	case *corev1.Node:
		v, ok := obj.Labels[ZkeStorageLabel]
		if ok && v == "true" {
			a.deleteNode(obj.Name)
		}
	}
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}

func (a *NodeAllocator) deleteNode(id string) {
	for i, n := range a.nodes {
		if n.Id == id {
			a.nodes = append(a.nodes[:i], a.nodes[i+1:]...)
			return
		}
	}

	log.Warnf("deleted storage node %s is unknown", id)
}

func (a *NodeAllocator) getFreeSize(addr string) (uint64, error) {
	conn, err := lvmd.NewLVMConnection(addr, ConnectTimeout)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.GetFreeSizeOfVG(context.TODO(), a.vgName)
}
