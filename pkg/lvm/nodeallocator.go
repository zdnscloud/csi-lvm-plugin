package lvm

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/nodemanager"
	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"
	"github.com/zdnscloud/gok8s/controller"
	"github.com/zdnscloud/gok8s/event"
	"github.com/zdnscloud/gok8s/handler"
	"github.com/zdnscloud/gok8s/predicate"
)

type StatefulSet struct {
	Name          string
	PodAndNode    map[string]string
	LVMVolumeName string
}

type StatefulSetList []*StatefulSet

type NodeAllocator struct {
	stopCh chan struct{}
	cache  cache.Cache

	lock         sync.Mutex
	statefulsets map[string]StatefulSetList
	knownPVC     map[k8stypes.UID]*StatefulSet
	lvmNodes     *nodemanager.NodeManager
}

func NewNodeAllocator(c cache.Cache, vgName, labelKey, labelValue string) *NodeAllocator {
	rand.Seed(time.Now().Unix())

	ctrl := controller.New("nodeAllocator", c, scheme.Scheme)
	ctrl.Watch(&corev1.Pod{})
	ctrl.Watch(&corev1.PersistentVolumeClaim{})
	ctrl.Watch(&appsv1.StatefulSet{})

	stopCh := make(chan struct{})
	a := &NodeAllocator{
		stopCh:       stopCh,
		cache:        c,
		lvmNodes:     nodemanager.New(c, vgName, labelKey, labelValue),
		statefulsets: make(map[string]StatefulSetList),
		knownPVC:     make(map[k8stypes.UID]*StatefulSet),
	}
	a.initStatefulSet()
	go ctrl.Start(stopCh, a, predicate.NewIgnoreUnchangedUpdate())

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

func (a *NodeAllocator) AllocateNodeForRequest(pvcUID string, size uint64) (node string, err error) {
	candidates := a.lvmNodes.GetNodesHasFreeSize(size)
	if len(candidates) == 0 {
		return "", nodemanager.ErrNoEnoughFreeSpace
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	ss, ok := a.knownPVC[k8stypes.UID(pvcUID)]
	var selectNode string
	if ok {
		log.Debugf("allocate node for pvc %s in statefulset %s", pvcUID, ss.Name)
		selectNode = allocateNodeForStatefulSet(ss, candidates)
	} else {
		log.Debugf("random allocate node for pvc %s", pvcUID)
		selectNode = randomeAllocateNode(candidates)
	}
	a.lvmNodes.Allocate(selectNode, size)
	return selectNode, nil
}

func allocateNodeForStatefulSet(ss *StatefulSet, candidate []string) string {
	if len(candidate) == 1 {
		return candidate[0]
	}

	usedNodes := make(map[string]int)
	for _, n := range ss.PodAndNode {
		usedNodes[n] = usedNodes[n] + 1
	}

	scores := make([]int, len(candidate))
	smallest := 10240 //a impossible value for pod count on a node
	for i, c := range candidate {
		score, ok := usedNodes[c]
		if ok == false {
			score = 0
		}
		scores[i] = score
		if score < smallest {
			smallest = score
		}
	}

	targetNodes := make([]string, 0, len(candidate))
	for i := 0; i < len(candidate); i++ {
		if scores[i] == smallest {
			targetNodes = append(targetNodes, candidate[i])
		}
	}

	if len(targetNodes) == 1 {
		return targetNodes[0]
	} else {
		return targetNodes[rand.Intn(len(targetNodes))]
	}
}

func randomeAllocateNode(candidate []string) string {
	return candidate[rand.Intn(len(candidate))]
}

func (a *NodeAllocator) Release(name string, size uint64) {
	a.lvmNodes.Release(name, size)
}

func (a *NodeAllocator) OnCreate(e event.CreateEvent) (handler.Result, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	switch obj := e.Object.(type) {
	case *appsv1.StatefulSet:
		a.addStatefulSet(obj, true)
	case *corev1.Pod:
		a.changeNodeOfPod(obj, obj.Spec.NodeName)
	case *corev1.PersistentVolumeClaim:
		sl, ok := a.statefulsets[obj.Namespace]
		if ok && obj.UID != "" {
			for _, ss := range sl {
				if strings.HasPrefix(obj.Name, ss.LVMVolumeName+"-"+ss.Name+"-") {
					log.Debugf("add pvc %s to statefulset %s", obj.Name, ss.Name)
					a.knownPVC[obj.UID] = ss
					break
				}
			}
		}
	}

	return handler.Result{}, nil
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
				log.Debugf("add new statefulset %s with volume name %s", ss.Name, vct.Name)
				sl = append(sl, &StatefulSet{
					Name:          ss.Name,
					PodAndNode:    make(map[string]string),
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
		if oldPod.Spec.NodeName != newObj.Spec.NodeName {
			a.changeNodeOfPod(newObj, newObj.Spec.NodeName)
		}
	}

	return handler.Result{}, nil
}

func (a *NodeAllocator) changeNodeOfPod(pod *corev1.Pod, nodeName string) {
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != "StatefulSet" {
		return
	}

	ownerName := pod.OwnerReferences[0].Name
	sl, ok := a.statefulsets[pod.Namespace]
	if ok == false {
		return
	}

	for _, ss := range sl {
		if ss.Name == ownerName {
			if nodeName == "" {
				delete(ss.PodAndNode, pod.Name)
			} else {
				log.Debugf("pod %s belongs to statefulset %s is scheduled to node %s", pod.Name, ss.Name, nodeName)
				ss.PodAndNode[pod.Name] = nodeName
			}
			break
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
	case *corev1.Pod:
		a.changeNodeOfPod(obj, "")
	}
	return handler.Result{}, nil
}

func (a *NodeAllocator) OnGeneric(e event.GenericEvent) (handler.Result, error) {
	return handler.Result{}, nil
}
