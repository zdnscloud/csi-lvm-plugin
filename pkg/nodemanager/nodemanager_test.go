package nodemanager

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/zdnscloud/cement/log"
	ut "github.com/zdnscloud/cement/unittest"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/nodemanager/testutil"
)

var globalSize uint64
var globalFreeSize uint64
var globalError error

func mockGetVGSize(addr, vg string) (uint64, uint64, error) {
	return globalSize, globalFreeSize, globalError
}

func TestNodeManager(t *testing.T) {
	fakeError := fmt.Errorf("cann't reached")
	log.InitLogger(log.Debug)
	cache := testutil.NewMockCache()
	mgr := NewNodeManager(cache, "k8s")
	ut.Assert(t, mgr != nil, "")
	mgr.vgSizeGetter = mockGetVGSize

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "master",
			Annotations: map[string]string{
				ZkeStorageLabel: ZkeStorageLabelValue,
			},
		},
	}
	globalError = fakeError
	mgr.AddNode(node)

	nodes := mgr.GetNodes()
	ut.Equal(t, len(nodes), 1)
	ut.Assert(t, len(mgr.GetNodesHasFreeSize(10)) == 0, "")

	globalError = nil
	globalSize = 1000
	globalFreeSize = 1000
	node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker1",
			Annotations: map[string]string{
				ZkeStorageLabel: ZkeStorageLabelValue,
			},
		},
	}
	mgr.AddNode(node)
	globalError = fakeError
	nodes = mgr.GetNodes()
	ut.Equal(t, len(nodes), 2)
	ut.Assert(t, len(mgr.GetNodesHasFreeSize(10)) == 1, "")
	globalError = nil
	globalSize = 2
	globalFreeSize = 2
	ut.Assert(t, len(mgr.GetNodesHasFreeSize(10)) == 1, "")
	ut.Assert(t, len(mgr.GetNodesHasFreeSize(1)) == 2, "")

	mgr.DeleteNode("worker1")
	ut.Assert(t, len(mgr.GetNodesHasFreeSize(1)) == 1, "")

	globalError = nil
	globalSize = 2000
	globalFreeSize = 1000
	node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker1",
			Annotations: map[string]string{
				ZkeStorageLabel: ZkeStorageLabelValue,
			},
		},
	}
	mgr.AddNode(node)
	ut.Equal(t, len(mgr.GetNodesHasFreeSize(1000)), 1)

	mgr.Allocate("worker1", 500)
	ut.Equal(t, len(mgr.GetNodesHasFreeSize(600)), 0)
	mgr.Release("worker1", 500)
	ut.Equal(t, len(mgr.GetNodesHasFreeSize(600)), 1)
}
