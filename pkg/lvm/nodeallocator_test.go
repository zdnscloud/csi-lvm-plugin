package lvm

import (
	"math/rand"
	"testing"
	"time"

	ut "github.com/zdnscloud/cement/unittest"
)

func TestAllocateNode(t *testing.T) {
	cases := []struct {
		statefulset *StatefulSet
		candidate   []string
		selectNode  string
	}{
		{
			&StatefulSet{
				PodAndNode: map[string]string{
					"p1": "n1",
				},
			},
			[]string{"n1", "n2"},
			"n2",
		},
		{
			&StatefulSet{
				PodAndNode: map[string]string{
					"p1": "n1",
					"p2": "n2",
				},
			},
			[]string{"n1", "n2", "n3"},
			"n3",
		},
		{
			&StatefulSet{
				PodAndNode: map[string]string{
					"p1": "n1",
					"p2": "n2",
					"p3": "n1",
				},
			},
			[]string{"n1", "n2"},
			"n2",
		},

		{
			&StatefulSet{
				PodAndNode: map[string]string{
					"p1": "n3",
					"p2": "n1",
					"p3": "n2",
					"p4": "n2",
				},
			},
			[]string{"n1", "n2"},
			"n1",
		},
	}

	for _, tc := range cases {
		ut.Equal(t, allocateNodeForStatefulSet(tc.statefulset, tc.candidate), tc.selectNode)
	}
}

func TestAllocateNodeWithEqulScore(t *testing.T) {
	rand.Seed(time.Now().Unix())
	ss := &StatefulSet{
		PodAndNode: map[string]string{
			"p1": "n3",
		},
	}
	nodes := []string{"n1", "n2", "n3"}

	for i := 0; i < 100; i++ {
		selectN1Count := 0
		selectN2Count := 0
		for j := 0; j < 100; j++ {
			target := allocateNodeForStatefulSet(ss, nodes)
			ut.Assert(t, target != "n3", "should never select n3")
			if target == "n1" {
				selectN1Count += 1
			} else if target == "n2" {
				selectN2Count += 1
			}
		}
		ut.Assert(t, selectN1Count > 30, "n1 select less")
		ut.Assert(t, selectN2Count > 30, "n2 select less")
	}
}
