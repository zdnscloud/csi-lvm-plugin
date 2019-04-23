package lvm

import (
	"testing"

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
					"p1": "n3",
				},
			},
			[]string{"n1", "n2"},
			"n1",
		},

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
			[]string{"n1", "n2"},
			"n1",
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
