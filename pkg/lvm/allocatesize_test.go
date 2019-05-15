package lvm

import (
	"testing"

	ut "github.com/zdnscloud/cement/unittest"
)

func TestSizeRoundToGigabytes(t *testing.T) {
	cases := []struct {
		requireBytes  int64
		allocateBytes int64
	}{
		{1, Gigabytes},
		{Gigabytes, Gigabytes},
		{Gigabytes - 1, Gigabytes},
		{2 * Gigabytes, 2 * Gigabytes},
		{10*Gigabytes - 1, 10 * Gigabytes},
	}

	for _, tc := range cases {
		ut.Equal(t, useGigaUnit(tc.requireBytes), tc.allocateBytes)
	}
}
