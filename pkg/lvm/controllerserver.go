/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lvm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/csi-common"
	lvmdclient "github.com/zdnscloud/lvmd/client"
	pb "github.com/zdnscloud/lvmd/proto"
)

const (
	DefaultFS      = "ext4"
	ConnectTimeout = 3 * time.Second
	Gigabytes      = int64(1024 * 1024 * 1024)
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	client    client.Client
	vgName    string
	allocator *NodeAllocator
}

func NewControllerServer(d *csicommon.CSIDriver, c client.Client, vgName string, cache cache.Cache) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		client:                  c,
		vgName:                  vgName,
		allocator:               NewNodeAllocator(cache, vgName),
	}
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Errorf("invalid create volume req: %v", req)
		return nil, err
	}

	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	} else {
		for _, cap := range req.VolumeCapabilities {
			if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
				return nil, status.Error(codes.InvalidArgument, "only single node writer is supported")
			}
		}
	}

	volumeId := req.GetName()
	requireBytes := req.GetCapacityRange().GetRequiredBytes()
	allocateBytes := useGigaUnit(requireBytes)
	pvcUID := strings.TrimPrefix(volumeId, "pvc-")
	node, err := cs.allocator.AllocateNodeForRequest(pvcUID, uint64(allocateBytes))
	if err != nil {
		log.Warnf("allocate node for csi request failed:%s", err.Error())
		return nil, status.Error(codes.ResourceExhausted, "insufficient capability")
	} else {
		log.Debugf("allocate node %s for csi volume request %s bytes %v", node, volumeId, requireBytes)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeId,
			CapacityBytes: allocateBytes,
			VolumeContext: req.GetParameters(),
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						NodeLabelKey: node,
					},
				},
			},
		},
	}, nil
}

//only support ReadWriteOnce
func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	vid := req.GetVolumeId()
	node, err := getVolumeNode(cs.client, vid)
	if err != nil {
		log.Warnf("get node for volume %s failed:%s", vid, err.Error())
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to getVolumeNode for %v: %v", vid, err))
	}

	addr, err := getLVMDAddr(cs.client, node)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to getLVMDAddr for %v: %v", node, err))
	}

	conn, err := lvmdclient.New(addr, ConnectTimeout)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to connect to %v: %v", addr, err))
	}
	defer conn.Close()

	rsp, err := conn.ListLV(ctx, &pb.ListLVRequest{
		VolumeGroup: fmt.Sprintf("%s/%s", cs.vgName, vid),
	})
	if err != nil {
		return nil, err
	}
	lvs := rsp.GetVolumes()
	if len(lvs) != 1 {
		return nil, errors.New("Volume %v not exists")
	}

	size := lvs[0].GetSize()

	_, err = conn.RemoveLV(ctx, &pb.RemoveLVRequest{
		VolumeGroup: cs.vgName,
		Name:        vid,
	})
	if err == nil {
		log.Debugf("release %v bytes from node %s", size, node)
		cs.allocator.Release(node, size)
		return &csi.DeleteVolumeResponse{}, nil
	} else {
		return nil, status.Errorf(
			codes.Internal,
			"Failed to remove volume: err=%v",
			err)
	}
}

func useGigaUnit(size int64) int64 {
	gs := (size + Gigabytes - 1) / Gigabytes
	return gs * Gigabytes
}

func (cs *controllerServer) validateExpandVolumeRequest(req *csi.ControllerExpandVolumeRequest) error {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME); err != nil {
		return fmt.Errorf("invalid ExpandVolumeRequest: %v", err)
	}

	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return status.Error(codes.InvalidArgument, "CapacityRange cannot be empty")
	}

	return nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if err := cs.validateExpandVolumeRequest(req); err != nil {
		return nil, fmt.Errorf("ControllerExpandVolumeRequest validation failed: %v", err)
	}
	volumeId := req.GetVolumeId()
	requireBytes := req.GetCapacityRange().GetRequiredBytes()
	allocateBytes := useGigaUnit(requireBytes)

	pv, err := getPV(cs.client, volumeId)
	if err != nil {
		return nil, fmt.Errorf("Failed to get pv by volumeId %s: %v", volumeId, err)
	}

	attach, err := isAttached(cs.client, pv.Name)
	if err != nil {
		return nil, fmt.Errorf("Failed to get volumeattachments for pv %s: %v", pv.Name, err)
	}
	if attach {
		return nil, fmt.Errorf("pv %s is attaching now, can not expand", pv.Name)
	}

	node, ok := pv.Annotations[lvmNodeAnnKey]
	if !ok {
		log.Debugf("lvm has not create for pv %s, update pv capacity", pv.Name)
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         allocateBytes,
			NodeExpansionRequired: false,
		}, nil
	}
	addr, err := getLVMDAddr(cs.client, node)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Failed to getLVMDAddr for %v: %v", node, err))
	}

	conn, err := lvmdclient.New(addr, ConnectTimeout)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Failed to connect to %v: %v", addr, err))
	}
	defer conn.Close()

	rsp, err := conn.ListLV(ctx, &pb.ListLVRequest{
		VolumeGroup: fmt.Sprintf("%s/%s", cs.vgName, volumeId),
	})
	if err != nil {
		return nil, err
	}
	lvs := rsp.GetVolumes()
	if len(lvs) != 1 {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Failed get lvm %v from %v: %v", cs.vgName+"/"+volumeId, addr, err))
	}

	resp, err := conn.ResizeLV(ctx, &pb.ResizeLVRequest{
		VolumeGroup: cs.vgName,
		Name:        volumeId,
		Size:        uint64(allocateBytes),
	})
	log.Infof("ResizeLV: %v", resp)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         allocateBytes,
		NodeExpansionRequired: false,
	}, nil
}
