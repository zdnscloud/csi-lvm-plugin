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
	"fmt"
	"time"

	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/zdnscloud/csi-lvm-plugin/logger"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/lvmd"
)

const (
	defaultFs      = "ext4"
	connectTimeout = 3 * time.Second
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
		logger.Error("invalid create volume req: %v", req)
		return nil, err
	}

	if len(req.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	volumeId := req.GetName()
	requireBytes := req.GetCapacityRange().GetRequiredBytes()
	node, err := cs.allocator.AllocateNodeForRequest(uint64(requireBytes))
	if err != nil {
		logger.Warn("allocate node for csi request failed:%s", err.Error())
		return nil, status.Error(codes.ResourceExhausted, "insufficient capability")
	}

	response := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeId,
			CapacityBytes: requireBytes,
			VolumeContext: req.GetParameters(),
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						NodeLabelKey: node,
					},
				},
			},
		},
	}
	return response, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	vid := req.GetVolumeId()
	node, err := getVolumeNode(cs.client, vid)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to getVolumeNode for %v: %v", vid, err))
	}
	if node != "" {
		addr, err := getLVMDAddr(cs.client, node)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Failed to getLVMDAddr for %v: %v", node, err))
		}

		conn, err := lvmd.NewLVMConnection(addr, connectTimeout)
		defer conn.Close()
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to connect to %v: %v", addr, err))
		}

		if _, size, err := conn.GetLV(ctx, cs.vgName, vid); err == nil {
			if err := conn.RemoveLV(ctx, cs.vgName, vid); err != nil {
				return nil, status.Errorf(
					codes.Internal,
					"Failed to remove volume: err=%v",
					err)
			}
			cs.allocator.Release(size, node)
		}
	}

	response := &csi.DeleteVolumeResponse{}
	return response, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return &csi.ControllerExpandVolumeResponse{}, nil
}
