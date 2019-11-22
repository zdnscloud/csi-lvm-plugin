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
	"os"
	"path/filepath"
	"strings"

	"github.com/zdnscloud/gok8s/client"
	"golang.org/x/net/context"
	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/csi-common"
	lvmdclient "github.com/zdnscloud/lvmd/client"
	pb "github.com/zdnscloud/lvmd/proto"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
	client client.Client
	nodeID string
	vgName string
}

func NewNodeServer(d *csicommon.CSIDriver, c client.Client, nodeID, vgName string) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		client:            c,
		vgName:            vgName,
		nodeID:            nodeID,
	}
}

func (ns *nodeServer) GetNodeID() string {
	return ns.nodeID
}

func (ns *nodeServer) createVolume(ctx context.Context, volumeId string) (*v1.PersistentVolume, error) {
	pv, err := getPV(ns.client, volumeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Failed to get pv by volumeId %s: %s", volumeId, err))
	}
	node, err := getNode(ns.client, ns.GetNodeID())
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Failed to get node by nodeId %s: %s", ns.GetNodeID(), err))
	}

	cap := pv.Spec.Capacity[v1.ResourceStorage]
	size := cap.Value()

	addr, err := getLVMDAddr(ns.client, ns.GetNodeID())
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Failed to getLVMDAddr for %v: %v", node, err))
	}

	conn, err := lvmdclient.New(addr, ConnectTimeout)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Failed to connect to %v: %v", addr, err))
	}
	defer conn.Close()

	resp, err := conn.CreateLV(ctx, &pb.CreateLVRequest{
		VolumeGroup: ns.vgName,
		Name:        volumeId,
		Size:        uint64(size),
	})
	log.Infof("CreateLV: %v", resp)

	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in CreateLogicalVolume: err=%v",
			err)
	}

	pv.Annotations[lvmNodeAnnKey] = node.GetName()
	return updatePV(ns.client, pv)
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	volumeId := req.GetVolumeId()
	devicePath := filepath.Join("/dev/", ns.vgName, volumeId)

	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		_, err := ns.createVolume(ctx, volumeId)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	log.Debugf("Determining filesystem type at %v", devicePath)
	existingFstype, err := determineFilesystemType(devicePath)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: err=%v",
			err)
	}
	log.Debugf("Existing filesystem type is '%v'", existingFstype)
	if existingFstype == "" {
		// There is no existing filesystem on the
		// device, format it with the requested
		// filesystem.
		log.Debugf("The device %v has no existing filesystem, formatting with %v", devicePath, DefaultFS)
		if err := formatDevice(devicePath, DefaultFS); err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"formatDevice failed: err=%v",
				err)
		}
		existingFstype = DefaultFS
	}

	// Volume Mount
	if notMnt {
		// Get Options
		var options []string
		if req.GetReadonly() {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}
		mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()
		options = append(options, mountFlags...)

		// Mount
		mounter := mount.New("")
		err = mounter.Mount(devicePath, targetPath, DefaultFS, options)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "Targetpath not found")
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	log.Infof("UnmountPath: %s", targetPath)
	err = util.UnmountPath(targetPath, mount.New(""))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	parentDir := targetPath[:strings.LastIndex(targetPath, "/")]
	log.Infof("Remove CSI volume path: %s", parentDir)
	if err := os.RemoveAll(parentDir); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
}
