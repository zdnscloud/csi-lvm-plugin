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
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"

	"github.com/zdnscloud/cement/log"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/csi-common"
)

type Driver struct {
	driver *csicommon.CSIDriver
	client client.Client

	ids *identityServer
	ns  *nodeServer
	cs  *controllerServer
}

var (
	vendorVersion = "1.0.0"
)

func NewDriver(client client.Client) *Driver {
	return &Driver{client: client}
}

func (lvm *Driver) Run(cache cache.Cache, conf *PluginConf) {
	lvm.driver = csicommon.NewCSIDriver(conf.DriverName, vendorVersion, conf.NodeID)
	if lvm.driver == nil {
		log.Fatalf("Failed to initialize CSI Driver.")
	}

	lvm.driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	})
	lvm.driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	lvm.ids = NewIdentityServer(lvm.driver)
	lvm.ns = NewNodeServer(lvm.driver, lvm.client, conf)
	lvm.cs = NewControllerServer(lvm.driver, lvm.client, cache, conf)

	server := csicommon.NewNonBlockingGRPCServer()
	server.Start(conf.Endpoint, lvm.ids, lvm.cs, lvm.ns)
	server.Wait()
}
