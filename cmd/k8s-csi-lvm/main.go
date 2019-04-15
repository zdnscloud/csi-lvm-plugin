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

package main

import (
	"flag"

	"github.com/zdnscloud/csi-lvm-plugin/logger"
	"github.com/zdnscloud/csi-lvm-plugin/pkg/lvm"

	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"
	"github.com/zdnscloud/gok8s/client/config"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	endpoint   = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	driverName = flag.String("drivername", "k8s-csi-lvm", "name of the driver")
	nodeID     = flag.String("nodeid", "", "node id")
	vgName     = flag.String("vgname", "k8s", "volume group name")
)

func main() {
	flag.Parse()

	logger.InitLogger()
	config, err := config.GetConfig()
	if err != nil {
		logger.Fatal("get k8s config failed:%s", err.Error())
	}

	cli, err := client.New(config, client.Options{})
	if err != nil {
		logger.Fatal("create k8s client failed:%s", err.Error())
	}

	stop := make(chan struct{})
	cache, err := cache.New(config, cache.Options{})
	if err != nil {
		logger.Fatal("create cache failed:%s", err.Error())
	}
	go cache.Start(stop)
	cache.WaitForCacheSync(stop)

	driver := lvm.GetLVMDriver(cli)
	driver.Run(*driverName, *nodeID, *endpoint, *vgName, cache)
}
