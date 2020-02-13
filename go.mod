module github.com/zdnscloud/csi-lvm-plugin

go 1.13

require (
	github.com/container-storage-interface/spec v1.2.0
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/kubernetes-csi/csi-lib-utils v0.6.1
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pborman/uuid v1.2.0 // indirect
	github.com/prometheus/client_golang v0.9.3 // indirect
	github.com/prometheus/common v0.4.1 // indirect
	github.com/prometheus/procfs v0.0.1 // indirect
	github.com/zdnscloud/cement v0.0.0-20200205075737-175eefa2a628
	github.com/zdnscloud/gok8s v0.0.0-20200205030309-01bcca9746a5
	github.com/zdnscloud/lvmd v0.0.0-20190911093443-19c3110dcb59
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	golang.org/x/sys v0.0.0-20200212091648-12a6c2dcc1e4 // indirect
	google.golang.org/genproto v0.0.0-20200212174721-66ed5ce911ce // indirect
	google.golang.org/grpc v1.27.1
	k8s.io/api v0.0.0-20191004102255-dacd7df5a50b
	k8s.io/apimachinery v0.0.0-20191004074956-01f8b7d1121a
	k8s.io/apiserver v0.0.0-20190521191746-6720cc558305 // indirect
	k8s.io/client-go v10.0.0+incompatible
	k8s.io/cloud-provider v0.0.0-20190528161616-75255ef19f99 // indirect
	k8s.io/csi-api v0.0.0-20190313123203-94ac839bf26c // indirect
	k8s.io/kubernetes v1.14.0-alpha.0.0.20190531054627-b88836236486
	k8s.io/utils v0.0.0-20190529001817-6999998975a7 // indirect
)
