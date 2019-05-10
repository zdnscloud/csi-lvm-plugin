package main

import (
	"context"
	"fmt"
	"github.com/zdnscloud/gok8s/client"
	"github.com/zdnscloud/gok8s/client/config"
	corev1 "k8s.io/api/core/v1"
	//types "k8s.io/api/core/v1/types.go"
)

const (
	ZkeStorageLabel     = "node-role.kubernetes.io/storage"
	ZkeInternalIPAnnKey = "zdnscloud.cn/internal-ip"
)

type Node struct {
	Name string
	Addr string
}

func main() {
	var nodes []*Node
	res := GetStorageNode()
	for _, v := range res {
		node := &Node{
			Name: v.Name,
			Addr: v.Annotations[ZkeInternalIPAnnKey],
		}
		nodes = append(nodes, node)
	}
	fmt.Println(nodes)
}

func GetStorageNode() []corev1.Node {
	config, _ := config.GetConfig()
	cli, _ := client.New(config, client.Options{})
	nodes := corev1.NodeList{}
	cli.List(context.TODO(), nil, &nodes)
	var res []corev1.Node
	for _, n := range nodes.Items {
		v, ok := n.Labels[ZkeStorageLabel]
		if ok && v == "true" {
			res = append(res, n)
		}
	}
	return res
}
