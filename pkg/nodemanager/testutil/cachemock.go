package testutil

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/zdnscloud/gok8s/cache"
	"github.com/zdnscloud/gok8s/client"
)

var _ cache.Cache = &MockCache{}

type MockCache struct {
	getResult  runtime.Object
	listResult runtime.Object
}

func NewMockCache() *MockCache {
	return &MockCache{}
}

func (c *MockCache) GetInformer(obj runtime.Object) (toolscache.SharedIndexInformer, error) {
	return nil, fmt.Errorf("not support")
}
func (c *MockCache) GetInformerForKind(gvk schema.GroupVersionKind) (toolscache.SharedIndexInformer, error) {
	return nil, fmt.Errorf("not support")
}
func (c *MockCache) Start(stopCh <-chan struct{}) error {
	return nil
}
func (c *MockCache) WaitForCacheSync(stop <-chan struct{}) bool {
	return true
}
func (c *MockCache) IndexField(obj runtime.Object, field string, extractValue cache.IndexerFunc) error {
	return nil
}

func (c *MockCache) Get(ctx context.Context, key client.ObjectKey, obj runtime.Object) error {
	if c.getResult != nil {
		pointer := reflect.ValueOf(obj)
		pointer.Elem().Set(reflect.Indirect(reflect.ValueOf(c.getResult)))
	}
	return nil
}

func (c *MockCache) List(ctx context.Context, opts *client.ListOptions, list runtime.Object) error {
	if c.listResult != nil {
		pointer := reflect.ValueOf(list)
		pointer.Elem().Set(reflect.Indirect(reflect.ValueOf(c.listResult)))
	}
	return nil
}

func (c *MockCache) SetGetResult(getResult runtime.Object) {
	c.getResult = getResult
}

func (c *MockCache) SetListResult(listResult runtime.Object) {
	c.listResult = listResult
}
