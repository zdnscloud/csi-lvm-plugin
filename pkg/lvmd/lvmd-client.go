package lvmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/golang/glog"
	lvmd "github.com/google/lvmd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	ErrVGNotExist = errors.New("vg doesn't exist")
)

type LVMConnection interface {
	GetLV(ctx context.Context, volGroup string, volumeId string) (string, uint64, error)
	CreateLV(ctx context.Context, opt *LVMOptions) (string, error)
	RemoveLV(ctx context.Context, volGroup string, volumeId string) error
	GetFreeSizeOfVG(ctx context.Context, vgName string) (uint64, error)

	Close() error
}

type lvmConnection struct {
	conn *grpc.ClientConn
}

var (
	_ LVMConnection = &lvmConnection{}
)

func NewLVMConnection(address string, timeout time.Duration) (LVMConnection, error) {
	conn, err := connect(address, timeout)
	if err != nil {
		return nil, err
	}
	return &lvmConnection{
		conn: conn,
	}, nil
}

func (c *lvmConnection) Close() error {
	return c.conn.Close()
}

func connect(address string, timeout time.Duration) (*grpc.ClientConn, error) {
	glog.V(2).Infof("Connecting to %s", address)
	dialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithUnaryInterceptor(logGRPC),
	}
	if strings.HasPrefix(address, "/") {
		dialOptions = append(dialOptions, grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
	}
	conn, err := grpc.Dial(address, dialOptions...)

	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			glog.V(4).Infof("Connection timed out")
			return conn, nil // return nil, subsequent GetPluginInfo will show the real connection error
		}
		if conn.GetState() == connectivity.Ready {
			glog.V(3).Infof("Connected")
			return conn, nil
		}
		glog.V(4).Infof("Still trying, connection is %s", conn.GetState())
	}
}

type LVMOptions struct {
	VolumeGroup string
	Name        string
	Size        uint64
	Tags        []string
}

func (c *lvmConnection) CreateLV(ctx context.Context, opt *LVMOptions) (string, error) {
	client := lvmd.NewLVMClient(c.conn)

	req := lvmd.CreateLVRequest{
		VolumeGroup: opt.VolumeGroup,
		Name:        opt.Name,
		Size:        opt.Size,
		Tags:        opt.Tags,
	}

	rsp, err := client.CreateLV(ctx, &req)
	if err != nil {
		return "", err
	}
	return rsp.GetCommandOutput(), nil
}

func (c *lvmConnection) GetLV(ctx context.Context, volGroup string, volumeId string) (string, uint64, error) {
	client := lvmd.NewLVMClient(c.conn)
	req := lvmd.ListLVRequest{
		VolumeGroup: fmt.Sprintf("%s/%s", volGroup, volumeId),
	}
	rsp, err := client.ListLV(ctx, &req)
	if err != nil {
		return "", 0, err
	}

	lvs := rsp.GetVolumes()
	if len(lvs) != 1 {
		return "", 0, errors.New("Volume %v not exists")
	}
	lv := lvs[0]
	return lv.GetName(), lv.GetSize(), nil
}

func (c *lvmConnection) RemoveLV(ctx context.Context, volGroup string, volumeId string) error {
	client := lvmd.NewLVMClient(c.conn)

	req := lvmd.RemoveLVRequest{
		VolumeGroup: volGroup,
		Name:        volumeId,
	}

	rsp, err := client.RemoveLV(ctx, &req)
	glog.V(5).Infof("removeLV output: %v", rsp.GetCommandOutput())
	return err
}

func (c *lvmConnection) GetFreeSizeOfVG(ctx context.Context, vgName string) (uint64, error) {
	client := lvmd.NewLVMClient(c.conn)
	req := lvmd.ListVGRequest{}
	rsp, err := client.ListVG(context.TODO(), &req)
	if err != nil {
		return 0, err
	}

	for _, vg := range rsp.VolumeGroups {
		if vg.Name == vgName {
			return vg.FreeSize, nil
		}
	}
	return 0, ErrVGNotExist
}

func logGRPC(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	glog.V(5).Infof("GRPC call: %s", method)
	glog.V(5).Infof("GRPC request: %+v", req)
	err := invoker(ctx, method, req, reply, cc, opts...)
	glog.V(5).Infof("GRPC response: %+v", reply)
	glog.V(5).Infof("GRPC error: %v", err)
	return err
}
