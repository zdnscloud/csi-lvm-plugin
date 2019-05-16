package lvmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/zdnscloud/cement/log"
	lvmd "github.com/zdnscloud/lvmd-server/proto"
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
	GetSizeOfVG(ctx context.Context, vgName string) (uint64, uint64, error)

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
			return conn, nil // return nil, subsequent GetPluginInfo will show the real connection error
		}

		if conn.GetState() == connectivity.Ready {
			return conn, nil
		}
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

	_, err := client.RemoveLV(ctx, &req)
	return err
}

func (c *lvmConnection) GetSizeOfVG(ctx context.Context, vgName string) (uint64, uint64, error) {
	client := lvmd.NewLVMClient(c.conn)
	req := lvmd.ListVGRequest{}
	rsp, err := client.ListVG(context.TODO(), &req)
	if err != nil {
		return 0, 0, err
	}

	for _, vg := range rsp.VolumeGroups {
		if vg.Name == vgName {
			return vg.Size, vg.FreeSize, nil
		}
	}
	return 0, 0, ErrVGNotExist
}

func logGRPC(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		log.Debugf("GRPC call: %s", method)
		log.Debugf("GRPC request: %+v", req)
		log.Debugf("GRPC response: %+v", reply)
		log.Debugf("GRPC error: %v", err)
	}
	return err
}
