package client

import (
	"time"

	"google.golang.org/grpc"

	pb "github.com/zdnscloud/lvmd/proto"
)

type Client struct {
	pb.LVMClient
	conn *grpc.ClientConn
}

func New(addr string, timeout time.Duration) (*Client, error) {
	dialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithTimeout(timeout),
	}

	conn, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		return nil, err
	}

	return &Client{
		LVMClient: pb.NewLVMClient(conn),
		conn:      conn,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
