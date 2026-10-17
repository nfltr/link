package grpcdeadline

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"pb"
)

func bad(c pb.EchoClient, cc *grpc.ClientConn) {
	_, _ = c.Unary(context.Background(), &pb.Req{}) // want `gRPC call with a context that has no deadline`
	ctx := context.TODO()
	_ = cc.Invoke(ctx, "/m", nil, nil)                // want `gRPC call with a context that has no deadline`
	st, _ := c.Watch(context.Background(), &pb.Req{}) // want `gRPC stream on a context with no deadline and no cancel`
	_ = st
}

func good(ctx context.Context, c pb.EchoClient) {
	_, _ = c.Unary(ctx, &pb.Req{})
	tctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = c.Unary(tctx, &pb.Req{})
}

type notGRPC struct{}

func (notGRPC) Call(ctx context.Context, opts ...int) {}

func other(n notGRPC) { n.Call(context.Background()) }
