// Package pb is generated-looking gRPC code for the tests.
package pb

import (
	"context"

	"google.golang.org/grpc"
)

type Req struct{}
type Res struct{}

type EchoClient interface {
	Unary(ctx context.Context, in *Req, opts ...grpc.CallOption) (*Res, error)
	Watch(ctx context.Context, in *Req, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Res], error)
	Upload(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[Req, Res], error)
	Chat(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[Req, Res], error)
}

type UnimplementedEchoServer struct{}

type Echo_ChatServer = grpc.BidiStreamingServer[Req, Res]
