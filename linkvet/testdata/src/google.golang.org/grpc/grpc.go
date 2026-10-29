// Package grpc is a stub of google.golang.org/grpc for the tests.
package grpc

import "context"

type CallOption interface{}
type DialOption interface{}
type MD map[string][]string

type ClientConn struct{}

func Dial(target string, opts ...DialOption) (*ClientConn, error) { return nil, nil }
func DialContext(ctx context.Context, target string, opts ...DialOption) (*ClientConn, error) {
	return nil, nil
}
func NewClient(target string, opts ...DialOption) (*ClientConn, error) { return nil, nil }
func (cc *ClientConn) Invoke(ctx context.Context, method string, args, reply any, opts ...CallOption) error {
	return nil
}
func (cc *ClientConn) Close() error { return nil }

type ClientStream interface {
	Header() (MD, error)
	Trailer() MD
	CloseSend() error
	Context() context.Context
	SendMsg(m any) error
	RecvMsg(m any) error
}

type ServerStreamingClient[Res any] interface {
	Recv() (*Res, error)
	ClientStream
}

type ClientStreamingClient[Req, Res any] interface {
	Send(*Req) error
	CloseAndRecv() (*Res, error)
	ClientStream
}

type BidiStreamingClient[Req, Res any] interface {
	Send(*Req) error
	Recv() (*Res, error)
	ClientStream
}

type ServerStream interface {
	Context() context.Context
	SendMsg(m any) error
	RecvMsg(m any) error
}

type BidiStreamingServer[Req, Res any] interface {
	Recv() (*Req, error)
	Send(*Res) error
	ServerStream
}
