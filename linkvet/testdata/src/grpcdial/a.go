package grpcdial

import (
	"context"
	"net/http"

	"github.com/nfltr/link"
	"google.golang.org/grpc"
	"pb"
)

func handler(w http.ResponseWriter, r *http.Request) {
	cc, _ := grpc.NewClient("x") // want `grpc.NewClient in an HTTP handler, which runs per request`
	cc.Close()
}

func register(mux *http.ServeMux) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cc, _ := grpc.Dial("x") // want `grpc.Dial in an HTTP handler`
		cc.Close()
	})
}

type server struct{ pb.UnimplementedEchoServer }

func (s *server) Unary(ctx context.Context, in *pb.Req) (*pb.Res, error) {
	cc, _ := grpc.DialContext(ctx, "x") // want `grpc.DialContext in a gRPC service method`
	cc.Close()
	return nil, nil
}

func (s *server) dial() { _, _ = grpc.NewClient("x") }

func serve(m *link.Mux) {
	_ = m.Serve(func(h []byte, s *link.Session) {
		defer s.Close()
		_, _ = grpc.NewClient("x") // want `grpc.NewClient in a link accept handler`
	})
}

func setup() (*grpc.ClientConn, error) { return grpc.NewClient("x") }

type plain struct{}

func (plain) Unary(ctx context.Context, in *pb.Req) (*pb.Res, error) {
	_, _ = grpc.NewClient("x") // not a gRPC service (no Unimplemented...Server)
	return nil, nil
}
