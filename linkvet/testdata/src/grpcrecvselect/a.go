package grpcrecvselect

import (
	"context"

	"google.golang.org/grpc"
	"pb"
)

func bad(ctx context.Context, st grpc.ServerStreamingClient[pb.Res]) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if _, err := st.Recv(); err != nil { // want `Recv blocks: the select's default case does not make it non-blocking`
				return
			}
		}
	}
}

func badServer(srv pb.Echo_ChatServer, stop chan struct{}) {
	select {
	case <-stop:
	default:
		_, _ = srv.Recv() // want `Recv blocks`
	}
}

func good(ctx context.Context, st grpc.ServerStreamingClient[pb.Res], ch chan int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			_, _ = st.Recv() // no default: the select waited on purpose
		}
		if _, err := st.Recv(); err != nil {
			return
		}
	}
}

type queue struct{}

func (queue) Recv() (int, error) { return 0, nil }

func notStream(q queue) {
	select {
	default:
		_, _ = q.Recv()
	}
}
