package grpcdrain

import (
	"context"

	"google.golang.org/grpc"
	"pb"
)

func bad(c pb.EchoClient) {
	st, err := c.Watch(context.Background(), &pb.Req{}) // want `client stream st is neither received until an error \(io.EOF\) nor canceled`
	if err != nil {
		return
	}
	_, _ = st.Recv()
}

func badChat(c pb.EchoClient) {
	ctx := context.TODO()
	st, _ := c.Chat(ctx) // want `client stream st is neither received`
	_ = st.Send(&pb.Req{})
	_ = st.CloseSend()
}

func drained(c pb.EchoClient) {
	st, err := c.Watch(context.Background(), &pb.Req{})
	if err != nil {
		return
	}
	for {
		if _, err := st.Recv(); err != nil {
			return
		}
	}
}

func canceled(c pb.EchoClient) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, _ := c.Watch(ctx, &pb.Req{})
	_, _ = st.Recv()
}

func callersContext(ctx context.Context, c pb.EchoClient) {
	st, _ := c.Watch(ctx, &pb.Req{})
	_, _ = st.Recv()
}

func upload(c pb.EchoClient) {
	st, _ := c.Upload(context.Background())
	_ = st.Send(&pb.Req{})
	_, _ = st.CloseAndRecv()
}

func handedOn(c pb.EchoClient) {
	st, _ := c.Watch(context.Background(), &pb.Req{})
	consume(st)
}

func consume(st grpc.ServerStreamingClient[pb.Res]) {}
