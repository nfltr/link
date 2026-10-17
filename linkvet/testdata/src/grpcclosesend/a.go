package grpcclosesend

import (
	"context"

	"pb"
)

func bad(ctx context.Context, c pb.EchoClient) {
	st, err := c.Chat(ctx) // want `client stream st sends but never calls CloseSend`
	if err != nil {
		return
	}
	_ = st.Send(&pb.Req{})
	for {
		if _, err := st.Recv(); err != nil {
			return
		}
	}
}

func good(ctx context.Context, c pb.EchoClient) {
	st, _ := c.Chat(ctx)
	_ = st.Send(&pb.Req{})
	_ = st.CloseSend()
	for {
		if _, err := st.Recv(); err != nil {
			return
		}
	}
}

func upload(ctx context.Context, c pb.EchoClient) {
	st, _ := c.Upload(ctx)
	_ = st.Send(&pb.Req{})
	_, _ = st.CloseAndRecv()
}

func sendsElsewhere(ctx context.Context, c pb.EchoClient) {
	st, _ := c.Chat(ctx)
	go func() {
		_ = st.Send(&pb.Req{})
		_ = st.CloseSend()
	}()
	_, _ = st.Recv()
}

func noSend(ctx context.Context, c pb.EchoClient) {
	st, _ := c.Chat(ctx)
	_, _ = st.Recv()
}
