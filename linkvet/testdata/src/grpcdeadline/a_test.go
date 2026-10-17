package grpcdeadline

import (
	"context"
	"testing"

	"pb"
)

func TestUnary(t *testing.T) {
	var c pb.EchoClient
	_, _ = c.Unary(context.Background(), &pb.Req{}) // a test: no report
}
