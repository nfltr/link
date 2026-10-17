package openctx

import (
	"context"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func bad(p *link.Pool, n *peerlink.Node) {
	s, _ := p.Open(context.Background(), nil) // want `Open with a context that never ends`
	defer s.Close()
	ctx := context.TODO()
	s2, _ := n.Open(ctx, "b", nil) // want `Open with a context that never ends`
	defer s2.Close()
}

func good(ctx context.Context, p *link.Pool, n *peerlink.Node) {
	s, _ := p.Open(ctx, nil)
	defer s.Close()
	tctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s2, _ := n.Open(tctx, "b", nil)
	defer s2.Close()
	bg := context.Background()
	bg, cancel2 := context.WithTimeout(bg, time.Second)
	defer cancel2()
	s3, _ := p.Open(bg, nil)
	defer s3.Close()
}
