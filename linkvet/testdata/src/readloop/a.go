package readloop

import (
	"context"
	"io"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func bad(ctx context.Context, p *link.Pool) {
	s, err := p.Open(ctx, nil)
	if err != nil {
		return
	}
	defer s.Close()
	buf := make([]byte, 10)
	for {
		if _, err := s.Read(buf); err != nil { // want `read of session s in a loop with no deadline and no cancel path`
			return
		}
	}
}

func badFull(n *peerlink.Node) {
	s, _ := n.Open(context.Background(), "b", nil)
	buf := make([]byte, 4)
	for range 10 {
		_, _ = io.ReadFull(s, buf) // want `read of session s in a loop`
	}
	s.Close()
}

func badMux(ctx context.Context, m *link.Mux) {
	s, _ := m.Open(nil)
	defer s.Close()
	for {
		if _, err := s.Read(nil); err != nil { // want `read of session s in a loop`
			return
		}
	}
}

func deadline(ctx context.Context, p *link.Pool) {
	s, _ := p.Open(ctx, nil)
	defer s.Close()
	for {
		_ = s.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := s.Read(nil); err != nil {
			return
		}
	}
}

func afterFunc(ctx context.Context, p *link.Pool) {
	s, _ := p.Open(ctx, nil)
	stop := context.AfterFunc(ctx, func() { s.Close() })
	defer stop()
	for {
		if _, err := s.Read(nil); err != nil {
			return
		}
	}
}

func noContext(m *link.Mux) {
	s, _ := m.Open(nil)
	defer s.Close()
	for {
		if _, err := s.Read(nil); err != nil {
			return
		}
	}
}

func once(ctx context.Context, p *link.Pool) {
	s, _ := p.Open(ctx, nil)
	defer s.Close()
	_, _ = s.Read(nil)
}

func handedOn(ctx context.Context, p *link.Pool) {
	s, _ := p.Open(ctx, nil)
	watch(ctx, s)
	for {
		if _, err := s.Read(nil); err != nil {
			return
		}
	}
}

func watch(ctx context.Context, s *link.Session) {}
