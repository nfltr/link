package sessionclose

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func good(m *link.Mux) error {
	s, err := m.Open([]byte("x"))
	if err != nil {
		return err
	}
	defer s.Close()
	_, err = s.Write([]byte("hi"))
	return err
}

func leakOnError(m *link.Mux) error {
	s, err := m.Open([]byte("x")) // want `session s is not closed on every path`
	if err != nil {
		return err
	}
	if _, err := s.Write([]byte("hi")); err != nil {
		return err // s stays open
	}
	return s.Close()
}

func neverClosed(m *link.Mux) {
	s, _ := m.OpenChain(nil, link.Chain{}) // want `session s is not closed on every path`
	_, _ = s.Write(nil)
}

func varDecl(m *link.Mux) {
	var s, _ = m.Open(nil) // want `session s is not closed on every path`
	_, _ = io.WriteString(s, "x")
}

func dropped(m *link.Mux) {
	m.Open(nil)        // want `the session this Open returns is dropped`
	_, _ = m.Open(nil) // want `the session this Open returns is dropped`
}

func peer(ctx context.Context, n *peerlink.Node) error {
	s, err := n.Open(ctx, "b", nil) // want `session s is not closed on every path`
	if err != nil {
		return err
	}
	_, err = s.Write(nil)
	return err
}

// Released, handed on or ended on every path: no report.

func returned(m *link.Mux) (*link.Session, error) {
	s, err := m.Open(nil)
	if err != nil {
		return nil, err
	}
	if _, err := s.Write(nil); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func joined(m *link.Mux, in *link.Session) error {
	out, err := m.Open(nil)
	if err != nil {
		return err
	}
	return link.Join(in, out)
}

func goroutine(m *link.Mux) {
	s, err := m.Open(nil)
	if err != nil {
		return
	}
	go func() {
		defer s.Close()
		_, _ = io.Copy(io.Discard, s)
	}()
}

func fatal(m *link.Mux) {
	s, err := m.Open(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
}

func panics(m *link.Mux) {
	s, err := m.Open(nil)
	if err != nil {
		panic(err)
	}
	if _, err := s.Write(nil); err != nil {
		panic(err)
	}
	s.Close()
}

func errEqNil(m *link.Mux) {
	if s, err := m.Open(nil); err == nil {
		s.Close()
	}
}

func nilCheck(ctx context.Context, p *link.Pool) {
	s, _ := p.Open(ctx, nil)
	if s == nil {
		return
	}
	s.Reset()
}

func halfClose(m *link.Mux) ([]byte, error) {
	s, err := m.Open(nil)
	if err != nil {
		return nil, err
	}
	_, _ = io.WriteString(s, "req")
	s.CloseWrite() // ends once the peer closes its side
	return io.ReadAll(s)
}

func unknownErrUse(m *link.Mux) error {
	s, err := m.Open(nil)
	if errors.Is(err, io.EOF) { // a test it does not follow: no report
		return err
	}
	return s.Close()
}

func loopReopen(m *link.Mux) {
	for range 3 {
		s, err := m.Open(nil)
		if err != nil {
			continue
		}
		s.Close()
	}
}

func stored(m *link.Mux, all map[string]*link.Session) {
	s, _ := m.Open(nil)
	all["x"] = s
}

func forever(m *link.Mux) {
	s, _ := m.Open(nil)
	for {
		_, _ = s.Write(nil)
	}
}

// Accept handlers own their session.

func serve(m *link.Mux, n *peerlink.Node) {
	_ = m.Serve(func(h []byte, s *link.Session) { // want `the accept handler owns session s and does not close it on every path`
		_, _ = io.Copy(io.Discard, s)
	})
	_ = m.Serve(func(h []byte, s *link.Session) {
		defer s.Close()
		_, _ = io.Copy(s, s)
	})
	_ = m.Serve(func(h []byte, _ *link.Session) {}) // want `the accept handler owns its session and never closes it`
	_ = m.Serve(handle)
	_ = m.Serve(handleOK)
	_ = n.Serve(func(peer string, h []byte, s peerlink.Session) {
		if len(h) == 0 {
			_ = s.Reset()
			return
		}
		_ = s.Close()
	})
	_ = m.Serve(nil)
}

func handle(h []byte, s *link.Session) { // want `the accept handler owns session s and does not close it on every path`
	if len(h) == 0 {
		return
	}
	s.Close()
}

func handleOK(h []byte, s *link.Session) { defer s.Close() }

// Not passed to Serve: its caller owns s.
func notHandler(h []byte, s *link.Session) {}

func viaInterface(t testing.TB, m *link.Mux) *link.Session {
	s, err := m.Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Header()) == 0 {
		t.Fatal("no header") // testing.TB's Fatal does not return
	}
	return s
}

func selectNoDefault(t *testing.T, m *link.Mux, got chan *link.Session) (*link.Session, *link.Session) {
	s, err := m.Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		return s, p
	case <-t.Context().Done():
		t.Fatal("no session") // the select has no default: no path falls out of it
	}
	return nil, nil
}
