package closewrite

import (
	"fmt"
	"io"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
)

func bad(s *link.Session) {
	s.CloseWrite()
	s.Write([]byte("late")) // want `write to session s after its CloseWrite \(line 12\)`
}

func badBranch(s peerlink.Session, half bool) {
	if half {
		_ = s.CloseWrite()
	}
	fmt.Fprintf(s, "x") // want `write to session s after its CloseWrite`
}

func badLoop(s *link.Session, msgs [][]byte) {
	for _, m := range msgs {
		_, _ = s.Write(m) // want `write to session s after its CloseWrite`
		_ = s.CloseWrite()
	}
}

func badCopy(s *link.Session, r io.Reader) {
	if err := s.CloseWrite(); err != nil {
		return
	}
	_, _ = io.Copy(s, r) // want `write to session s after its CloseWrite`
}

func good(s *link.Session, msgs [][]byte) ([]byte, error) {
	for _, m := range msgs {
		_, _ = s.Write(m)
	}
	_ = s.CloseWrite()
	return io.ReadAll(s) // reading after CloseWrite is fine
}

func reopened(m *link.Mux) {
	s, _ := m.Open(nil)
	_ = s.CloseWrite()
	s, _ = m.Open(nil)
	_, _ = s.Write(nil)
}

func deferred(s *link.Session) {
	defer s.CloseWrite()
	_, _ = s.Write(nil)
}

func otherGoroutine(s *link.Session) {
	go func() { _ = s.CloseWrite() }()
	_, _ = s.Write(nil)
}

func otherSession(a, b *link.Session) {
	_ = a.CloseWrite()
	_, _ = b.Write(nil)
}
