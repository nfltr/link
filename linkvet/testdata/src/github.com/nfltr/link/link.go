// Package link is a stub of github.com/nfltr/link's API for the tests.
package link

import (
	"context"
	"io"
	"time"
)

type Session struct{}

func (s *Session) Read(b []byte) (int, error)                { return 0, nil }
func (s *Session) Write(b []byte) (int, error)               { return 0, nil }
func (s *Session) WriteBuffers(b [][]byte) (int64, error)    { return 0, nil }
func (s *Session) Close() error                              { return nil }
func (s *Session) CloseWrite() error                         { return nil }
func (s *Session) Reset() error                              { return nil }
func (s *Session) Abort(code uint32, msg string) error       { return nil }
func (s *Session) Refuse(code uint32, msg string) error      { return nil }
func (s *Session) SetDeadline(t time.Time) error             { return nil }
func (s *Session) SetReadDeadline(t time.Time) error         { return nil }
func (s *Session) Done() <-chan struct{}                     { return nil }
func (s *Session) Header() []byte                            { return nil }
func (s *Session) Answered(ctx context.Context) error        { return nil }
func (s *Session) Pump(w io.Writer, done func(int64, error)) {}

type Chain struct{}

type Mux struct{}

func (m *Mux) Open(header []byte) (*Session, error)               { return nil, nil }
func (m *Mux) OpenChain(header []byte, c Chain) (*Session, error) { return nil, nil }
func (m *Mux) Serve(accept func(open []byte, s *Session)) error   { return nil }

type Pool struct{}

func (p *Pool) Open(ctx context.Context, header []byte) (*Session, error) { return nil, nil }

func Join(a, b *Session) error { return nil }
