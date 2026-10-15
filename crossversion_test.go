// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package link

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// oldPeer is a link v0.9.4 program (no FeatureChain): "serve" echoes the
// sessions opened to it (resetting one whose header is "refuse") and
// prints its address; "dial ADDR" opens an echo session and a "refuse"
// one, checks them and prints "ok".
const oldPeer = `package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/nfltr/link"
)

func main() {
	cfg := link.Config{Auth: link.NoAuth}
	if os.Args[1] == "serve" {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		fmt.Println(ln.Addr())
		go func() { _, _ = bufio.NewReader(os.Stdin).ReadString('\n'); os.Exit(0) }()
		for {
			nc, err := ln.Accept()
			if err != nil {
				panic(err)
			}
			go func() {
				c, _, err := link.Server(context.Background(), nc, cfg)
				if err != nil {
					return
				}
				_ = link.NewMux(c).Serve(func(h []byte, s *link.Session) {
					if string(h) == "refuse" {
						_ = s.Reset()
						return
					}
					_, _ = io.Copy(s, s)
					_ = s.CloseWrite()
				})
			}()
		}
	}
	nc, err := net.Dial("tcp", os.Args[2])
	if err != nil {
		panic(err)
	}
	c, err := link.Client(context.Background(), nc, cfg)
	if err != nil {
		panic(err)
	}
	m := link.NewMux(c)
	go func() { _ = m.Serve(nil) }()
	s, err := m.Open([]byte("echo"))
	if err != nil {
		panic(err)
	}
	data := bytes.Repeat([]byte("v0.9.4 "), 100000)
	go func() { _, _ = s.Write(data); _ = s.CloseWrite() }()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, data) {
		panic(fmt.Sprintf("echo: %d bytes, %v", len(got), err))
	}
	r, err := m.Open([]byte("refuse"))
	if err != nil {
		panic(err)
	}
	if _, err := io.ReadAll(r); err == nil {
		panic("refused session read a clean end")
	}
	fmt.Println("ok")
}
`

// buildOldPeer builds oldPeer against link v0.9.4 from the module cache
// (no network: GOPROXY=off); it skips when the cache lacks v0.9.4.
func buildOldPeer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(name, s string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module oldpeer\n\ngo 1.27.0\n\nrequire github.com/nfltr/link v0.9.4\n")
	write("main.go", oldPeer)
	bin := filepath.Join(dir, "oldpeer")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOPROXY=off", "GOFLAGS=-mod=mod", "GOWORK=off", "GOSUMDB=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("link v0.9.4 not in the module cache (go mod download github.com/nfltr/link@v0.9.4): %v\n%s", err, out)
	}
	return bin
}

// TestChainAgainstV094: v0.10.0 against a v0.9.4 peer, both directions:
// the connection stays on v0.9.4's behaviour (no answers, a refusal is a
// reset), and a Forward hop in front of the old peer accepts once its
// Route returned.
func TestChainAgainstV094(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a v0.9.4 program")
	}
	bin := buildOldPeer(t)

	t.Run("new dials old", func(t *testing.T) {
		cmd := exec.Command(bin, "serve")
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Wait() })
		addr, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		dial := func() *Mux {
			nc, err := net.Dial("tcp", strings.TrimSpace(addr))
			if err != nil {
				t.Fatal(err)
			}
			c, err := Client(context.Background(), nc, Config{Auth: NoAuth})
			if err != nil {
				t.Fatal(err)
			}
			m := NewMux(c)
			go func() { _ = m.Serve(nil) }()
			t.Cleanup(func() { _ = m.Close() })
			return m
		}
		m := dial()
		if m.Conn().chain() {
			t.Fatal("FeatureChain negotiated with v0.9.4")
		}
		s, _ := m.Open([]byte("echo"))
		if s.Answers() || !errors.Is(s.Answered(context.Background()), ErrNoAnswers) {
			t.Fatal("answers from v0.9.4")
		}
		roundTrip(t, s, 1<<20)
		r, _ := m.Open([]byte("refuse"))
		if _, err := io.ReadAll(r); !errors.Is(err, ErrReset) {
			t.Fatalf("refused: %v", err)
		}

		// origin (new) -> hop (new, Forward) -> old far end.
		onward := dial()
		hc, oc := v2pair(t, Config{Window: testWindow, Auth: testToken}, Config{Window: testWindow, Auth: testToken})
		om, hm := NewMux(hc), NewMux(oc)
		go func() { _ = om.Serve(nil) }()
		go func() {
			_ = hm.Serve(Forward(func(ctx context.Context, in *Session) (*Session, error) {
				ch, _ := ChainFromContext(ctx)
				return onward.OpenChain(in.Header(), ch)
			}, ForwardConfig{}))
		}()
		s, _ = om.Open([]byte("echo"))
		if err := s.Answered(context.Background()); err != nil || s.PathLen() != 1 {
			t.Fatalf("through a hop: %v, PathLen %d", err, s.PathLen())
		}
		roundTrip(t, s, 1<<20)
		r, _ = om.Open([]byte("refuse"))
		_ = r.Answered(context.Background())
		_, err = io.ReadAll(r)
		if a := errAbort(t, err); a.Code != CodeUnspecified || a.Hops != 1 {
			t.Fatalf("refused through a hop: %+v", a)
		}
	})

	t.Run("old dials new", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			c, _, err := Server(context.Background(), nc, Config{Auth: NoAuth})
			if err != nil {
				return
			}
			_ = NewMux(c).Serve(func(h []byte, s *Session) {
				if s.Answers() {
					_ = s.Abort(CodeApp, "answers with v0.9.4")
					return
				}
				if string(h) == "refuse" {
					_ = s.Refuse(CodeApp, "no") // a reset to v0.9.4
					return
				}
				farEcho(h, s)
			})
		}()
		out, err := exec.Command(bin, "dial", ln.Addr().String()).CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "ok" {
			t.Fatalf("%v: %s", err, out)
		}
	})
}
