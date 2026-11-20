// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package pathswitch

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

// vector is one entry of testdata/wire_vectors.json (WIRE.md "Test
// vectors"): a valid frame's fields and bytes, or bytes a reader refuses.
type vector struct {
	Name    string `json:"name"`
	Type    string `json:"type,omitempty"`
	N       uint64 `json:"n,omitempty"`
	Epoch   uint64 `json:"epoch,omitempty"`
	Abort   bool   `json:"abort,omitempty"`
	Token   string `json:"token,omitempty"`
	Data    string `json:"data,omitempty"`
	Invalid string `json:"invalid,omitempty"`
	Hex     string `json:"hex"`
}

func (v vector) frame(t *testing.T) frame {
	tok, err := hex.DecodeString(v.Token)
	if err != nil {
		t.Fatal(err)
	}
	data, err := hex.DecodeString(v.Data)
	if err != nil {
		t.Fatal(err)
	}
	f := frame{t: v.Type[0], n: v.N, epoch: v.Epoch, abort: v.Abort, token: tok, data: data}
	return f
}

// TestWireVectors: every valid vector's fields encode to its bytes and its
// bytes read back to its fields, consuming all of them; every invalid
// vector's bytes are refused with its error (ErrProtocol, or io errors for
// a truncated frame: "truncated").
func TestWireVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/wire_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vs []vector
	if err := json.Unmarshal(b, &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range vs {
		raw, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		r := bufio.NewReader(bytes.NewReader(raw))
		var buf []byte
		got, err := readFrame(r, &buf)
		if v.Invalid != "" {
			switch {
			case v.Invalid == "ErrProtocol" && errors.Is(err, ErrProtocol):
			case v.Invalid == "truncated" && err != nil && !errors.Is(err, ErrProtocol):
			default:
				t.Errorf("%s: read %v, want %s", v.Name, err, v.Invalid)
			}
			continue
		}
		want := v.frame(t)
		if enc := encodeFrame(want); !bytes.Equal(enc, raw) {
			t.Errorf("%s: encodes to %x, want %x", v.Name, enc, raw)
		}
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		if r.Buffered() != 0 {
			t.Errorf("%s: %d bytes left over", v.Name, r.Buffered())
		}
		if got.t != want.t || got.n != want.n || got.epoch != want.epoch || got.abort != want.abort ||
			!bytes.Equal(got.token, want.token) || !bytes.Equal(got.data, want.data) {
			t.Errorf("%s: read %+v, want %+v", v.Name, got, want)
		}
	}
}
