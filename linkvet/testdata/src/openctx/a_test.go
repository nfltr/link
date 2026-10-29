package openctx

import (
	"context"
	"testing"

	"github.com/nfltr/link"
)

func TestOpen(t *testing.T) {
	var p *link.Pool
	s, _ := p.Open(context.Background(), nil) // a test: no report
	defer s.Close()
}
