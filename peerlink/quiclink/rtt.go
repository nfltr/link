// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package quiclink

import "time"

// MinRTT is the least round trip QUIC measured on the session's connection
// (0 before a sample). A pathswitch session on it grows its window
// (pathswitch.RoundTripper, Config.MaxWindow): the transport's round trip,
// which no reader holds back.
func (s *Session) MinRTT() time.Duration { return s.c.q.ConnectionStats().MinRTT }
