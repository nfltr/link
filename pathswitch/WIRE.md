# pathswitch wire format

The bytes `github.com/nfltr/link/pathswitch` puts on each stream (epoch)
of a session. pathswitch is a module of its own and stays v0 (outside
link's v1 promise); test vectors for every frame are in
[`testdata/wire_vectors.json`](testdata/wire_vectors.json)
(`TestWireVectors`). The protocol (who sends what, when) is `doc.go` and
`spec/DirectPath.tla` in link.

## Frames

One type byte, then unsigned varints (LEB128,
`encoding/binary.AppendUvarint`), except OPEN's flags, one byte.

| type | name | body | rules |
|---|---|---|---|
| `D` (0x44) | DATA | len, payload | len 1 to 16384 |
| `K` (0x4b) | ACK | n | the peer holds every position below n |
| `O` (0x4f) | OPEN | epoch, rcvd, flags(1), token_len, token | token at most 256 bytes; flags bit 0: the client reset the session; any other bit is a protocol error |
| `R` (0x52) | RESUME | n | the server holds every position below n |
| `E` (0x45) | END | n | the sender's last byte is n-1 (its END is position n) |
| `X` (0x58) | ABORT | (none) | the session's reset |

Any other type, a DATA of 0 or over 16384 bytes, an OPEN with an unknown
flag or a token over 256 bytes is a protocol error (`ErrProtocol`): the
reader ends the stream. Positions count bytes from the session's start,
and the END as one more.

## Versions (decision, lane LINKV013, 2026-11-20)

The wire carries no version field, and none is added in v0. The caller
already names what a stream carries before pathswitch reads it (nf1: the
client's first byte after the open's answer marks a framed session, a
re-attach or a control exchange, relay `client/agent/direct_nf1.go`;
nfltr `docs/decisions/direct-paths-2026-11-11.md`), so a new pathswitch
wire is a new mark there, chosen before the first OPEN, as link
negotiates its own wire in the hello. Inside the wire the
extension point is OPEN's flags: a reader refuses an unknown bit, so a
client sets a new bit only toward a server whose caller announced it.
Rejected: a version byte in OPEN. A server that does not speak a version
could only refuse it after the client sent its OPEN, the same round trip
the caller's negotiation already pays, and every frame after OPEN would
still need the caller to agree. pathswitch v1 freezes this table and
these vectors.
