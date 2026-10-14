# link wire format, version 2

This is the specification of the bytes `github.com/nfltr/link` puts on a
connection. Version 2 is the only version since v0.5.0. Test vectors for
every message below are in [`testdata/wire_vectors.json`](testdata/wire_vectors.json);
`testdata/v2_session.golden` holds a recorded session. Both files are
written from the code and every release is tested against them byte for
byte (`TestWireVectors`, `TestV2Golden`).

All integers are unsigned and big endian. Sizes are in bytes.

## 1. The connection

link runs over one reliable, ordered byte stream that the caller dials and
secures (TLS, mutual TLS, an SSH channel, a pipe). The stream carries a
**hello** from the side that dialled (the *dialer*), a **reply** from the
side that accepted (the *listener*), then **frames** both ways until either
side closes the stream. Nothing in the wire is encrypted or integrity
protected by link itself: that is the stream's job.

## 2. Hello (dialer to listener)

| offset | size | field | value |
|---:|---:|---|---|
| 0 | 4 | magic | `NFDP` (`4e 46 44 50`) |
| 4 | 1 | version | `2` |
| 5 | 1 | max_version | the highest version the dialer speaks (informational) |
| 6 | 4 | features | feature bits the dialer reads (section 6) |
| 10 | 4 | window | how many bytes of one session the dialer buffers |
| 14 | 4 | max_sessions | how many sessions the listener may have open toward the dialer at once |
| 18 | 4 | interval_ms | liveness interval, milliseconds |
| 22 | 4 | timeout_ms | liveness timeout, milliseconds |
| 26 | 1 | auth_method | 0 none, 1 a bearer token the listener issued, 2-127 reserved, 128-255 the application's |
| 27 | 2 | auth_len | length of auth, at most 4096 |
| 29 | auth_len | auth | the credential, opaque to link |

A hello is at most 29 + 4096 = 4125 bytes. The magic lets a listener share
a port with TLS or HTTP/2 (`IsHello`): an HTTP/2 connection starts `PRI `.

The listener refuses (and closes the stream) a hello whose:

- magic is not `NFDP`, or whose version is 0 (malformed);
- auth_len is over 4096;
- window is outside [131072, 2^30] (`MinWindow` = 2 x 64 KiB, `MaxWindow`);
- max_sessions is outside [1, 2^20];
- interval_ms is outside [1000, 15000], or timeout_ms is under interval_ms.

## 3. Reply (listener to dialer)

The first byte is a status:

| status | meaning | rest |
|---:|---|---|
| 0 | bound | version(1) = 2, features(4), window(4), max_sessions(4): 14 bytes in all |
| 1 | refused: the credential was not accepted | nothing; the listener closes the stream |
| 2 | version: the hello's version is not spoken | max_version(1), the highest version the listener speaks; it closes the stream |

A listener that receives a version-1 hello (link v0.4 and older) replies
`02 02`; such a dialer reads it as a refusal. A listener of link v0.4 or
older that speaks only version 1 reads 37 bytes of a version-2 hello and
closes the stream without a reply.

The dialer refuses a bound reply whose version is not 2, whose window is
out of range or whose max_sessions is out of range (the hello's bounds),
and any other status (malformed).

Once bound, each side has the other's window, max_sessions and features.
Liveness is the dialer's unless the listener configured its own
(section 8).

## 4. Frames

Every frame, both ways:

| size | field |
|---:|---|
| 4 | length: of the body, at least 1 and at most 1 + 255 + 2^20 = 1048832 |
| 1 | type |
| length | body |

A session id (*sid*) in a body is `sid_len(1) sid(sid_len)`, at most 255
bytes. Any violation of a rule below is a protocol error: the receiver
ends the connection (`ErrFrame`, `ErrCredit` or `ErrFull`).

| type | name | body | rules |
|---:|---|---|---|
| 0 | data | sid, payload | sid_len at most length - 1; the payload may be empty; at most 2^20 per frame (senders split) |
| 1 | message | the caller's bytes | never empty; carried only between callers that drive a `Conn` themselves: a `Mux` drops them |
| 2 | window | sid, increment(4) | body exactly 5 + sid_len |
| 3 | open | sid, header | header at most 65536 |
| 4 | close | sid, code(1) | body exactly 2 + sid_len; code 0, 1 or 2 (section 5) |
| 5 | ping | 8 opaque bytes | body exactly 8 |
| 6 | pong | the ping's 8 bytes | body exactly 8 |
| 7 | goaway | drain_ms(4), reason | reason UTF-8, at most 1024; body at least 4 |
| 8-255 | | | unknown: a protocol error |

Data, message and open frames surface to the receiver's caller (`Recv`);
window, ping, pong and goaway frames are applied by the receiver itself.
Session ids that begin with `_` are left to the caller by a `Mux` (it
never picks one: its ids are uvarints, section 5).

## 5. Sessions

A session is a pair of byte streams identified by its sid.

- **Open.** A side opens a session with an open frame; its header is the
  opener's bytes (what the session is for). The open starts the session's
  credit on both sides (section 7). An open past the receiver's
  max_sessions (sessions opened by the peer and not yet closed) is a
  protocol error (`ErrFull`). An open of an id that is already open starts
  it again: the old session ends.
- **Data.** Data frames carry a session's bytes in order, on credit.
  Data for a session the receiver does not know is dropped and not
  charged (it may have crossed a close).
- **Close.** A close frame's code:
  - `0`: the sender neither sends nor reads more of the session; its
    credit ends on both sides.
  - `1` (half-close): the sender sends no more; it still reads. The
    receiver reads to the end, then end of stream. A session both sides
    half-closed has ended.
  - `2` (reset): as 0, and the session failed: the receiver's reads
    return an error, never a clean end of stream. Sent only to a peer
    that announced `FeatureReset`; to another, a reset is sent as code 0.
- **Ids in a Mux.** A `Mux` picks ids as unsigned varints (LEB128,
  `encoding/binary.AppendUvarint`), odd on the dialer and even on the
  listener, so the two sides never pick the same; a `Mux` refuses an open
  it does not take (no accept function, or after GOAWAY) with a reset.
  A `Conn` without a `Mux` leaves ids to the caller.

## 6. Feature bits

Each side announces, in its hello or reply, the bits it reads; it ignores
bits it does not know, and sends a feature's frames only to a peer that
announced it.

| bit | name | since | meaning |
|---:|---|---|---|
| 0 | `FeatureReset` | v0.3.0 | the side reads close code 2 |

Bits 1-31 are reserved.

## 7. Credit (per-session flow control)

Each side's window is how many bytes of one session it buffers. A sender
may have at most the peer's window of a session's data that the peer has
not yet returned:

- At open, each side's send credit for the session is the peer's window.
- A sender takes credit before it sends data (at most 64 KiB at once,
  `MaxChunk`) and waits while it has none.
- A receiver returns credit with a window frame once bytes left its
  buffer (its application read them), in increments of at least a quarter
  of its window.
- Data past a session's window, or a window frame that would raise the
  sender's credit past the receiver's own window, is a protocol error
  (`ErrCredit`). A window frame for an unknown session is ignored.

A connection's buffered bytes are therefore at most max_sessions x window
per direction. A session whose reader is slow stalls only itself.

## 8. Liveness

Liveness comes from the dialer's hello (interval, timeout) unless the
listener configured its own. Each side:

- sends a ping once it received nothing for the interval;
- ends the connection (`ErrDeadPeer`) once nothing arrived for the timeout
  after that ping, so a silent peer is dead within interval + timeout of
  its last frame, whether the connection is idle or busy;
- answers every ping with a pong carrying the ping's 8 bytes, after every
  frame it sent before it read the ping. When several pings wait, it
  answers the latest (one answer per batch).

The 8 bytes are the pinger's own (link sends a monotonic timestamp and
reads back the round trip); a receiver treats them as opaque.

## 9. GOAWAY (drain)

A goaway frame tells the receiver to open no more sessions on the
connection; drain_ms is how long the sender lets the open ones run (0: no
deadline) and the reason is informational. After sending or receiving a
goaway, a side opens nothing more on the connection.

The sender follows its goaway with a ping. Opens that arrive before that
ping's pong crossed the goaway and run like any other session (since
v0.4.2; before, they were refused). An open after the pong breaks the
protocol and is refused with a reset. The sender closes the connection
once its last session ended and the pong arrived, after writing every
frame it buffered, or at its drain deadline.

## 10. Limits

| limit | value | constant |
|---|---:|---|
| hello auth | 4096 | `MaxAuth` |
| window | 131072 to 2^30 | `MinWindow`, `MaxWindow` (default `DefaultWindow` 1 MiB) |
| max_sessions on the wire | 1 to 2^20 | (default `MaxSessions` 1024) |
| liveness interval | 1 s to 15 s | `MinLivenessInterval`, `MaxLivenessInterval` |
| frame body | 1 to 1048832 | |
| data per frame | 2^20 | |
| sid | 255 | `MaxSID` |
| open header | 65536 | |
| goaway reason | 1024 | |
| credit taken at once | 65536 | `MaxChunk` |

## 11. Test vectors

[`testdata/wire_vectors.json`](testdata/wire_vectors.json) is an array of
objects:

```json
{"name": "close-half", "kind": "frame",
 "frame": {"type": "close", "sid": "01", "code": 1},
 "hex": "0000000304010101"}
```

- `kind` is `hello`, `reply` or `frame`; `hex` the bytes.
- A valid vector has its fields in `hello`, `reply` or `frame` (byte
  fields in hex): they encode to `hex`, and `hex` decodes to them.
- An invalid vector has `invalid`: the error a decoder refuses `hex` with
  (`ErrFrame`, `ErrCredit`, `ErrV1`, `ErrVersion`, or `error` for any).
- Frame vectors are decoded on a connection where session `01` is open
  with its full credit, the listener's window 262144, and every byte of
  `hex` must be consumed. Window frames are decoded after the session
  sent `increment` bytes.

An implementation in another language can run the same vectors.
