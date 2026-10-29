module github.com/nfltr/link/compare

go 1.27.0

require (
	github.com/hashicorp/yamux v0.1.2
	github.com/nfltr/link v0.10.18
	github.com/nfltr/link/peerlink/quiclink v0.4.0
	github.com/nfltr/quic-go v0.63.1-nfltr.2
	golang.ngrok.com/muxado/v2 v2.0.2
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/nfltr/link => ../

replace github.com/nfltr/link/peerlink/quiclink => ../peerlink/quiclink
