module github.com/nfltr/link/soak

go 1.27.0

require (
	github.com/nfltr/link v0.13.3
	github.com/nfltr/link/peerlink/quiclink v0.4.0
	github.com/nfltr/quic-go v0.63.1-nfltr.8
)

require (
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/nfltr/link => ../

replace github.com/nfltr/link/peerlink/quiclink => ../peerlink/quiclink
