module github.com/nfltr/link/soak

go 1.27.0

require (
	github.com/nfltr/link v0.11.2
	github.com/nfltr/link/peerlink/quiclink v0.4.0
	github.com/nfltr/quic-go v0.63.1-nfltr.2.0.20261030112406-3f0877f63743
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/nfltr/link => ../

replace github.com/nfltr/link/peerlink/quiclink => ../peerlink/quiclink
