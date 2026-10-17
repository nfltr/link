module github.com/nfltr/link/soak

go 1.27.0

require (
	github.com/nfltr/link v0.10.11
	github.com/nfltr/link/peerlink/quiclink v0.4.0
	github.com/nfltr/quic-go v0.63.1-nfltr.1.0.20261017085836-a0728504a86f
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/nfltr/link => ../

replace github.com/nfltr/link/peerlink/quiclink => ../peerlink/quiclink
