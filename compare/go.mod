module github.com/nfltr/link/compare

go 1.27.0

require (
	github.com/hashicorp/yamux v0.1.2
	github.com/nfltr/link v0.9.0
	github.com/nfltr/link/peerlink/quiclink v0.3.0
	github.com/quic-go/quic-go v0.63.1-0.20261014084224-4b404ee15e68
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/nfltr/link => ../

replace github.com/nfltr/link/peerlink/quiclink => ../peerlink/quiclink
