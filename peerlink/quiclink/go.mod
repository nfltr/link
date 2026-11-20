module github.com/nfltr/link/peerlink/quiclink

go 1.27.0

replace github.com/nfltr/link => ../../

require (
	github.com/nfltr/link v0.12.20
	github.com/nfltr/quic-go v0.63.1-nfltr.2.0.20261112202211-40e0a2570e52
)

require (
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
