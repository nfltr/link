module github.com/nfltr/link/peerlink/quiclink

go 1.27.0

replace github.com/nfltr/link => ../../

require (
	github.com/nfltr/link v0.10.11
	github.com/nfltr/quic-go v0.63.1-nfltr.1
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
