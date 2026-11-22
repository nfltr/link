module github.com/nfltr/link/peerlink/quiclink

go 1.27.0

replace github.com/nfltr/link => ../../

require (
	github.com/nfltr/link v0.14.0
	github.com/nfltr/link/pathswitch v0.1.5
	github.com/nfltr/quic-go v0.63.1-nfltr.10
	golang.org/x/net v0.62.0
)

require (
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/nfltr/link/pathswitch => ../../pathswitch
