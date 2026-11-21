module github.com/nfltr/link/peerlink/quiclink

go 1.27.0

replace github.com/nfltr/link => ../../

require (
	github.com/nfltr/link v0.13.2
	github.com/nfltr/link/pathswitch v0.1.2
	github.com/nfltr/quic-go v0.63.1-nfltr.3.0.20261121073520-3fae9562c1d8
	golang.org/x/net v0.62.0
)

require (
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/nfltr/link/pathswitch => ../../pathswitch
