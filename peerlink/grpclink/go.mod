module github.com/nfltr/link/peerlink/grpclink

go 1.27.0

replace github.com/nfltr/link => ../../

require (
	github.com/nfltr/link v0.9.4
	google.golang.org/grpc v1.84.0
)

require (
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
