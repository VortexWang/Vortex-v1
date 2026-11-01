module vortex/test/e2e

go 1.24.7

require (
	google.golang.org/grpc v1.70.0
	google.golang.org/protobuf v1.36.6
	vortex/edgehub v0.0.0
	vortex/engine v0.0.0
	vortex/node v0.0.0
	vortex/proto/gen/go v0.0.0
)

require (
	golang.org/x/net v0.32.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241202173237-19429a94021a // indirect
)

replace (
	vortex/edgehub => ../../edge-hub
	vortex/engine => ../../engine
	vortex/node => ../../node
)

replace vortex/proto/gen/go => ../../proto/gen/go
