module vortex/test/e2e

go 1.24.7

require (
	vortex/edgehub v0.0.0
	vortex/engine v0.0.0
	vortex/node v0.0.0
)

replace (
	vortex/edgehub => ../../edge-hub
	vortex/engine => ../../engine
	vortex/node => ../../node
)
