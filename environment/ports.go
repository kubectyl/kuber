package environment

import (
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
)

// Defines the allocations available for a given server. When using the Docker environment
// driver these correspond to mappings for the container that allow external connections.
type Allocations struct {
	// Defines the default allocation that should be used for this server. This is
	// what will be used for {SERVER_IP} and {SERVER_PORT} when modifying configuration
	// files or the startup arguments for a server.
	DefaultPort int `json:"default_port"`

	// Mappings contains all the ports that should be assigned to a given server
	// attached to the IP they correspond to.
	AdditionalPorts []string `json:"additional_ports"`
}

// Converts the server allocation mappings into a format that can be understood by Docker. While
// we do strive to support multiple environments, using Docker's standardized format for the
// bindings certainly makes life a little easier for managing things.
//
// You'll want to use DockerBindings() if you need to re-map 127.0.0.1 to the Docker interface.
func (a *Allocations) Bindings() nat.PortMap {
	out := nat.PortMap{}

	for _, port := range a.AdditionalPorts {
		intVar, _ := strconv.Atoi(port)

		// Skip over invalid ports.
		if intVar < 1 || intVar > 65535 {
			continue
		}

		binding := nat.PortBinding{
			HostPort: port,
		}

		tcp := nat.Port(fmt.Sprintf("%s/tcp", port))
		udp := nat.Port(fmt.Sprintf("%s/udp", port))

		out[tcp] = append(out[tcp], binding)
		out[udp] = append(out[udp], binding)
	}

	return out
}
