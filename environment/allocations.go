package environment

import (
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
)

// Defines the allocations available for a given server. When using the Docker environment
// driver these correspond to mappings for the container that allow external connections.
type Allocations struct {
	// ForceOutgoingIP causes a dedicated bridge network to be created for the
	// server with a special option, causing Docker to SNAT outgoing traffic to
	// the DefaultMapping's IP. This is important to servers which rely on external
	// services that check the IP of the server (Source Engine servers, for example).
	ForceOutgoingIP bool `json:"force_outgoing_ip"`
	// Defines the default allocation that should be used for this server. This is
	// what will be used for {SERVER_IP} and {SERVER_PORT} when modifying configuration
	// files or the startup arguments for a server.
	DefaultMapping struct {
		Ip   string `json:"ip"`
		Port int    `json:"port"`
	} `json:"default"`

	// Mappings contains all the ports that should be assigned to a given server
	// attached to the IP they correspond to.
	Mappings map[string][]int `json:"mappings"`
}

// Converts the server allocation mappings into a format that can be understood by Kubernetes.
func (a *Allocations) Bindings() nat.PortMap {
	out := nat.PortMap{}

	for ip, ports := range a.Mappings {
		for _, port := range ports {
			if port >= 1 && port <= 65535 {
				binding := nat.PortBinding{
					HostIP:   ip,
					HostPort: strconv.Itoa(port),
				}
				tcp := nat.Port(fmt.Sprintf("%d/tcp", port))
				udp := nat.Port(fmt.Sprintf("%d/udp", port))
				out[tcp] = append(out[tcp], binding)
				out[udp] = append(out[udp], binding)
			}
		}
	}

	if a.DefaultMapping.Port <= 0 {
		return nil
	}

	binding := nat.PortBinding{
		HostIP:   a.DefaultMapping.Ip,
		HostPort: strconv.Itoa(a.DefaultMapping.Port),
	}

	tcp := nat.Port(fmt.Sprintf("%d/tcp", a.DefaultMapping.Port))
	udp := nat.Port(fmt.Sprintf("%d/udp", a.DefaultMapping.Port))

	out[tcp] = append(out[tcp], binding)
	out[udp] = append(out[udp], binding)

	return out
}
