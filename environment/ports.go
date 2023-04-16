package environment

import (
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
)

type Ports struct {
	// Defines the default port that should be used for this server.
	DefaultMapping struct {
		Port int `json:"port"`
	} `json:"default"`

	// Mappings contains all the additional ports that should be assigned to a given server.
	Mappings []string `json:"mappings"`
}

func (p *Ports) Bindings() nat.PortMap {
	out := nat.PortMap{}

	for _, port := range p.Mappings {
		if p, err := strconv.Atoi(port); err == nil && p >= 1 && p <= 65535 {
			binding := nat.PortBinding{HostPort: strconv.Itoa(p)}
			out[nat.Port(fmt.Sprintf("%d/tcp", p))] = append(out[nat.Port(fmt.Sprintf("%d/tcp", p))], binding)
			out[nat.Port(fmt.Sprintf("%d/udp", p))] = append(out[nat.Port(fmt.Sprintf("%d/udp", p))], binding)
		}
	}

	if p.DefaultMapping.Port > 0 {
		p := p.DefaultMapping.Port
		binding := nat.PortBinding{HostPort: strconv.Itoa(p)}
		out[nat.Port(fmt.Sprintf("%d/tcp", p))] = append(out[nat.Port(fmt.Sprintf("%d/tcp", p))], binding)
		out[nat.Port(fmt.Sprintf("%d/udp", p))] = append(out[nat.Port(fmt.Sprintf("%d/udp", p))], binding)
	} else {
		return nil
	}

	return out
}
