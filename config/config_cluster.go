package config

import "sort"

type ClusterConfiguration struct {
	Namespace string `default:"default" yaml:"namespace"`

	Host string `yaml:"host"`

	BearerToken string `json:"bearer_token" yaml:"bearer_token"`

	ServiceType string `default:"nodeport" yaml:"service_type"`

	StorageClass string `default:"manual" yaml:"storage_class"`

	Insecure bool `yaml:"insecure" default:"false"`

	Network ClusterNetworkConfiguration `json:"network" yaml:"network"`

	// InstallerLimits defines the limits on the installer containers that prevents a server's
	// installation process from unintentionally consuming more resources than expected. This
	// is used in conjunction with the server's defined limits. Whichever value is higher will
	// take precedence in the installer containers.
	InstallerLimits struct {
		Memory int64 `default:"1024" json:"memory" yaml:"memory"`
		Cpu    int64 `default:"100" json:"cpu" yaml:"cpu"`
	} `json:"installer_limits" yaml:"installer_limits"`

	// Overhead controls the memory overhead given to all containers to circumvent certain
	// software such as the JVM not staying below the maximum memory limit.
	Overhead Overhead `json:"overhead" yaml:"overhead"`

	// CertData string `yaml:"certdata"`

	// KeyData string `yaml:"keydata"`

	// CAData string `yaml:"cadata"`
}

type ClusterNetworkConfiguration struct {
	Dns []string `default:"[\"1.1.1.1\", \"1.0.0.1\"]"`
}

// Overhead controls the memory overhead given to all containers to circumvent certain
// software such as the JVM not staying below the maximum memory limit.
type Overhead struct {
	// Override controls if the overhead limits should be overridden by the values in the config file.
	Override bool `default:"false" json:"override" yaml:"override"`

	// DefaultMultiplier sets the default multiplier for if no Multipliers are able to be applied.
	DefaultMultiplier float64 `default:"1.05" json:"default_multiplier" yaml:"default_multiplier"`

	// Multipliers allows overriding DefaultMultiplier depending on the amount of memory
	// configured for a server.
	//
	// Default values (used if Override is `false`)
	// - Less than 2048 MB of memory, multiplier of 1.15 (15%)
	// - Less than 4096 MB of memory, multiplier of 1.10 (10%)
	// - Otherwise, multiplier of 1.05 (5%) - specified in DefaultMultiplier
	//
	// If the defaults were specified in the config they would look like:
	// ```yaml
	// multipliers:
	//   2048: 1.15
	//   4096: 1.10
	// ```
	Multipliers map[int]float64 `json:"multipliers" yaml:"multipliers"`
}

func (o Overhead) GetMultiplier(memoryLimit int64) float64 {
	// Default multiplier values.
	if !o.Override {
		if memoryLimit <= 2048 {
			return 1.15
		} else if memoryLimit <= 4096 {
			return 1.10
		}
		return 1.05
	}

	// This plucks the keys of the Multipliers map, so they can be sorted from
	// smallest to largest in order to correctly apply the proper multiplier.
	i := 0
	multipliers := make([]int, len(o.Multipliers))
	for k := range o.Multipliers {
		multipliers[i] = k
		i++
	}
	sort.Ints(multipliers)

	// Loop through the memory values in order (smallest to largest)
	for _, m := range multipliers {
		// If the server's memory limit exceeds the modifier's limit, don't apply it.
		if memoryLimit > int64(m) {
			continue
		}
		return o.Multipliers[m]
	}

	return o.DefaultMultiplier
}