package environment

import (
	"github.com/kubectyl/kuber/config"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func Cluster() (c *rest.Config, clientset *kubernetes.Clientset, err error) {
	cfg := config.Get().Cluster

	c = &rest.Config{
		Host:        cfg.Host,
		BearerToken: cfg.BearerToken,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: cfg.Insecure,
		},
	}

	client, err := kubernetes.NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return c, client, err
}
