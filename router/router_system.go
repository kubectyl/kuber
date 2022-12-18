package router

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apex/log"
	"github.com/gin-gonic/gin"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/router/middleware"
	"github.com/kubectyl/kuber/server"
	"github.com/kubectyl/kuber/server/installer"
	"github.com/kubectyl/kuber/system"
)

// Returns information about the system that wings is running on.
func getSystemInformation(c *gin.Context) {
	i, err := system.GetSystemInformation()
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	if c.Query("v") == "2" {
		c.JSON(http.StatusOK, i)
		return
	}

	cfg := config.Get().Cluster

	config := &rest.Config{
		Host:        cfg.Host,
		BearerToken: cfg.BearerToken,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: cfg.Insecure,
		},
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		fmt.Printf(" error in discoveryClient %v", err)
	}

	information, err := discoveryClient.ServerVersion()
	if err != nil {
		fmt.Println("Error while fetching server version information", err)
	}

	c.JSON(http.StatusOK, struct {
		Architecture  string `json:"architecture"`
		CPUCount      int    `json:"cpu_count"`
		KernelVersion string `json:"kernel_version"`
		OS            string `json:"os"`
		Version       string `json:"version"`
		Git           string `json:"git_version"`
		Go            string `json:"go_version"`
		Platform      string `json:"platform"`
	}{
		Architecture:  i.System.Architecture,
		CPUCount:      i.System.CPUThreads,
		KernelVersion: i.System.KernelVersion,
		OS:            i.System.OSType,
		Version:       i.Version,
		Git:           information.GitVersion,
		Go:            information.GoVersion,
		Platform:      information.Platform,
	})
}

// Returns all the servers that are registered and configured correctly on
// this wings instance.
func getAllServers(c *gin.Context) {
	servers := middleware.ExtractManager(c).All()
	out := make([]server.APIResponse, len(servers), len(servers))
	for i, v := range servers {
		out[i] = v.ToAPIResponse()
	}
	c.JSON(http.StatusOK, out)
}

// Creates a new server on the wings daemon and begins the installation process
// for it.
func postCreateServer(c *gin.Context) {
	manager := middleware.ExtractManager(c)

	details := installer.ServerDetails{}
	if err := c.BindJSON(&details); err != nil {
		return
	}

	install, err := installer.New(c.Request.Context(), manager, details)
	if err != nil {
		if installer.IsValidationError(err) {
			c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
				"error": "The data provided in the request could not be validated.",
			})
			return
		}

		middleware.CaptureAndAbort(c, err)
		return
	}

	// Plop that server instance onto the request so that it can be referenced in
	// requests from here-on out.
	manager.Add(install.Server())

	// Begin the installation process in the background to not block the request
	// cycle. If there are any errors they will be logged and communicated back
	// to the Panel where a reinstall may take place.
	go func(i *installer.Installer) {
		if err := i.Server().CreateEnvironment(); err != nil {
			i.Server().Log().WithField("error", err).Error("failed to create server environment during install process")
			return
		}

		if err := i.Server().Install(); err != nil {
			log.WithFields(log.Fields{"server": i.Server().ID(), "error": err}).Error("failed to run install process for server")
			return
		}

		if i.StartOnCompletion {
			log.WithField("server_id", i.Server().ID()).Debug("starting server after successful installation")
			if err := i.Server().HandlePowerAction(server.PowerActionStart, 30); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					log.WithFields(log.Fields{"server_id": i.Server().ID(), "action": "start"}).Warn("could not acquire a lock while attempting to perform a power action")
				} else {
					log.WithFields(log.Fields{"server_id": i.Server().ID(), "action": "start", "error": err}).Error("encountered error processing a server power action in the background")
				}
			}
		} else {
			log.WithField("server_id", i.Server().ID()).Debug("skipping automatic start after successful server installation")
		}
	}(install)

	c.Status(http.StatusAccepted)
}

// Updates the running configuration for this Wings instance.
func postUpdateConfiguration(c *gin.Context) {
	cfg := config.Get()
	if err := c.BindJSON(&cfg); err != nil {
		return
	}

	// Keep the SSL certificates the same since the Panel will send through Lets Encrypt
	// default locations. However, if we picked a different location manually we don't
	// want to override that.
	//
	// If you pass through manual locations in the API call this logic will be skipped.
	if strings.HasPrefix(cfg.Api.Ssl.KeyFile, "/etc/letsencrypt/live/") {
		cfg.Api.Ssl.KeyFile = config.Get().Api.Ssl.KeyFile
		cfg.Api.Ssl.CertificateFile = config.Get().Api.Ssl.CertificateFile
	}

	// Try to write this new configuration to the disk before updating our global
	// state with it.
	if err := config.WriteToDisk(cfg); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	// Since we wrote it to the disk successfully now update the global configuration
	// state to use this new configuration struct.
	config.Set(cfg)
	c.Status(http.StatusNoContent)
}
