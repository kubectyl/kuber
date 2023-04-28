package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"emperror.dev/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/prometheus/client_golang/api"

	pv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

// Uptime returns the current uptime of the container in milliseconds. If the
// container is not currently running this will return 0.
func (e *Environment) Uptime(ctx context.Context) (int64, error) {
	ins, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "environment: could not get pod")
	}
	if ins.Status.Phase != v1.PodRunning {
		return 0, nil
	}
	started, err := time.Parse(time.RFC3339, ins.Status.StartTime.Format(time.RFC3339))
	if err != nil {
		return 0, errors.Wrap(err, "environment: failed to parse pod start time")
	}
	return time.Since(started).Milliseconds(), nil
}

// Attach to the instance and then automatically emit an event whenever the resource usage for the
// server process changes.
func (e *Environment) pollResources(ctx context.Context) error {
	if e.st.Load() == environment.ProcessOfflineState {
		return errors.New("cannot enable resource polling on a stopped server")
	}

	e.log().Info("starting resource polling for container")
	defer e.log().Debug("stopped resource polling for container")

	uptime, err := e.Uptime(ctx)
	if err != nil {
		e.log().WithField("error", err).Warn("failed to calculate pod uptime")
	}

	req := e.client.CoreV1().RESTClient().
		Get().
		Namespace(config.Get().Cluster.Namespace).
		Resource("pods").
		Name(e.Id).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: "process",
			Command:   []string{"bash", "-c", "while true; do cat /proc/net/dev | grep eth0 | awk '{print $2,$10}'; sleep 1; done"},
			Stdin:     false,
			Stdout:    true,
			Stderr:    false,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return err
	}

	r, w, _ := os.Pipe()

	go func() {
		err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdout: w,
		})
	}()

	rbuf := bufio.NewReader(r)

	var cpuQuery string
	var memoryQuery string
	var promAPI pv1.API
	var mc *metrics.Clientset

	cfg := config.Get().Cluster
	switch cfg.Metrics {
	case "prometheus":
		// Set up the Prometheus API client
		promURL := cfg.PrometheusAddress
		promClient, err := api.NewClient(api.Config{
			Address: promURL,
		})
		if err != nil {
			return err
		}
		promAPI = pv1.NewAPI(promClient)

		// Define the queries to retrieve the CPU and memory usage of a container in a pod
		cpuQuery = fmt.Sprintf("sum(rate(container_cpu_usage_seconds_total{namespace=\"%s\",pod=\"%s\",container=\"process\"}[5m]))", config.Get().Cluster.Namespace, e.Id)
		memoryQuery = fmt.Sprintf("sum(container_memory_working_set_bytes{namespace=\"%s\",pod=\"%s\",container=\"process\"})", config.Get().Cluster.Namespace, e.Id)

	default:
		mc, err = metrics.NewForConfig(e.config)
		if err != nil {
			return err
		}
	}

	for {
		// Disable collection if the server is in an offline state and this process is still running.
		if e.st.Load() == environment.ProcessOfflineState {
			e.log().Debug("process in offline state while resource polling is still active; stopping poll")
			break
		}

		var cpu string
		var memory int64

		switch cfg.Metrics {
		case "prometheus":
			cpuResult, _, err := promAPI.Query(ctx, cpuQuery, time.Now())
			if err != nil {
				return err
			}
			if vec, ok := cpuResult.(model.Vector); ok && len(vec) > 0 {
				cpu = vec[0].Value.String()
			}

			memoryResult, _, err := promAPI.Query(ctx, memoryQuery, time.Now())
			if err != nil {
				return err
			}
			if vec, ok := memoryResult.(model.Vector); ok && len(vec) > 0 {
				memory = int64(vec[0].Value)
			}
		default:
			// Don't throw an error if pod metrics are not available yet,
			// just keep trying until the server context is canceled.
			podMetrics, _ := mc.MetricsV1beta1().PodMetricses(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})

			if len(podMetrics.Containers) != 0 {
				for i, c := range podMetrics.Containers {
					if c.Name == "process" {
						cpu = podMetrics.Containers[i].Usage.Cpu().AsDec().String()
						memory, _ = podMetrics.Containers[i].Usage.Memory().AsInt64()
					}
					continue
				}
			}
		}

		uptime = uptime + 1000
		f, _ := strconv.ParseFloat(cpu, 32)

		st := environment.Stats{
			Uptime:      uptime,
			Memory:      uint64(memory),
			CpuAbsolute: f * 100,
			Network:     environment.NetworkStats{},
		}

		// Make sure we still receive any network data
		b, err := rbuf.ReadBytes('\n')
		if err == io.EOF {
			continue
		}

		bytes := strings.Fields(string(bytes.TrimSpace(b)))
		if len(bytes) != 0 {
			rxBytes, _ := strconv.ParseUint(bytes[0], 10, 64)
			txBytes, _ := strconv.ParseUint(bytes[1], 10, 64)

			st.Network.RxBytes += rxBytes
			st.Network.TxBytes += txBytes
		}

		e.Events().Publish(environment.ResourceEvent, st)

		time.Sleep(time.Second)
	}
	return nil
}
