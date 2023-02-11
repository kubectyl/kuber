package kubernetes

import (
	"bufio"
	"bytes"
	"context"
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
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
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

	mc, err := metrics.NewForConfig(e.config)
	if err != nil {
		panic(err)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	for {
		// Disable collection if the server is in an offline state and this process is still running.
		if e.st.Load() == environment.ProcessOfflineState {
			e.log().Debug("process in offline state while resource polling is still active; stopping poll")
			break
		}

		// Don't throw an error if pod metrics are not available, just keep trying.
		podMetrics, err := mc.MetricsV1beta1().PodMetricses(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})

		// Check if container index 0 is not out of range,
		// if it is then send only the uptime stats.
		//
		// @see https://stackoverflow.com/questions/26126235/panic-runtime-error-index-out-of-range-in-go
		if len(podMetrics.Containers) != 0 {
			cpuQuantity := ""
			memQuantity, ok := int64(0), false

			for i, c := range podMetrics.Containers {
				if c.Name == "process" {
					cpuQuantity = podMetrics.Containers[i].Usage.Cpu().AsDec().String()
					memQuantity, ok = podMetrics.Containers[i].Usage.Memory().AsInt64()
				} else {
					continue
				}
			}

			if !ok {
				break
			}

			uptime = uptime + 1000

			f, _ := strconv.ParseFloat(cpuQuantity, 32)
			if err != nil {
				break
			}

			st := environment.Stats{
				Uptime:      uptime,
				Memory:      uint64(memQuantity),
				CpuAbsolute: f * 100,
				Network:     environment.NetworkStats{},
			}

			b, err := rbuf.ReadBytes('\n')
			if err == io.EOF {
				continue
			}

			words := strings.Fields(string(bytes.TrimSpace(b)))

			if len(words) != 0 {
				rxBytes, _ := strconv.ParseUint(words[0], 10, 64)
				txBytes, _ := strconv.ParseUint(words[1], 10, 64)

				st.Network.RxBytes += rxBytes
				st.Network.TxBytes += txBytes
			}

			e.Events().Publish(environment.ResourceEvent, st)
		} else {
			uptime = uptime + 1000

			st := environment.Stats{
				Uptime: uptime,
			}

			b, err := rbuf.ReadBytes('\n')
			if err == io.EOF {
				continue
			}

			words := strings.Fields(string(bytes.TrimSpace(b)))

			if len(words) != 0 {
				rxBytes, _ := strconv.ParseUint(words[0], 10, 64)
				txBytes, _ := strconv.ParseUint(words[1], 10, 64)

				st.Network.RxBytes += rxBytes
				st.Network.TxBytes += txBytes
			}

			e.Events().Publish(environment.ResourceEvent, st)
		}

		time.Sleep(time.Second)
	}
	return nil
}
