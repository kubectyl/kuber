package kubernetes

import (
	"bufio"
	"context"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/docker/docker/api/types/mount"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/system"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ErrNotAttached = errors.Sentinel("not attached to instance")

// A custom console writer that allows us to keep a function blocked until the
// given stream is properly closed. This does nothing special, only exists to
// make a noop io.Writer.
type noopWriter struct{}

var _ io.Writer = noopWriter{}

// Implement the required Write function to satisfy the io.Writer interface.
func (nw noopWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

// Attach attaches to the docker container itself and ensures that we can pipe
// data in and out of the process stream. This should always be called before
// you have started the container, but after you've ensured it exists.
//
// Calling this function will poll resources for the container in the background
// until the container is stopped. The context provided to this function is used
// for the purposes of attaching to the container, a second context is created
// within the function for managing polling.
func (e *Environment) Attach(ctx context.Context) error {
	// if e.IsAttached() {
	// 	return nil
	// }

	req := e.client.CoreV1().RESTClient().
		Post().
		Namespace(config.Get().Cluster.Namespace).
		Resource("pods").
		Name(e.Id).
		SubResource("attach").
		VersionedParams(&corev1.PodAttachOptions{
			Container: "process",
			Stdin:     true,
			Stdout:    false,
			Stderr:    false,
			TTY:       true,
		}, scheme.ParameterCodec)

	// Set the stream again with the container.
	if exec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL()); err != nil {
		return err
	} else {
		e.SetStream(exec)
	}

	go func() {
		// Don't use the context provided to the function, that'll cause the polling to
		// exit unexpectedly. We want a custom context for this, the one passed to the
		// function is to avoid a hang situation when trying to attach to a container.
		pollCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// defer func() {
		// e.SetState(environment.ProcessOfflineState)
		// e.SetStream(nil)
		// }()

		go func() {
			if err := e.pollResources(pollCtx); err != nil {
				if !errors.Is(err, context.Canceled) {
					e.log().WithField("error", err).Error("error during environment resource polling")
				} else {
					e.log().Warn("stopping server resource polling: context canceled")
				}
			}
		}()

		reader := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).GetLogs(e.Id, &corev1.PodLogOptions{
			Container: "process",
			Follow:    true,
		})
		podLogs, err := reader.Stream(context.TODO())
		if err != nil {
			return
		}
		defer podLogs.Close()

		if err := system.ScanReader(podLogs, func(v []byte) {
			e.logCallbackMx.Lock()
			defer e.logCallbackMx.Unlock()
			e.logCallback(v)
		}); err != nil && err != io.EOF {
			log.WithField("error", err).WithField("container_id", e.Id).Warn("error processing scanner line in console output")
			return
		}
	}()

	return nil
}

func ptrPodFSGroupChangePolicy(p corev1.PodFSGroupChangePolicy) *corev1.PodFSGroupChangePolicy {
	return &p
}

// Create creates a new container for the server using all the data that is
// currently available for it. If the container already exists it will be
// returned.
func (e *Environment) Create() error {
	ctx := context.Background()

	// If the pod already exists don't hit the user with an error, just return
	// the current information about it which is what we would do when creating the
	// pod anyways.
	if _, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "environment/kubernetes: failed to inspect pod")
	}

	cfg := config.Get()
	a := e.Configuration.Allocations()
	evs := e.Configuration.EnvironmentVariables()

	// Merge user-provided labels with system labels
	confLabels := e.Configuration.Labels()
	labels := make(map[string]string, 2+len(confLabels))

	for key := range confLabels {
		labels[key] = confLabels[key]
	}
	labels["uuid"] = e.Id
	labels["Service"] = "Kubectyl"
	labels["ContainerType"] = "server_process"

	resources := e.Configuration.Limits()

	var dnspolicy corev1.DNSPolicy
	switch cfg.Cluster.DNSPolicy {
	case "clusterfirstwithhostnet":
		dnspolicy = corev1.DNSClusterFirstWithHostNet
	case "default":
		dnspolicy = corev1.DNSDefault
	case "none":
		dnspolicy = corev1.DNSNone
	default:
		dnspolicy = corev1.DNSClusterFirst
	}

	var imagepullpolicy corev1.PullPolicy
	switch cfg.Cluster.ImagePullPolicy {
	case "always":
		imagepullpolicy = corev1.PullAlways
	case "never":
		imagepullpolicy = corev1.PullNever
	default:
		imagepullpolicy = corev1.PullIfNotPresent
	}

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.Id,
			Namespace: cfg.Cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				FSGroup:             &[]int64{2000}[0],
				FSGroupChangePolicy: ptrPodFSGroupChangePolicy("OnRootMismatch"),
			},
			DNSPolicy: dnspolicy,
			DNSConfig: &corev1.PodDNSConfig{Nameservers: config.Get().Cluster.Network.Dns},
			Volumes: []corev1.Volume{
				{
					Name: "tmp",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							Medium: corev1.StorageMedium("Memory"),
							SizeLimit: &resource.Quantity{
								Format: resource.Format("BinarySI"),
							},
						},
					},
				},
				{
					Name: "storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: e.Id + "-pvc",
						},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "kuber",
							},
						},
					},
				},
				{
					Name: "secret",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "ed25519",
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "process",
					Image:           e.meta.Image,
					ImagePullPolicy: imagepullpolicy,
					TTY:             true,
					Stdin:           true,
					WorkingDir:      "/home/container",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: int32(a.DefaultPort),
							Protocol:      corev1.Protocol("TCP"),
						},
						{
							ContainerPort: int32(a.DefaultPort),
							Protocol:      corev1.Protocol("UDP"),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: &[]bool{false}[0],
						RunAsUser:                &[]int64{int64(0)}[0],
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    *resource.NewMilliQuantity(resources.CpuLimit*10, resource.DecimalSI),
							corev1.ResourceMemory: *resource.NewQuantity(resources.BoundedMemoryLimit(), resource.BinarySI),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    *resource.NewMilliQuantity(resources.CpuLimit*10, resource.DecimalSI),
							corev1.ResourceMemory: *resource.NewQuantity(resources.BoundedMemoryLimit(), resource.BinarySI),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "tmp",
							MountPath: "/tmp",
						},
						{
							Name:      "storage",
							MountPath: "/home/container",
						},
					},
				},
				{
					Name:            "sftp-server",
					Image:           cfg.System.Sftp.SftpImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Env: []corev1.EnvVar{
						{
							Name:  "P_SERVER_UUID",
							Value: e.Id,
						},
					},
					Ports: []corev1.ContainerPort{
						{
							HostPort:      0,
							ContainerPort: int32(config.Get().System.Sftp.Port),
							Protocol:      corev1.Protocol("TCP"),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "config",
							MountPath: "/etc/kubectyl",
						},
						{
							Name:      "storage",
							MountPath: path.Join(cfg.System.Data, e.Id),
						},
						{
							Name:      "secret",
							ReadOnly:  true,
							MountPath: path.Join(cfg.System.Data, ".sftp"),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicy("Never"),
		},
	}

	// Assign all TCP / UDP ports to the container
	for b := range a.Bindings() {
		port, err := strconv.ParseInt(b.Port(), 10, 32)
		if err != nil {
			return err
		}
		protocol := strings.ToUpper(b.Proto())

		pod.Spec.Containers[0].Ports = append(pod.Spec.Containers[0].Ports,
			corev1.ContainerPort{
				ContainerPort: int32(port),
				Protocol:      corev1.Protocol(protocol),
			})
	}

	for _, k := range evs {
		a := strings.SplitN(k, "=", 2)

		// If a variable is empty, skip it
		if a[0] != "" && a[1] != "" {
			pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env,
				corev1.EnvVar{
					Name:  a[0],
					Value: a[1],
				})
		}
	}

	// Set the user running the container properly depending on what mode we are operating in.
	securityContext := pod.Spec.Containers[0].SecurityContext
	if cfg.System.User.Rootless.Enabled {
		securityContext.RunAsNonRoot = &[]bool{true}[0]
		securityContext.RunAsUser = &[]int64{int64(cfg.System.User.Rootless.ContainerUID)}[0]
		securityContext.RunAsGroup = &[]int64{int64(cfg.System.User.Rootless.ContainerGID)}[0]
	}

	// Check if the services exists before we create the actual pod
	err := e.CreateService()
	if err != nil {
		return err
	}

	if _, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return errors.Wrap(err, "environment/kubernetes: failed to create pod")
	}

	return nil
}

// Google GKE Autopilot
// May not contain more than 1 protocol when type is 'LoadBalancer'
//
// @see https://stackoverflow.com/questions/65094464/expose-both-tcp-and-udp-from-the-same-ingress
func (e *Environment) CreateService() error {
	ctx := context.Background()

	cfg := config.Get()
	a := e.Configuration.Allocations()

	var servicetype string
	switch cfg.Cluster.ServiceType {
	case "loadbalancer":
		servicetype = "LoadBalancer"
	default:
		servicetype = "NodePort"
	}

	udp := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuber-" + e.Id + "-udp",
			Namespace: config.Get().Cluster.Namespace,
			Labels: map[string]string{
				"uuid": e.Id,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "udp" + strconv.Itoa(a.DefaultPort),
					Protocol: corev1.ProtocolUDP,
					Port:     int32(a.DefaultPort),
				},
			},
			Selector: map[string]string{
				"uuid": e.Id,
			},
			Type:                     corev1.ServiceType(servicetype),
			ExternalTrafficPolicy:    corev1.ServiceExternalTrafficPolicyTypeLocal,
			HealthCheckNodePort:      0,
			PublishNotReadyAddresses: true,
		},
	}

	if cfg.Cluster.ServiceType == "loadbalancer" {
		if cfg.Cluster.MetalLBSharedIP && len(cfg.Cluster.MetalLBAddressPool) != 0 {
			udp.Annotations = map[string]string{
				"metallb.universe.tf/address-pool":    cfg.Cluster.MetalLBAddressPool,
				"metallb.universe.tf/allow-shared-ip": e.Id,
			}
		} else if cfg.Cluster.MetalLBSharedIP && len(cfg.Cluster.MetalLBAddressPool) == 0 {
			udp.Annotations = map[string]string{
				"metallb.universe.tf/allow-shared-ip": e.Id,
			}
		}
	}

	for b := range a.Bindings() {
		protocol := strings.ToUpper(b.Proto())

		if protocol == "UDP" {
			port, err := strconv.ParseInt(b.Port(), 10, 32)
			if err != nil {
				return err
			}

			udp.Spec.Ports = append(udp.Spec.Ports,
				corev1.ServicePort{Name: b.Proto() + b.Port(),
					Protocol: corev1.Protocol(protocol),
					Port:     int32(port),
				})
		}
	}

	if _, err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).Create(ctx, udp, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return errors.Wrap(err, "environment/kubernetes: failed to create UDP service")
		}
	}

	tcp := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuber-" + e.Id + "-tcp",
			Namespace: config.Get().Cluster.Namespace,
			Labels: map[string]string{
				"uuid": e.Id,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "tcp" + strconv.Itoa(cfg.System.Sftp.Port),
					Protocol: corev1.ProtocolTCP,
					Port:     int32(cfg.System.Sftp.Port),
				},
				{
					Name:     "tcp" + strconv.Itoa(a.DefaultPort),
					Protocol: corev1.ProtocolTCP,
					Port:     int32(a.DefaultPort),
				},
			},
			Selector: map[string]string{
				"uuid": e.Id,
			},
			Type:                     corev1.ServiceType(servicetype),
			ExternalTrafficPolicy:    corev1.ServiceExternalTrafficPolicyTypeLocal,
			HealthCheckNodePort:      0,
			PublishNotReadyAddresses: true,
		},
	}

	if cfg.Cluster.ServiceType == "loadbalancer" {
		if cfg.Cluster.MetalLBSharedIP && len(cfg.Cluster.MetalLBAddressPool) != 0 {
			udp.Annotations = map[string]string{
				"metallb.universe.tf/address-pool":    cfg.Cluster.MetalLBAddressPool,
				"metallb.universe.tf/allow-shared-ip": e.Id,
			}
		} else if cfg.Cluster.MetalLBSharedIP && len(cfg.Cluster.MetalLBAddressPool) == 0 {
			udp.Annotations = map[string]string{
				"metallb.universe.tf/allow-shared-ip": e.Id,
			}
		}
	}

	for b := range a.Bindings() {
		protocol := strings.ToUpper(b.Proto())

		if protocol == "TCP" {
			port, err := strconv.ParseInt(b.Port(), 10, 32)
			if err != nil {
				return err
			}

			tcp.Spec.Ports = append(tcp.Spec.Ports,
				corev1.ServicePort{
					Name:     b.Proto() + b.Port(),
					Protocol: corev1.Protocol(protocol),
					Port:     int32(port),
				})
		}
	}

	if _, err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).Create(ctx, tcp, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return errors.Wrap(err, "environment/kubernetes: failed to create TCP service")
		}
	}

	return nil
}

// Destroy will remove the Docker container from the server. If the container
// is currently running it will be forcibly stopped by Docker.
func (e *Environment) Destroy() error {
	// We set it to stopping than offline to prevent crash detection from being triggered.
	e.SetState(environment.ProcessStoppingState)

	var zero int64 = 0
	policy := metav1.DeletePropagationForeground

	// Loop through services with server UUID and delete them
	services, err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "uuid=" + e.Id,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	} else {
		for _, s := range services.Items {
			err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).Delete(context.TODO(), s.Name, metav1.DeleteOptions{})
			if err != nil {
				return err
			}
		}
	}

	// Delete installer pod
	err = e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id+"-installer", metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	// Delete configmap
	err = e.client.CoreV1().ConfigMaps(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id+"-configmap", metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	// Delete pod
	err = e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id, metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	// Delete pvc
	err = e.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id+"-pvc", metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	e.SetState(environment.ProcessOfflineState)

	return nil
}

// SendCommand sends the specified command to the stdin of the running container
// instance. There is no confirmation that this data is sent successfully, only
// that it gets pushed into the stdin.
func (e *Environment) SendCommand(c string) error {
	if !e.IsAttached() {
		return errors.Wrap(ErrNotAttached, "environment/kubernetes: cannot send command to container")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// If the command being processed is the same as the process stop command then we
	// want to mark the server as entering the stopping state otherwise the process will
	// stop and Kuber will think it has crashed and attempt to restart it.
	if e.meta.Stop.Type == "command" && c == e.meta.Stop.Value {
		e.SetState(environment.ProcessStoppingState)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	w.Write([]byte(c + "\n"))

	err = e.stream.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: r,
		Tty:   true,
	})
	if err != nil {
		return err
	}

	return errors.Wrap(err, "environment/kubernetes: could not write to container stream")
}

// Readlog reads the log file for the server. This does not care if the server
// is running or not, it will simply try to read the last X bytes of the file
// and return them.
func (e *Environment) Readlog(lines int) ([]string, error) {
	r := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).GetLogs(e.Id, &corev1.PodLogOptions{
		Container: "process",
		TailLines: &[]int64{int64(lines)}[0],
	})
	podLogs, err := r.Stream(context.Background())
	if err != nil {
		return nil, err
	}
	defer podLogs.Close()

	var out []string
	scanner := bufio.NewScanner(podLogs)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}

	return out, nil
}

func (e *Environment) convertMounts() []mount.Mount {
	var out []mount.Mount

	for _, m := range e.Configuration.Mounts() {
		out = append(out, mount.Mount{
			Type:     mount.TypeBind,
			Source:   m.Source,
			Target:   m.Target,
			ReadOnly: m.ReadOnly,
		})
	}

	return out
}
