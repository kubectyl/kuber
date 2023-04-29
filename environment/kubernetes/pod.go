package kubernetes

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	errors2 "emperror.dev/errors"
	"github.com/apex/log"
	"github.com/docker/docker/api/types/mount"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/system"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ErrNotAttached = errors2.Sentinel("not attached to instance")

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
		defer func() {
			e.SetState(environment.ProcessOfflineState)
			e.SetStream(nil)
		}()

		go func() {
			if err := e.pollResources(pollCtx); err != nil {
				if !errors2.Is(err, context.Canceled) {
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
			log.WithField("error", err).WithField("pod_name", e.Id).Warn("error processing scanner line in console output")
			return
		}
	}()

	return nil
}

func (e *Environment) InSituUpdate() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pod, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
	if err != nil {
		// If the pod doesn't exist for some reason there really isn't anything
		// we can do to fix that in this process (it doesn't make sense at least). In those
		// cases just return without doing anything since we still want to save the configuration
		// to the disk.

		// We'll let a boot process make modifications to the pod if needed at this point.
		if errors.IsNotFound(err) {
			return nil
		}
		return errors2.Wrap(err, "environment/kubernetes: could not inspect pod")
	}

	resources := e.Configuration.Limits()

	originalPodBytes, err := json.Marshal(pod)
	if err != nil {
		return err
	}

	modifiedPodBytes, err := json.Marshal(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: pod.Spec.Containers[0].Name,
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    *resource.NewMilliQuantity(10*resources.CpuLimit, resource.DecimalSI),
							"memory": *resource.NewQuantity(resources.MemoryLimit*1_000_000, resource.BinarySI),
							// "hugepages-2Mi":       *resource.NewQuantity(1<<30, resource.BinarySI),
						},
						Requests: v1.ResourceList{
							"cpu":    *resource.NewMilliQuantity(10*resources.CpuRequest, resource.DecimalSI),
							"memory": *resource.NewQuantity(resources.MemoryRequest*1_000_000, resource.BinarySI),
							// "hugepages-2Mi":       *resource.NewQuantity(1<<30, resource.BinarySI),
						},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(originalPodBytes, modifiedPodBytes, &v1.Pod{})
	if err != nil {
		return err
	}

	if _, err = e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Patch(ctx, e.Id, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return errors2.Wrap(err, "environment/kubernetes: could not patch pod")
	}

	return nil
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
	} else if !errors.IsNotFound(err) {
		return errors2.Wrap(err, "environment/kubernetes: failed to inspect pod")
	}

	cfg := config.Get()
	p := e.Configuration.Ports()
	a := e.Configuration.Allocations()
	evs := e.Configuration.EnvironmentVariables()
	// cfs := e.Configuration.ConfigurationFiles()

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

	dnsPolicies := map[string]corev1.DNSPolicy{
		"clusterfirstwithhostnet": corev1.DNSClusterFirstWithHostNet,
		"default":                 corev1.DNSDefault,
		"none":                    corev1.DNSNone,
		"clusterfirst":            corev1.DNSClusterFirst,
	}

	dnspolicy, ok := dnsPolicies[cfg.Cluster.DNSPolicy]
	if !ok {
		dnspolicy = corev1.DNSClusterFirst
	}

	imagePullPolicies := map[string]corev1.PullPolicy{
		"always":       corev1.PullAlways,
		"never":        corev1.PullNever,
		"ifnotpresent": corev1.PullIfNotPresent,
	}

	imagepullpolicy, ok := imagePullPolicies[cfg.Cluster.ImagePullPolicy]
	if !ok {
		imagepullpolicy = corev1.PullIfNotPresent
	}

	getInt := func() int32 {
		port := p.DefaultMapping.Port
		if port == 0 {
			port = a.DefaultMapping.Port
		}
		return int32(port)
	}
	port := getInt()

	// Prevents high CPU usage of kubelet by preventing chown on the entire CSI
	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch

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
				FSGroupChangePolicy: &fsGroupChangePolicy,
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
							ClaimName: fmt.Sprintf("%s-pvc", e.Id),
						},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "sftp",
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
							ContainerPort: port,
							Protocol:      corev1.Protocol("TCP"),
						},
						{
							ContainerPort: port,
							Protocol:      corev1.Protocol("UDP"),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: &[]bool{false}[0],
						RunAsUser:                &[]int64{int64(0)}[0],
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    *resource.NewMilliQuantity(10*resources.CpuLimit, resource.DecimalSI),
							corev1.ResourceMemory: *resource.NewQuantity(resources.MemoryLimit*1_000_000, resource.BinarySI),
							// "hugepages-2Mi":       *resource.NewQuantity(1<<30, resource.BinarySI),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    *resource.NewMilliQuantity(10*resources.CpuRequest, resource.DecimalSI),
							corev1.ResourceMemory: *resource.NewQuantity(resources.MemoryRequest*1_000_000, resource.BinarySI),
							// "hugepages-2Mi":       *resource.NewQuantity(1<<30, resource.BinarySI),
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
					ImagePullPolicy: imagepullpolicy,
					Env: []corev1.EnvVar{
						{
							Name:  "P_SERVER_UUID",
							Value: e.Id,
						},
					},
					Ports: []corev1.ContainerPort{
						{
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
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	// Disable CPU request / limit if is set to Unlimited
	if resources.CpuLimit == 0 {
		pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: *resource.NewQuantity(resources.MemoryRequest*1_000_000, resource.BinarySI),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: *resource.NewQuantity(resources.MemoryLimit*1_000_000, resource.BinarySI),
			},
		}
	}

	// Prevents high CPU usage of kubelet by preventing chown on the entire CSI
	if pod.Spec.SecurityContext.FSGroupChangePolicy == nil {
		fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch
		pod.Spec.SecurityContext.FSGroupChangePolicy = &fsGroupChangePolicy
	}

	// Check if Node Selector array is empty before we continue
	if len(e.Configuration.NodeSelectors()) > 0 {
		pod.Spec.NodeSelector = map[string]string{}

		// Loop through the map and create a node selector string
		for k, v := range e.Configuration.NodeSelectors() {
			if !environment.LabelNameRegex.MatchString(k) {
				continue
			}
			pod.Spec.NodeSelector[k] = v
			if !environment.LabelValueRegex.MatchString(v) {
				pod.Spec.NodeSelector[k] = ""
			}
		}
	}

	// for _, k := range cfs {
	// replacement := make(map[string]string)
	// for _, t := range k.Replace {
	// 	replacement[t.Match] = t.ReplaceWith.String()
	// }

	// Generate the init container ENV from a loop
	// envs := []corev1.EnvVar{}

	// for key, value := range replacement {
	// 	envs = append(envs, corev1.EnvVar{
	// 		Name:  fmt.Sprintf("%s_%s", strings.ToUpper(strings.ReplaceAll(k.FileName, ".", "_")), key),
	// 		Value: value,
	// 	})
	// }

	// command := k.Parse(k.FileName, false)

	// Initialize an empty slice of initContainers
	// pod.Spec.InitContainers = []corev1.Container{}

	// Add a new initContainer to the Pod
	// newInitContainer := corev1.Container{
	// 	Name:      "replace-" + strings.ToLower(strings.ReplaceAll(k.FileName, ".", "-")),
	// 	Image:     "busybox",
	// 	Command:   command,
	// 	Env:       envs,
	// 	Resources: corev1.ResourceRequirements{},
	// 	VolumeMounts: []corev1.VolumeMount{
	// 		{
	// 			Name:      "storage",
	// 			MountPath: "/home/container",
	// 		},
	// 	},
	// }

	// pod.Spec.InitContainers = append(pod.Spec.InitContainers, newInitContainer)
	// }

	// Assign all TCP / UDP ports or allocations to the container
	bindings := p.Bindings()
	if len(bindings) == 0 {
		bindings = a.Bindings()
	}

	for b := range bindings {
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

	// Check if the services exists before we create the actual pod.
	err := e.CreateService()
	if err != nil {
		return err
	}

	if _, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return errors2.Wrap(err, "environment/kubernetes: failed to create pod")
	}

	return nil
}

// Google GKE Autopilot
// May not contain more than 1 protocol when type is 'LoadBalancer'.
func (e *Environment) CreateService() error {
	ctx := context.Background()

	cfg := config.Get()
	p := e.Configuration.Ports()
	a := e.Configuration.Allocations()

	serviceTypeMap := map[string]string{
		"loadbalancer": "LoadBalancer",
	}

	serviceType := serviceTypeMap[cfg.Cluster.ServiceType]
	if serviceType == "" {
		serviceType = "NodePort"
	}

	var externalPolicy corev1.ServiceExternalTrafficPolicyType
	trafficPolicies := map[string]corev1.ServiceExternalTrafficPolicyType{
		"cluster": corev1.ServiceExternalTrafficPolicyTypeCluster,
		"local":   corev1.ServiceExternalTrafficPolicyTypeLocal,
	}

	if policy, ok := trafficPolicies[cfg.Cluster.ExternalTrafficPolicy]; ok {
		externalPolicy = policy
	} else {
		externalPolicy = corev1.ServiceExternalTrafficPolicyTypeCluster
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
				"uuid":    e.Id,
				"Service": "Kubectyl",
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:     "tcp2022",
					Protocol: corev1.Protocol("TCP"),
					Port:     int32(cfg.System.Sftp.Port),
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: int32(cfg.System.Sftp.Port),
					},
					NodePort: 0,
				},
			},
			Selector: map[string]string{
				"uuid": e.Id,
			},
			Type:                          corev1.ServiceType(serviceType),
			ExternalTrafficPolicy:         corev1.ServiceExternalTrafficPolicyType(externalPolicy),
			HealthCheckNodePort:           0,
			PublishNotReadyAddresses:      true,
			AllocateLoadBalancerNodePorts: new(bool),
		},
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
				"uuid":    e.Id,
				"Service": "Kubectyl",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"uuid": e.Id,
			},
			Type:                          corev1.ServiceType(serviceType),
			ExternalTrafficPolicy:         corev1.ServiceExternalTrafficPolicyType(externalPolicy),
			HealthCheckNodePort:           0,
			PublishNotReadyAddresses:      true,
			AllocateLoadBalancerNodePorts: new(bool),
		},
	}

	if serviceType == "LoadBalancer" && cfg.Cluster.MetalLBSharedIP {
		tcp.Annotations = map[string]string{
			"metallb.universe.tf/allow-shared-ip": e.Id,
		}

		udp.Annotations = map[string]string{
			"metallb.universe.tf/allow-shared-ip": e.Id,
		}

		if len(cfg.Cluster.MetalLBAddressPool) != 0 {
			tcp.Annotations["metallb.universe.tf/address-pool"] = cfg.Cluster.MetalLBAddressPool
			udp.Annotations["metallb.universe.tf/address-pool"] = cfg.Cluster.MetalLBAddressPool
		}
	}

	bindings := p.Bindings()
	if len(bindings) == 0 {
		bindings = a.Bindings()
	}

	for b := range bindings {
		protocol := strings.ToUpper(b.Proto())

		var service *corev1.Service
		if protocol == "TCP" {
			service = tcp
		} else if protocol == "UDP" {
			service = udp
		}

		port, err := strconv.ParseInt(b.Port(), 10, 32)
		if err != nil {
			return err
		}

		service.Spec.Ports = append(service.Spec.Ports,
			corev1.ServicePort{
				Name:     b.Proto() + b.Port(),
				Protocol: corev1.Protocol(protocol),
				Port:     int32(port),
			})

		if serviceType == "LoadBalancer" {
			service.Spec.Ports[len(service.Spec.Ports)-1].NodePort = 0
			*service.Spec.AllocateLoadBalancerNodePorts = false
			service.Spec.LoadBalancerIP = a.DefaultMapping.Ip
		}
	}

	services, err := e.client.CoreV1().Services(cfg.Cluster.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, service := range services.Items {
		if service.Spec.Type == "LoadBalancer" && service.Status.LoadBalancer.Ingress != nil {
			for _, ingress := range service.Status.LoadBalancer.Ingress {
				if ingress.IP == a.DefaultMapping.Ip {
					shared := service.Annotations["metallb.universe.tf/allow-shared-ip"]

					if len(shared) > 0 {
						annotations := map[string]string{
							"metallb.universe.tf/allow-shared-ip": shared,
						}
						tcp.Annotations = annotations
						udp.Annotations = annotations

						port, err := e.CreateServiceWithUniquePort()
						if err != nil || port == 0 {
							return err
						}
						tcp.Spec.Ports[0].Port = int32(port)
					}
				}
			}
		}
	}

	if _, err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).Create(ctx, tcp, metav1.CreateOptions{}); err != nil {
		if !errors.IsAlreadyExists(err) {
			return errors2.Wrap(err, "environment/kubernetes: failed to create TCP service")
		}
	}

	if _, err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).Create(ctx, udp, metav1.CreateOptions{}); err != nil {
		if !errors.IsAlreadyExists(err) {
			return errors2.Wrap(err, "environment/kubernetes: failed to create UDP service")
		}
	}

	return nil
}

func (e *Environment) CreateSFTP(ctx context.Context) error {
	cfg := config.Get()

	dnsPolicies := map[string]corev1.DNSPolicy{
		"clusterfirstwithhostnet": corev1.DNSClusterFirstWithHostNet,
		"default":                 corev1.DNSDefault,
		"none":                    corev1.DNSNone,
		"clusterfirst":            corev1.DNSClusterFirst,
	}

	dnspolicy, ok := dnsPolicies[cfg.Cluster.DNSPolicy]
	if !ok {
		dnspolicy = corev1.DNSClusterFirst
	}

	imagePullPolicies := map[string]corev1.PullPolicy{
		"always":       corev1.PullAlways,
		"never":        corev1.PullNever,
		"ifnotpresent": corev1.PullIfNotPresent,
	}

	imagepullpolicy, ok := imagePullPolicies[cfg.Cluster.ImagePullPolicy]
	if !ok {
		imagepullpolicy = corev1.PullIfNotPresent
	}

	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.Id,
			Namespace: cfg.Cluster.Namespace,
			Labels: map[string]string{
				"uuid":          e.Id,
				"Service":       "Kubectyl",
				"ContainerType": "sftp_server",
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				FSGroup:             &[]int64{2000}[0],
				FSGroupChangePolicy: &fsGroupChangePolicy,
			},
			DNSPolicy: dnspolicy,
			DNSConfig: &corev1.PodDNSConfig{Nameservers: config.Get().Cluster.Network.Dns},
			Volumes: []corev1.Volume{
				{
					Name: "storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: fmt.Sprintf("%s-pvc", e.Id),
						},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "sftp",
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
					Name:            "sftp-server",
					Image:           cfg.System.Sftp.SftpImage,
					ImagePullPolicy: imagepullpolicy,
					Env: []corev1.EnvVar{
						{
							Name:  "P_SERVER_UUID",
							Value: e.Id,
						},
					},
					Ports: []corev1.ContainerPort{
						{
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
			RestartPolicy: corev1.RestartPolicyAlways,
		},
	}

	_, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			pod, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if pod.Status.Phase != v1.PodRunning {
				var zero int64 = 0
				policy := metav1.DeletePropagationForeground

				if err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(ctx, e.Id, metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy}); err != nil {
					return err
				}

				return e.CreateSFTP(ctx)
			}
		} else {
			return errors2.Wrap(err, "environment/kubernetes: failed to create SFTP pod")
		}
	}

	err = wait.PollImmediate(time.Second, 30*time.Second, func() (bool, error) {
		pod, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case v1.PodPending:
			return false, nil
		case v1.PodRunning:
			return true, nil
		case v1.PodFailed, v1.PodSucceeded:
			return false, fmt.Errorf("pod ran to completion")
		default:
			return false, fmt.Errorf("unknown pod status")
		}
	})
	if err != nil {
		return err
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
	if err != nil && !errors.IsNotFound(err) {
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
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Delete configmap
	err = e.client.CoreV1().ConfigMaps(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id+"-configmap", metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &policy,
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Delete pod
	err = e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id, metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &policy,
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Delete pvc
	err = e.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Delete(context.Background(), e.Id+"-pvc", metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &policy,
	})
	if err != nil && !errors.IsNotFound(err) {
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
		return errors2.Wrap(ErrNotAttached, "environment/kubernetes: cannot send command to container")
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

	return errors2.Wrap(err, "environment/kubernetes: could not write to container stream")
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
