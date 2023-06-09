package kubernetes

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	errors2 "emperror.dev/errors"
	"github.com/apex/log"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/pointer"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/system"

	corev1 "k8s.io/api/core/v1"
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

	pvc, err := e.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Get(ctx, fmt.Sprintf("%s-pvc", e.Id), metav1.GetOptions{})
	if err != nil {
		// If the pvc doesn't exist for some reason there really isn't anything
		// we can do to fix that in this process.
		if errors.IsNotFound(err) {
			return nil
		}
		return errors2.Wrap(err, "environment/kubernetes: could not inspect pvc")
	}

	newSize := e.Configuration.Limits().DiskSpace
	oldSize := pvc.Spec.Resources.Requests["storage"]

	oldSizeInMb := oldSize.Value() / (1024 * 1024)

	// Skip further execution and do nothing
	if newSize == oldSizeInMb {
		return nil
	} else if newSize < oldSizeInMb {
		e.log().Warn("environment/kubernetes: PVC new size is less than previous value")
		return nil
	}

	pvc.Spec.Resources.Requests["storage"] = *resource.NewQuantity(e.Configuration.Limits().DiskSpace, resource.BinarySI)

	if _, err = e.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		// Don't throw an error if the PVC is not dynamically created,
		if statusErr, ok := err.(*errors.StatusError); ok && statusErr.ErrStatus.Reason == "Invalid" {
			for _, detail := range statusErr.ErrStatus.Details.Causes {
				if detail.Type == "FieldValueForbidden" && detail.Field == "spec.resources.requests.storage" {
					return nil
				}
			}
		}
		return errors2.Wrap(err, "environment/kubernetes: could not update pvc")
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
	cfs := e.Configuration.ConfigurationFiles()

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
				FSGroup:             pointer.Int64(2000),
				FSGroupChangePolicy: &fsGroupChangePolicy,
				RunAsUser:           pointer.Int64(1000),
				RunAsNonRoot:        pointer.Bool(true),
			},
			DNSPolicy: map[string]corev1.DNSPolicy{
				"clusterfirstwithhostnet": corev1.DNSClusterFirstWithHostNet,
				"default":                 corev1.DNSDefault,
				"none":                    corev1.DNSNone,
				"clusterfirst":            corev1.DNSClusterFirst,
			}[cfg.Cluster.DNSPolicy],
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

	volumeMounts, volumes := e.convertMounts()
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, volumeMounts...)
	pod.Spec.Volumes = append(pod.Spec.Volumes, volumes...)

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

	if len(cfs) > 0 {
		// Initialize an empty slice of initContainers
		pod.Spec.InitContainers = []corev1.Container{}

		// Create a map to store the file data for the ConfigMap
		fileData := make(map[string]string)

		for _, k := range cfs {
			// replacement := make(map[string]string)
			for _, t := range k.Replace {
				// replacement[t.Match] = t.ReplaceWith.String()
				fileData[k.FileName] += fmt.Sprintf("%s=%s\n", t.Match, t.ReplaceWith.String())
			}

			command, err := k.Parse("/config/", "/home/container/")
			if err != nil {
				return err
			}

			// Add a new initContainer to the Pod
			newInitContainer := corev1.Container{
				Name:            "configuration-files",
				Image:           "busybox",
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:    pointer.Int64(1000),
					RunAsNonRoot: pointer.Bool(true),
				},
				Command:   command,
				Resources: corev1.ResourceRequirements{},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "replacement",
						MountPath: "/config",
						ReadOnly:  true,
					},
					{
						Name:      "storage",
						MountPath: "/home/container",
					},
				},
			}

			pod.Spec.InitContainers = append(pod.Spec.InitContainers, newInitContainer)
		}

		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "replacement",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: e.Id + "-replacement",
					},
				},
			},
		})

		// Create the ConfigMap object with the file data
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      e.Id + "-replacement",
				Namespace: cfg.Cluster.Namespace,
			},
			Data: fileData,
		}

		// Check if the ConfigMap already exists
		_, err := e.client.CoreV1().ConfigMaps(cfg.Cluster.Namespace).Get(context.TODO(), e.Id+"-replacement", metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				_, err = e.client.CoreV1().ConfigMaps(cfg.Cluster.Namespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
				if err != nil {
					return err
				}
				e.log().Info("replacement configmap created successfully")
			} else {
				return err
			}
		} else {
			_, err = e.client.CoreV1().ConfigMaps(cfg.Cluster.Namespace).Update(context.TODO(), configMap, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			e.log().Info("replacement configmap updated successfully")
		}
	}

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

	// Configure pod spec for restricted security standard
	if cfg.Cluster.RestrictedPodSecurityStandard {
		if err := e.ConfigurePodSpecForRestrictedStandard(&pod.Spec); err != nil {
			return err
		}
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

func (e *Environment) CreateSFTP(ctx context.Context, cancelFunc context.CancelFunc) error {
	cfg := config.Get()

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
				FSGroup:             pointer.Int64(2000),
				FSGroupChangePolicy: &fsGroupChangePolicy,
				RunAsUser:           pointer.Int64(1000),
				RunAsNonRoot:        pointer.Bool(true),
			},
			DNSPolicy: map[string]corev1.DNSPolicy{
				"clusterfirstwithhostnet": corev1.DNSClusterFirstWithHostNet,
				"default":                 corev1.DNSDefault,
				"none":                    corev1.DNSNone,
				"clusterfirst":            corev1.DNSClusterFirst,
			}[cfg.Cluster.DNSPolicy],
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
				{
					Name: "logs",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "sftp-server",
					Image: cfg.System.Sftp.SftpImage,
					ImagePullPolicy: map[string]corev1.PullPolicy{
						"always":       corev1.PullAlways,
						"never":        corev1.PullNever,
						"ifnotpresent": corev1.PullIfNotPresent,
					}[cfg.Cluster.ImagePullPolicy],
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
						{
							Name:      "logs",
							MountPath: cfg.System.LogDirectory,
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyAlways,
		},
	}

	// Configure pod spec for restricted security standard
	if cfg.Cluster.RestrictedPodSecurityStandard {
		if err := e.ConfigurePodSpecForRestrictedStandard(&pod.Spec); err != nil {
			return err
		}
	}

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

	_, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			pod, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if pod.Status.Phase != corev1.PodRunning {
				policy := metav1.DeletePropagationForeground

				if err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(ctx, e.Id, metav1.DeleteOptions{
					GracePeriodSeconds: pointer.Int64(0),
					PropagationPolicy:  &policy,
				}); err != nil {
					return err
				}

				return e.CreateSFTP(ctx, cancelFunc)
			}
		} else {
			e.log().WithField("error", err).Warn("environment/kubernetes: failed to create SFTP pod")
		}
	}

	err = wait.PollUntilWithContext(ctx, time.Second, func(ctx context.Context) (bool, error) {
		pod, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case corev1.PodPending:
			return false, nil
		case corev1.PodRunning:
			return true, nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, fmt.Errorf("pod ran to completion")
		default:
			return false, fmt.Errorf("unknown pod status")
		}
	})
	if err != nil && !errors2.Is(err, context.Canceled) {
		// Don't throw an error if the pod has been deleted in the meantime
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

// Destroy will remove all K8s resources of this server. If the pod
// is currently running it will be forcibly stopped.
func (e *Environment) Destroy() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// We set it to stopping than offline to prevent crash detection from being triggered.
	e.SetState(environment.ProcessStoppingState)

	policy := metav1.DeletePropagationForeground

	// Loop through services with service Kubectyl and server UUID and delete them
	services, err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", e.Id),
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	} else {
		for _, s := range services.Items {
			err := e.client.CoreV1().Services(config.Get().Cluster.Namespace).Delete(ctx, s.Name, metav1.DeleteOptions{
				GracePeriodSeconds: pointer.Int64(30),
				PropagationPolicy:  &policy,
			})
			if err != nil {
				return err
			}
		}
	}

	// Delete configmaps
	err = e.client.CoreV1().ConfigMaps(config.Get().Cluster.Namespace).DeleteCollection(ctx, metav1.DeleteOptions{
		GracePeriodSeconds: pointer.Int64(30),
		PropagationPolicy:  &policy,
	}, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", e.Id),
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Delete pods
	err = e.client.CoreV1().Pods(config.Get().Cluster.Namespace).DeleteCollection(ctx, metav1.DeleteOptions{
		GracePeriodSeconds: pointer.Int64(30),
		PropagationPolicy:  &policy,
	}, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", e.Id),
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Delete pvc
	err = e.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Delete(ctx, e.Id+"-pvc", metav1.DeleteOptions{
		GracePeriodSeconds: pointer.Int64(30),
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
func (e *Environment) Readlog(lines int64) ([]string, error) {
	r := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).GetLogs(e.Id, &corev1.PodLogOptions{
		Container: "process",
		TailLines: pointer.Int64(lines),
	})
	podLogs, err := r.Stream(context.Background())
	if err != nil {
		return nil, nil
	}
	defer podLogs.Close()

	var out []string
	scanner := bufio.NewScanner(podLogs)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}

	return out, nil
}

func (e *Environment) convertMounts() ([]corev1.VolumeMount, []corev1.Volume) {
	var out []corev1.VolumeMount
	var volumes []corev1.Volume

	for i, m := range e.Configuration.Mounts() {
		out = append(out, corev1.VolumeMount{
			Name:      fmt.Sprintf("volume-%d", i),
			MountPath: m.Target,
			ReadOnly:  m.ReadOnly,
		})

		volumes = append(volumes, corev1.Volume{
			Name: fmt.Sprintf("volume-%d", i),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: m.Source,
				},
			},
		})
	}

	return out, volumes
}
