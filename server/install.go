package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	errors2 "emperror.dev/errors"
	"github.com/apex/log"
	"k8s.io/client-go/kubernetes"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/remote"
	"github.com/kubectyl/kuber/system"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

var deleteFiles = true

// Install executes the installation stack for a server process. Bubbles any
// errors up to the calling function which should handle contacting the panel to
// notify it of the server state.
//
// Pass true as the first argument in order to execute a server sync before the
// process to ensure the latest information is used.
func (s *Server) Install() error {
	return s.install(false)
}

func (s *Server) install(reinstall bool) error {
	var err error
	if !s.Config().SkipRocketScripts {
		// Send the start event so the Panel can automatically update. We don't
		// send this unless the process is actually going to run, otherwise all
		// sorts of weird rapid UI behavior happens since there isn't an actual
		// install process being executed.
		s.Events().Publish(InstallStartedEvent, "")

		err = s.internalInstall()
	} else {
		s.Log().Info("server configured to skip running installation scripts for this rocket, not executing process")
	}

	s.Log().WithField("was_successful", err == nil).Debug("notifying panel of server install state")
	if serr := s.SyncInstallState(err == nil, reinstall); serr != nil {
		l := s.Log().WithField("was_successful", err == nil)

		// If the request was successful but there was an error with this request,
		// attach the error to this log entry. Otherwise, ignore it in this log
		// since whatever is calling this function should handle the error and
		// will end up logging the same one.
		if err == nil {
			l.WithField("error", err)
		}

		l.Warn("failed to notify panel of server install state")
	}

	// Ensure that the server is marked as offline at this point, otherwise you
	// end up with a blank value which is a bit confusing.
	s.Environment.SetState(environment.ProcessOfflineState)

	// Push an event to the websocket, so we can auto-refresh the information in
	// the panel once the installation is completed.
	s.Events().Publish(InstallCompletedEvent, "")

	return errors2.WithStackIf(err)
}

// Reinstall reinstalls a server's software by utilizing the installation script
// for the server rocket. This does not touch any existing files for the server,
// other than what the script modifies.
func (s *Server) Reinstall(delete bool) error {
	deleteFiles = delete

	if s.Environment.State() != environment.ProcessOfflineState {
		s.Log().Debug("waiting for server instance to enter a stopped state")
		if err := s.Environment.WaitForStop(s.Context(), 10*time.Second, true); err != nil {
			return errors2.WrapIf(err, "install: failed to stop running environment")
		}
	}

	s.Log().Info("syncing server state with remote source before executing re-installation process")
	if err := s.Sync(); err != nil {
		return errors2.WrapIf(err, "install: failed to sync server state with Panel")
	}

	return s.install(true)
}

// Internal installation function used to simplify reporting back to the Panel.
func (s *Server) internalInstall() error {
	script, err := s.client.GetInstallationScript(s.Context(), s.ID())
	if err != nil {
		return err
	}
	p, err := NewInstallationProcess(s, &script)
	if err != nil {
		return err
	}

	s.Log().Info("beginning installation process for server")
	if err := p.Run(); err != nil {
		return err
	}

	s.Log().Info("completed installation process for server")

	// Avoid blocking the whole process
	go func() {
		services := s.Environment.GetServiceDetails()
		if len(services) > 0 {
			name := fmt.Sprintf("kuber-%s-tcp", s.ID())
			for _, svc := range services {
				if svc.Name != name {
					continue
				}

				ip := svc.Spec.ClusterIP
				port := config.Get().System.Sftp.Port

				switch svc.Spec.Type {
				case "LoadBalancer":
					if len(svc.Status.LoadBalancer.Ingress) > 0 {
						ip = svc.Status.LoadBalancer.Ingress[0].IP
						if len(svc.Spec.Ports) > 0 {
							port = int(svc.Spec.Ports[0].Port)
						}
					}
				case "NodePort":
					if len(svc.Spec.Ports) > 0 {
						port = int(svc.Spec.Ports[0].NodePort)
					}
				}

				if err := s.fs.SetManager(fmt.Sprintf("%s:%v", ip, port)); err != nil {
					return
				}
			}
		}
		s.Log().Info("booting server SFTP process for the first time after installation")
		if err := s.Environment.CreateSFTP(s.Context()); err != nil {
			return
		}
	}()

	return nil
}

type InstallationProcess struct {
	Server *Server
	Script *remote.InstallationScript
	client *kubernetes.Clientset
}

// NewInstallationProcess returns a new installation process struct that will be
// used to create containers and otherwise perform installation commands for a
// server.
func NewInstallationProcess(s *Server, script *remote.InstallationScript) (*InstallationProcess, error) {
	proc := &InstallationProcess{
		Script: script,
		Server: s,
	}

	if _, c, err := environment.Cluster(); err != nil {
		return nil, err
	} else {
		proc.client = c
	}

	return proc, nil
}

// IsInstalling returns if the server is actively running the installation
// process by checking the status of the installer lock.
func (s *Server) IsInstalling() bool {
	return s.installing.Load()
}

func (s *Server) IsTransferring() bool {
	return s.transferring.Load()
}

func (s *Server) SetTransferring(state bool) {
	s.transferring.Store(state)
}

func (s *Server) IsRestoring() bool {
	return s.restoring.Load()
}

func (s *Server) SetRestoring(state bool) {
	s.restoring.Store(state)
}

// RemoveContainer removes the installation container for the server.
func (ip *InstallationProcess) RemoveContainer() error {
	policy := metav1.DeletePropagationForeground
	if err := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).DeleteCollection(
		ip.Server.Context(),
		metav1.DeleteOptions{
			PropagationPolicy: &policy,
		},
		metav1.ListOptions{
			LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", ip.Server.ID()),
		},
	); err != nil {
		return errors2.WithMessage(err, "failed to delete PODs associated with PVC")
	}

	if err := wait.Poll(5*time.Second, 1*time.Minute, func() (bool, error) {
		pods, err := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).List(ip.Server.Context(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", ip.Server.ID()),
		})
		if err != nil {
			return false, err
		}

		if len(pods.Items) == 0 {
			return true, nil
		}

		return false, nil
	}); err != nil {
		return errors2.WithMessage(err, "failed while waiting for PODs to be deleted")
	}

	if err := ip.client.CoreV1().ConfigMaps(config.Get().Cluster.Namespace).Delete(ip.Server.Context(), fmt.Sprintf("%s-installer", ip.Server.ID()), metav1.DeleteOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// Run runs the installation process, this is done as in a background thread.
// This will configure the required environment, and then spin up the
// installation container. Once the container finishes installing the results
// are stored in an installation log in the server's configuration directory.
func (ip *InstallationProcess) Run() error {
	ip.Server.Log().Debug("acquiring installation process lock")
	if !ip.Server.installing.SwapIf(true) {
		return errors2.New("install: cannot obtain installation lock")
	}

	// We now have an exclusive lock on this installation process. Ensure that whenever this
	// process is finished that the semaphore is released so that other processes and be executed
	// without encountering a wait timeout.
	defer func() {
		ip.Server.Log().Debug("releasing installation process lock")
		ip.Server.installing.Store(false)
	}()

	if err := ip.BeforeExecute(); err != nil {
		return err
	}

	cID, err := ip.Execute()
	if err != nil {
		_ = ip.RemoveContainer()
		return err
	}

	// If this step fails, log a warning but don't exit out of the process. This is completely
	// internal to the daemon's functionality, and does not affect the status of the server itself.
	if err := ip.AfterExecute(cID); err != nil {
		ip.Server.Log().WithField("error", err).Warn("failed to complete after-execute step of installation process")
	}

	return nil
}

// Returns the location of the temporary data for the installation process.
func (ip *InstallationProcess) tempDir() string {
	return filepath.Join(config.Get().System.TmpDirectory, ip.Server.ID())
}

// Writes the installation script to a temporary file on the host machine so that it
// can be properly mounted into the installation container and then executed.
func (ip *InstallationProcess) writeScriptToDisk() error {
	// Make sure the temp directory root exists before trying to make a directory within it. The
	// ioutil.TempDir call expects this base to exist, it won't create it for you.
	if err := os.MkdirAll(ip.tempDir(), 0o700); err != nil {
		return errors2.WithMessage(err, "could not create temporary directory for install process")
	}

	f, err := os.OpenFile(filepath.Join(ip.tempDir(), "install.sh"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return errors2.WithMessage(err, "failed to write server installation script to disk before mount")
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	scanner := bufio.NewScanner(bytes.NewReader([]byte(ip.Script.Script)))
	for scanner.Scan() {
		w.WriteString(scanner.Text() + "\n")
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	w.Flush()

	return nil
}

// BeforeExecute runs before the container is executed. This pulls down the
// required docker container image as well as writes the installation script to
// the disk. This process is executed in an async manner, if either one fails
// the error is returned.
func (ip *InstallationProcess) BeforeExecute() error {
	if err := ip.writeScriptToDisk(); err != nil {
		return errors2.WithMessage(err, "failed to write installation script to disk")
	}

	if err := ip.RemoveContainer(); err != nil {
		return errors2.WithMessage(err, "failed to remove existing install container for server")
	}

	if deleteFiles {
		if err := ip.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Delete(context.Background(), ip.Server.ID()+"-pvc", metav1.DeleteOptions{}); err != nil {
			if !errors.IsNotFound(err) {
				return errors2.WithMessage(err, "failed to remove PVC before running installation")
			}
		}

		if err := wait.PollUntilWithContext(ip.Server.Context(), time.Second, func(ctx context.Context) (bool, error) {
			_, err := ip.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Get(ctx, fmt.Sprintf("%s-pvc", ip.Server.ID()), metav1.GetOptions{})
			if err != nil && !errors.IsNotFound(err) {
				return false, err
			}

			if errors.IsNotFound(err) {
				return true, nil
			}

			return false, nil
		}); err != nil {
			return errors2.WithMessage(err, "failed while waiting for PVC to be deleted")
		}
	} else {
		ip.Server.Log().Info("skipping PVC deletion, disabled by user")
	}
	return nil
}

// GetLogPath returns the log path for the installation process.
func (ip *InstallationProcess) GetLogPath() string {
	return filepath.Join(config.Get().System.LogDirectory, "/install", ip.Server.ID()+".log")
}

// AfterExecute cleans up after the execution of the installation process.
// This grabs the logs from the process to store in the server configuration
// directory, and then destroys the associated installation container.
func (ip *InstallationProcess) AfterExecute(containerId string) error {
	defer ip.RemoveContainer()

	ip.Server.Log().WithField("pod_id", containerId).Debug("pulling installation logs for server")
	reader := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).GetLogs(ip.Server.ID()+"-installer", &corev1.PodLogOptions{
		Follow: false,
	})
	podLogs, err := reader.Stream(ip.Server.Context())
	if err != nil {
		return err
	}
	defer podLogs.Close()

	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	f, err := os.OpenFile(ip.GetLogPath(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	// We write the contents of the container output to a more "permanent" file so that they
	// can be referenced after this container is deleted. We'll also include the environment
	// variables passed into the container to make debugging things a little easier.
	ip.Server.Log().WithField("path", ip.GetLogPath()).Debug("writing most recent installation logs to disk")

	tmpl, err := template.New("header").Parse(`Kubectyl Server Installation Log

|
| Details
| ------------------------------
  Server UUID:          {{.Server.ID}}
  Container Image:      {{.Script.ContainerImage}}
  Container Entrypoint: {{.Script.Entrypoint}}

|
| Environment Variables
| ------------------------------
{{ range $key, $value := .Server.GetEnvironmentVariables }}  {{ $value }}
{{ end }}

|
| Script Output
| ------------------------------
`)
	if err != nil {
		return err
	}

	if err := tmpl.Execute(f, ip); err != nil {
		return err
	}

	if _, err := io.Copy(f, podLogs); err != nil {
		return err
	}

	return nil
}

// Execute executes the installation process inside a specially created Kubernetes pod
// container.
func (ip *InstallationProcess) Execute() (string, error) {
	// Create a child context that is canceled once this function is done running. This
	// will also be canceled if the parent context (from the Server struct) is canceled
	// which occurs if the server is deleted.
	ctx, cancel := context.WithCancel(ip.Server.Context())
	defer cancel()

	fileContents, err := os.ReadFile(filepath.Join(ip.tempDir(), "install.sh"))
	if err != nil {
		return "", err
	}

	configmap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ip.Server.ID() + "-installer",
			Namespace: config.Get().Cluster.Namespace,
		},
		Data: map[string]string{
			"install.sh": string(fileContents),
		},
	}

	_, err = ip.client.CoreV1().ConfigMaps(config.Get().Cluster.Namespace).Create(ctx, configmap, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		ip.Server.Log().WithField("error", err).Warn("failed to create configmap")
	}

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ip.Server.ID() + "-pvc",
			Namespace: config.Get().Cluster.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.PersistentVolumeAccessMode("ReadWriteOnce"),
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					"storage": *resource.NewQuantity(ip.Server.DiskSpace(), resource.BinarySI),
				},
			},
			StorageClassName: &[]string{config.Get().Cluster.StorageClass}[0],
		},
	}

	// Override server storage class if defined
	if len(ip.Server.cfg.StorageClass) != 0 {
		pvc.Spec.StorageClassName = &ip.Server.cfg.StorageClass
	}

	_, err = ip.client.CoreV1().PersistentVolumeClaims(config.Get().Cluster.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", err
	}

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ip.Server.ID() + "-installer",
			Namespace: config.Get().Cluster.Namespace,
			Labels: map[string]string{
				"uuid":    ip.Server.ID(),
				"Service": "Kubectyl",
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: ip.Server.ID() + "-pvc",
						},
					},
				},
				{
					Name: "configmap",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: ip.Server.ID() + "-installer",
							},
							DefaultMode: &[]int32{int32(0755)}[0],
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "installer",
					Image: ip.Script.ContainerImage,
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: &[]bool{false}[0],
						RunAsUser:                &[]int64{int64(0)}[0],
					},
					Command: []string{
						"/mnt/install/install.sh",
					},
					Resources: corev1.ResourceRequirements{},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "configmap",
							MountPath: "/mnt/install",
						},
						{
							Name:      "storage",
							MountPath: "/mnt/server",
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicy("Never"),
		},
	}

	if len(ip.Server.Config().NodeSelectors) > 0 {
		pod.Spec.NodeSelector = map[string]string{}

		// Loop through the map and create a node selector string
		for k, v := range ip.Server.cfg.NodeSelectors {
			if !environment.LabelNameRegex.MatchString(k) {
				continue
			}
			pod.Spec.NodeSelector[k] = v
			if !environment.LabelValueRegex.MatchString(v) {
				pod.Spec.NodeSelector[k] = ""
			}
		}
	}

	// Env
	for _, k := range ip.Server.GetEnvironmentVariables() {
		a := strings.SplitN(k, "=", 2)

		if a[0] != "" && a[1] != "" {
			pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: a[0], Value: a[1]})
		}
	}

	cfg := config.Get()
	securityContext := pod.Spec.Containers[0].SecurityContext
	if cfg.System.User.Rootless.Enabled {
		securityContext.RunAsNonRoot = &[]bool{false}[0]
		securityContext.RunAsUser = &[]int64{int64(cfg.System.User.Rootless.ContainerUID)}[0]
		securityContext.RunAsGroup = &[]int64{int64(cfg.System.User.Rootless.ContainerGID)}[0]
	}

	ip.Server.Log().WithField("install_script", ip.tempDir()+"/install.sh").Info("creating install container for server process")
	// Remove the temporary directory when the installation process finishes for this server container.
	defer func() {
		if err := os.RemoveAll(ip.tempDir()); err != nil {
			if !os.IsNotExist(err) {
				ip.Server.Log().WithField("error", err).Warn("failed to remove temporary data directory after install process")
			}
		}
	}()

	r, err := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	ip.Server.Log().WithField("pod_id", r.UID).Info("running installation script for server in container")

	// Process the install event in the background by listening to the stream output until the
	// container has stopped, at which point we'll disconnect from it.
	//
	// If there is an error during the streaming output just report it and do nothing else, the
	// install can still run, the console just won't have any output.
	go func(id string) {
		ip.Server.Events().Publish(DaemonMessageEvent, "Starting installation process, this could take a few minutes...")

		err = wait.PollInfinite(time.Second, func() (bool, error) {
			pod, err := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(context.TODO(), ip.Server.ID()+"-installer", metav1.GetOptions{})
			if err != nil {
				return false, err
			}

			switch pod.Status.Phase {
			case corev1.PodRunning:
				return true, nil
			case corev1.PodFailed, corev1.PodSucceeded:
				return false, nil
			}
			return false, nil
		})
		if err != nil {
			ip.Server.Log().WithField("error", err).Warn("pod never entered running phase")
		}

		if err := ip.StreamOutput(ctx, id); err != nil {
			ip.Server.Log().WithField("error", err).Warn("error connecting to server install stream output")
		}
	}(string(r.UID))

	err = wait.PollInfinite(time.Second, func() (bool, error) {
		pod, err := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(context.TODO(), ip.Server.ID()+"-installer", metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			return false, nil
		}
		return false, nil
	})
	// Once the container has stopped running we can mark the install process as being completed.
	if err == nil {
		ip.Server.Events().Publish(DaemonMessageEvent, "Installation process completed.")
	} else {
		return "", err
	}

	return string(r.UID), nil
}

// StreamOutput streams the output of the installation process to a log file in
// the server configuration directory, as well as to a websocket listener so
// that the process can be viewed in the panel by administrators.
func (ip *InstallationProcess) StreamOutput(ctx context.Context, id string) error {
	req := ip.client.CoreV1().Pods(config.Get().Cluster.Namespace).GetLogs(ip.Server.ID()+"-installer", &corev1.PodLogOptions{
		Follow: true,
	})
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return err
	}
	defer podLogs.Close()

	err = system.ScanReader(podLogs, ip.Server.Sink(system.InstallSink).Push)
	if err != nil && !errors2.Is(err, context.Canceled) {
		ip.Server.Log().WithFields(log.Fields{"pod_id": id, "error": err}).Warn("error processing install output lines")
	}
	return nil
}

// SyncInstallState makes an HTTP request to the Panel instance notifying it that
// the server has completed the installation process, and what the state of the
// server is.
func (s *Server) SyncInstallState(successful, reinstall bool) error {
	return s.client.SetInstallationStatus(s.Context(), s.ID(), remote.InstallStatusRequest{
		Successful: successful,
		Reinstall:  reinstall,
	})
}
