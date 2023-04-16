package environment

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/kubectyl/kuber/config"
	"gopkg.in/yaml.v2"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	var caData []byte
	if cfg.KeyFile != "" && !cfg.Insecure {
		// Load the cluster certificate authority data
		caData, err = ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			fmt.Printf("Error reading certificate authority data: %v\n", err)
			return
		}

		c.TLSClientConfig.CAData = caData
	}

	var certData []byte
	if cfg.KeyFile != "" && !cfg.Insecure {
		// Load the client certificate data
		certData, err = ioutil.ReadFile(cfg.CertFile)
		if err != nil {
			fmt.Printf("Error reading client certificate data: %v\n", err)
			return
		}

		c.TLSClientConfig.CertData = certData
	}

	var keyData []byte
	if cfg.KeyFile != "" && !cfg.Insecure {
		// Load the client key data
		keyData, err = ioutil.ReadFile(cfg.KeyFile)
		if err != nil {
			fmt.Printf("Error reading client key data: %v\n", err)
			return
		}

		c.TLSClientConfig.KeyData = keyData
	}

	client, err := kubernetes.NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return c, client, err
}

func CreateSftpConfigmap() error {
	_, c, err := Cluster()
	if err != nil {
		return err
	}

	cfg := config.Get()

	tempDir := cfg.System.TmpDirectory
	tempFile := filepath.Join(tempDir, "sftp.yaml")

	data := map[string]interface{}{
		"debug":    cfg.Debug,
		"token_id": cfg.AuthenticationTokenId,
		"token":    cfg.AuthenticationToken,
		"system": map[string]interface{}{
			"log_directory": cfg.System.LogDirectory,
			"data":          cfg.System.Data,
			"sftp":          cfg.System.Sftp,
		},
		"remote":       cfg.PanelLocation,
		"remote_query": cfg.RemoteQuery,
	}

	yamlData, err := yaml.Marshal(&data)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		return errors.WithMessage(err, "could not create temporary directory for sftp configmap")
	}

	err = os.WriteFile(tempFile, yamlData, 0644)
	if err != nil {
		return err
	}
	defer os.Remove(tempFile)

	fileContents, err := os.ReadFile(tempFile)
	if err != nil {
		return err
	}

	configmap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sftp",
			Namespace: config.Get().Cluster.Namespace,
		},
		Data: map[string]string{
			filepath.Base(config.DefaultLocation): string(fileContents),
		},
	}

	client := c.CoreV1().ConfigMaps(config.Get().Cluster.Namespace)

	log.WithField("configmap", "sftp").Info("checking and updating sftp configmap")
	_, errG := client.Create(context.TODO(), configmap, metav1.CreateOptions{})
	if errG != nil {
		if apierrors.IsAlreadyExists(errG) {
			cm, err := client.Get(context.TODO(), configmap.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}

			cm.Data = map[string]string{
				filepath.Base(config.DefaultLocation): string(fileContents),
			}

			_, err = client.Update(context.TODO(), cm, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		} else {
			return errG
		}
	}

	return nil
}

func CreateSftpSecret() error {
	_, c, err := Cluster()
	if err != nil {
		return err
	}

	secret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ed25519",
			Namespace: config.Get().Cluster.Namespace,
		},
		Type: corev1.SSHAuthPrivateKey,
	}

	client := c.CoreV1().Secrets(config.Get().Cluster.Namespace)

	sec, err := client.Get(context.Background(), "ed25519", metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if apierrors.IsNotFound(err) {
		if _, err := os.Stat(PrivateKeyPath()); os.IsNotExist(err) {
			if err := generateED25519PrivateKey(); err != nil {
				return err
			}
		} else if err != nil {
			return errors.Wrap(err, "sftp: could not stat private key file")
		}

		fileContents, err := os.ReadFile(PrivateKeyPath())
		if err != nil {
			return err
		}

		secret.Data = map[string][]byte{
			"id_ed25519": fileContents,
		}

		if _, err = client.Create(context.Background(), secret, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else {
		if err := os.MkdirAll(path.Dir(PrivateKeyPath()), 0o755); err != nil {
			return errors.Wrap(err, "sftp: could not create internal sftp data directory")
		}

		privateKeyFile, err := os.OpenFile(PrivateKeyPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer privateKeyFile.Close()

		privateKeyWriter := bufio.NewWriter(privateKeyFile)
		scanner := bufio.NewScanner(bytes.NewReader(sec.Data["id_ed25519"]))

		for scanner.Scan() {
			privateKeyWriter.WriteString(scanner.Text() + "\n")
		}

		if err := scanner.Err(); err != nil {
			return err
		}

		if err := privateKeyWriter.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Generates a new ED25519 private key that is used for host authentication when
// a user connects to the SFTP server.
func generateED25519PrivateKey() error {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return errors.Wrap(err, "sftp: failed to generate ED25519 private key")
	}
	if err := os.MkdirAll(path.Dir(PrivateKeyPath()), 0o755); err != nil {
		return errors.Wrap(err, "sftp: could not create internal sftp data directory")
	}
	o, err := os.OpenFile(PrivateKeyPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return errors.WithStack(err)
	}
	defer o.Close()

	b, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return errors.Wrap(err, "sftp: failed to marshal private key into bytes")
	}
	if err := pem.Encode(o, &pem.Block{Type: "PRIVATE KEY", Bytes: b}); err != nil {
		return errors.Wrap(err, "sftp: failed to write ED25519 private key to disk")
	}
	return nil
}

// PrivateKeyPath returns the path the host private key for this server instance.
func PrivateKeyPath() string {
	return path.Join(config.Get().System.Data, ".sftp/id_ed25519")
}
