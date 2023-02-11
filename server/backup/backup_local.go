package backup

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"emperror.dev/errors"
	"github.com/juju/ratelimit"
	"github.com/mholt/archiver/v4"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/remote"
	"github.com/kubectyl/kuber/server/filesystem"

	corev1 "k8s.io/api/core/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

type LocalBackup struct {
	Backup
}

var _ BackupInterface = (*LocalBackup)(nil)

func NewLocal(client remote.Client, uuid string, ignore string) *LocalBackup {
	return &LocalBackup{
		Backup{
			client:  client,
			Uuid:    uuid,
			Ignore:  ignore,
			adapter: LocalBackupAdapter,
		},
	}
}

// LocateLocal finds the backup for a server and returns the local path. This
// will obviously only work if the backup was created as a local backup.
func LocateLocal(client remote.Client, uuid string) (*LocalBackup, os.FileInfo, error) {
	b := NewLocal(client, uuid, "")
	st, err := os.Stat(b.Path())
	if err != nil {
		return nil, nil, err
	}

	if st.IsDir() {
		return nil, nil, errors.New("invalid archive, is directory")
	}

	return b, st, nil
}

// Remove removes a backup from the system.
func (b *LocalBackup) Remove() error {
	return os.Remove(b.Path())
}

// WithLogContext attaches additional context to the log output for this backup.
func (b *LocalBackup) WithLogContext(c map[string]interface{}) {
	b.logContext = c
}

// Generate generates a backup of the selected files and pushes it to the
// defined location for this instance.
func (b *LocalBackup) Generate(ctx context.Context, sID, ignore string) (*ArchiveDetails, error) {
	a := &filesystem.Archive{
		BasePath: path.Join(config.Get().System.BackupDirectory, b.Identifier()),
		Ignore:   ignore,
	}

	b.log().WithField("path", b.Path()).Info("creating backup for server")
	defer os.RemoveAll(a.BasePath)

	if err := b.copyFromPod("/home/container", sID); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			fmt.Println("Error2: ", err)
			return nil, err
		}
	}
	b.log().Info("created backup successfully")

	ad, err := b.Details(ctx, nil)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details for local backup")
	}
	return ad, nil
}

func InitRestClient() (*rest.Config, *corev1client.CoreV1Client, error) {
	// cfg := config.Get().Cluster

	// c := &rest.Config{
	// 	Host:        cfg.Host,
	// 	BearerToken: cfg.BearerToken,
	// 	TLSClientConfig: rest.TLSClientConfig{
	// 		Insecure: cfg.Insecure,
	// 	},
	// }

	c, _, _ := environment.Cluster()

	client, err := corev1client.NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return c, client, err
}

func (b *Backup) copyFromPod(srcPath, sID string) error {
	restconfig, coreclient, err := InitRestClient()
	reader, outStream := io.Pipe()

	req := coreclient.RESTClient().
		Get().
		Namespace(config.Get().Cluster.Namespace).
		Resource("pods").
		Name(sID).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "process",
			Command:   []string{"bash", "-c", fmt.Sprintf("tar cf - %s | cat", srcPath)},
			Stdin:     false,
			Stdout:    true,
			Stderr:    false,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(restconfig, "POST", req.URL())
	if err != nil {
		log.Fatalf("error %s\n", err)
		return err
	}

	go func() {
		defer outStream.Close()
		err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
			Stdout: outStream,
		})
	}()

	prefix := strings.TrimLeft(srcPath, "/")
	prefix = path.Clean(prefix)
	prefix = string(prefix)
	err = b.untarAll(reader, prefix)
	return err
}

func (b *Backup) untarAll(reader io.Reader, prefix string) error {
	out, err := os.Create(b.Path())
	if err != nil {
		log.Fatalln("Error writing archive:", err)
	}
	defer out.Close()

	tw := tar.NewWriter(out)
	defer tw.Close()

	tarReader := tar.NewReader(reader)

	for {
		header, err := tarReader.Next()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		if header.FileInfo().IsDir() {
			continue
		}

		header.Name = header.Name[len(prefix):]
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if _, err := io.Copy(tw, tarReader); err != nil {
			return err
		}
	}

	return nil
}

// Restore will walk over the archive and call the callback function for each
// file encountered.
func (b *LocalBackup) Restore(ctx context.Context, _ io.Reader, callback RestoreCallback) error {
	f, err := os.Open(b.Path())
	if err != nil {
		return err
	}

	var reader io.Reader = f
	// Steal the logic we use for making backups which will be applied when restoring
	// this specific backup. This allows us to prevent overloading the disk unintentionally.
	if writeLimit := int64(config.Get().System.Backups.WriteLimit * 1024 * 1024); writeLimit > 0 {
		reader = ratelimit.Reader(f, ratelimit.NewBucketWithRate(float64(writeLimit), writeLimit))
	}
	if err := format.Extract(ctx, reader, nil, func(ctx context.Context, f archiver.File) error {
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()

		return callback(filesystem.ExtractNameFromArchive(f), f.FileInfo, r)
	}); err != nil {
		return err
	}
	return nil
}
