package snapshot

import (
	"context"
	"io"
	"io/fs"

	"github.com/apex/log"
	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"github.com/mholt/archiver/v4"
	"k8s.io/client-go/kubernetes"

	"github.com/kubectyl/kuber/remote"
)

var format = archiver.CompressedArchive{
	Compression: archiver.Gz{},
	Archival:    archiver.Tar{},
}

type AdapterType string

const (
	LocalBackupAdapter AdapterType = "kuber"
)

// RestoreCallback is a generic restoration callback that exists for both local
// and remote backups allowing the files to be restored.
type RestoreCallback func(file string, info fs.FileInfo, r io.ReadCloser) error

// noinspection GoNameStartsWithPackageName
type BackupInterface interface {
	// SetClient sets the API request client on the backup interface.
	SetClient(remote.Client)
	// Identifier returns the UUID of this backup as tracked by the panel
	// instance.
	Identifier() string
	// WithLogContext attaches additional context to the log output for this
	// backup.
	WithLogContext(map[string]interface{})
	// Generate creates a backup in whatever the configured source for the
	// specific implementation is.
	Generate(context.Context, string) (*ArchiveDetails, error)
	// Remove removes a backup file.
	Remove() error
	// Restore is called when a backup is ready to be restored to the disk from
	// the given source. Not every backup implementation will support this nor
	// will every implementation require a reader be provided.
	Restore(context.Context, string, int64, string, RestoreCallback) error
}

type Backup struct {
	// The UUID of this backup object. This must line up with a backup from
	// the panel instance.
	Uuid string `json:"uuid"`

	snapshotClient *snapshotclientset.Clientset
	clientset      *kubernetes.Clientset
	client         remote.Client
	logContext     map[string]interface{}
}

func (b *Backup) SetClient(c remote.Client) {
	b.client = c
}

func (b *Backup) Identifier() string {
	return b.Uuid
}

// Returns a logger instance for this snapshot with the additional context fields
// assigned to the output.
func (b *Backup) log() *log.Entry {
	l := log.WithField("snapshot", b.Identifier())
	for k, v := range b.logContext {
		l = l.WithField(k, v)
	}
	return l
}

type ArchiveDetails struct {
	Snapcontent string              `json:"snapcontent"`
	Size        int64               `json:"size"`
	Parts       []remote.BackupPart `json:"parts"`
}

// ToRequest returns a request object.
func (ad *ArchiveDetails) ToRequest(successful bool) remote.BackupRequest {
	return remote.BackupRequest{
		Snapcontent: ad.Snapcontent,
		Size:        ad.Size,
		Successful:  successful,
		Parts:       ad.Parts,
	}
}
