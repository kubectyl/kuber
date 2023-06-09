package server

import (
	"io"
	"os"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/docker/docker/client"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/remote"
	"github.com/kubectyl/kuber/server/snapshot"
)

// Notifies the panel of a snapshot's state and returns an error if one is encountered
// while performing this action.
func (s *Server) notifyPanelOfSnapshot(uuid string, ad *snapshot.ArchiveDetails, successful bool) error {
	if err := s.client.SetSnapshotStatus(s.Context(), uuid, ad.ToRequest(successful)); err != nil {
		if !remote.IsRequestError(err) {
			s.Log().WithFields(log.Fields{
				"snapshot": uuid,
				"error":    err,
			}).Error("failed to notify panel of snapshot status due to kuber error")
			return err
		}

		return errors.New(err.Error())
	}

	return nil
}

// Get all of the ignored files for a server based on its .pteroignore file in the root.
func (s *Server) getServerwideIgnoredFiles() (string, error) {
	f, st, err := s.Filesystem().File(".pteroignore")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	defer f.Close()
	if st.Mode()&os.ModeSymlink != 0 || st.Size() > 32*1024 {
		// Don't read a symlinked ignore file, or a file larger than 32KiB in size.
		return "", nil
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Snapshot performs a server snapshot and then emits the event over the server
// websocket. We let the actual snapshot system handle notifying the panel of the
// status, but that won't emit a websocket event.
func (s *Server) Snapshot(b snapshot.BackupInterface) error {
	ad, err := b.Generate(s.Context(), s.ID(), "")
	if err != nil {
		if err := s.notifyPanelOfSnapshot(b.Identifier(), &snapshot.ArchiveDetails{}, false); err != nil {
			s.Log().WithFields(log.Fields{
				"snapshot": b.Identifier(),
				"error":    err,
			}).Warn("failed to notify panel of failed snapshot state")
		} else {
			s.Log().WithField("snapshot", b.Identifier()).Info("notified panel of failed snapshot state")
		}

		s.Events().Publish(BackupCompletedEvent+":"+b.Identifier(), map[string]interface{}{
			"uuid":          b.Identifier(),
			"is_successful": false,
			"snapcontent":   "",
			"file_size":     0,
		})

		return errors.WrapIf(err, "snapshot: error while generating server snapshot")
	}

	// Try to notify the panel about the status of this snapshot. If for some reason this request
	// fails, delete the archive from the daemon and return that error up the chain to the caller.
	if notifyError := s.notifyPanelOfSnapshot(b.Identifier(), ad, true); notifyError != nil {
		_ = b.Remove()

		s.Log().WithField("error", notifyError).Info("failed to notify panel of successful snapshot state")
		return err
	} else {
		s.Log().WithField("snapshot", b.Identifier()).Info("notified panel of successful snapshot state")
	}

	// Emit an event over the socket so we can update the snapshot in realtime on
	// the frontend for the server.
	s.Events().Publish(BackupCompletedEvent+":"+b.Identifier(), map[string]interface{}{
		"uuid":          b.Identifier(),
		"is_successful": true,
		"snapcontent":   ad.Snapcontent,
		"file_size":     ad.Size,
	})

	return nil
}

// RestoreBackup calls the Restore function on the provided snapshot. Once this
// restoration is completed an event is emitted to the websocket to notify the
// Panel that is has been completed.
//
// In addition to the websocket event an API call is triggered to notify the
// Panel of the new state.
func (s *Server) RestoreBackup(b snapshot.BackupInterface, reader io.ReadCloser) (err error) {
	s.Config().SetSuspended(true)
	// Local snapshots will not pass a reader through to this function, so check first
	// to make sure it is a valid reader before trying to close it.
	defer func() {
		s.Config().SetSuspended(false)
		if reader != nil {
			_ = reader.Close()
		}
	}()
	// Send an API call to the Panel as soon as this function is done running so that
	// the Panel is informed of the restoration status of this snapshot.
	defer func() {
		if rerr := s.client.SendRestorationStatus(s.Context(), b.Identifier(), err == nil); rerr != nil {
			s.Log().WithField("error", rerr).WithField("snapshot", b.Identifier()).Error("failed to notify Panel of snapshot restoration status")
		}
	}()

	// Don't try to restore the server until we have completely stopped the running
	// instance, otherwise you'll likely hit all types of write errors due to the
	// server being suspended.
	if s.Environment.State() != environment.ProcessOfflineState {
		if err = s.Environment.WaitForStop(s.Context(), 2*time.Minute, true); err != nil {
			if !client.IsErrNotFound(err) {
				return errors.WrapIf(err, "server/snapshot: restore: failed to wait for container stop")
			}
		}
	}

	// Attempt to restore the snapshot to the server by running through each entry
	// in the file one at a time and writing them to the disk.
	s.Log().Debug("starting process for snapshot restoration")

	storageClass := config.Get().Cluster.StorageClass
	// Override storage class
	if len(s.cfg.StorageClass) > 0 {
		storageClass = s.cfg.StorageClass
	}

	err = b.Restore(s.Context(), s.ID(), s.DiskSpace(), storageClass, nil)

	return errors.WithStackIf(err)
}
