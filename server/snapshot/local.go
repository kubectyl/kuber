package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/remote"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

type LocalBackup struct {
	Backup
}

var _ BackupInterface = (*LocalBackup)(nil)

func NewLocal(snapshotClient *snapshotclientset.Clientset, client remote.Client, clientset *kubernetes.Clientset, uuid string) *LocalBackup {
	return &LocalBackup{
		Backup{
			snapshotClient: snapshotClient,
			clientset:      clientset,
			client:         client,
			Uuid:           uuid,
		},
	}
}

// LocateLocal finds the backup for a server and returns the local path. This
// will obviously only work if the backup was created as a local backup.
func LocateLocal(snapshotClient *snapshotclientset.Clientset, client remote.Client, clientset *kubernetes.Clientset, uuid string) (*LocalBackup, error) {
	b := NewLocal(snapshotClient, client, clientset, uuid)

	return b, nil
}

// Remove removes a snapshot from the system.
func (b *LocalBackup) Remove() error {
	err := b.snapshotClient.SnapshotV1().VolumeSnapshots(config.Get().Cluster.Namespace).Delete(context.Background(), b.Identifier(), metav1.DeleteOptions{})
	if err != nil && errors.IsNotFound(err) {
		return nil
	}

	return err
}

// WithLogContext attaches additional context to the log output for this backup.
func (b *LocalBackup) WithLogContext(c map[string]interface{}) {
	b.logContext = c
}

// Generate generates a backup of the selected files and pushes it to the
// defined location for this instance.
func (b *LocalBackup) Generate(ctx context.Context, sid, ignore string) (*ArchiveDetails, error) {
	b.log().Info("creating snapshot for server")

	cfg := config.Get().Cluster

	pvcName := fmt.Sprintf("%s-pvc", sid)
	snapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      b.Identifier(),
			Namespace: cfg.Namespace,
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &cfg.SnapshotClass,
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}

	_, err := b.snapshotClient.SnapshotV1().VolumeSnapshots(cfg.Namespace).Create(context.Background(), snapshot, metav1.CreateOptions{})
	if err != nil {
		fmt.Println(err.Error())
	}

	// Extract the snapcontent from the VolumeSnapshot object.
	snapcontent := ""
	var size *resource.Quantity

	err = wait.PollImmediate(time.Second, time.Minute*15, func() (bool, error) {
		snapshot, err := b.snapshotClient.SnapshotV1().VolumeSnapshots(cfg.Namespace).Get(context.Background(), b.Identifier(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if snapshot.Status == nil || snapshot.Status.ReadyToUse == nil || !*snapshot.Status.ReadyToUse {
			if snapshot.Status != nil && snapshot.Status.Error != nil {
				return false, fmt.Errorf("%s", *snapshot.Status.Error.Message)
			}
			return false, nil
		} else {
			snapcontent = *snapshot.Status.BoundVolumeSnapshotContentName
			size = snapshot.Status.RestoreSize

			return true, nil
		}
	})
	if err != nil {
		return nil, err
	}
	b.log().Info("created snapshot successfully")

	var parts []remote.BackupPart
	ad := ArchiveDetails{Parts: parts}

	ad.Snapcontent = snapcontent
	ad.Size = size.Value()
	return &ad, nil
}

// Restore will walk over the archive and call the callback function for each
// file encountered.
func (b *LocalBackup) Restore(ctx context.Context, sId string, disk int64, storageClass string, callback RestoreCallback) error {
	cfg := config.Get().Cluster

	snapshot, err := b.snapshotClient.SnapshotV1().VolumeSnapshots(cfg.Namespace).Get(ctx, b.Identifier(), metav1.GetOptions{})
	if err != nil {
		return err
	}

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sId + "-pvc",
			Namespace: config.Get().Cluster.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.PersistentVolumeAccessMode("ReadWriteOnce"),
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					"storage": *resource.NewQuantity(disk, resource.BinarySI),
				},
			},
			StorageClassName: &storageClass,
		},
	}

	if snapshot.Status != nil {
		var seconds int64 = 30
		policy := metav1.DeletePropagationForeground

		err = b.clientset.CoreV1().Pods(cfg.Namespace).DeleteCollection(ctx, metav1.DeleteOptions{
			GracePeriodSeconds: &seconds,
			PropagationPolicy:  &policy,
		}, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", sId),
		})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		if err := wait.PollUntilWithContext(ctx, 3*time.Second, func(ctx context.Context) (bool, error) {
			pods, err := b.clientset.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("Service=Kubectyl,uuid=%s", sId),
			})
			if err != nil {
				return false, err
			}

			if len(pods.Items) == 0 {
				return true, nil
			}

			return false, nil
		}); err != nil {
			return err
		}

		err = b.clientset.CoreV1().PersistentVolumeClaims(cfg.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &seconds,
			PropagationPolicy:  &policy,
		})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		err = wait.PollUntilWithContext(ctx, 3*time.Second, func(ctx context.Context) (bool, error) {
			_, err := b.clientset.CoreV1().PersistentVolumeClaims(cfg.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			if err != nil && !errors.IsNotFound(err) {
				return false, err
			}
			if errors.IsNotFound(err) {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return err
		}

		snapshotAPIGroup := "snapshot.storage.k8s.io"

		restoredPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvc.Name,
				Namespace: cfg.Namespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: pvc.Spec.StorageClassName,
				AccessModes:      pvc.Spec.AccessModes,
				Resources:        pvc.Spec.Resources,
				DataSource: &corev1.TypedLocalObjectReference{
					Kind:     "VolumeSnapshot",
					APIGroup: &snapshotAPIGroup,
					Name:     snapshot.Name,
				},
			},
		}

		_, err = b.clientset.CoreV1().PersistentVolumeClaims(cfg.Namespace).Create(ctx, restoredPVC, metav1.CreateOptions{})
		if err != nil {
			return err
		}

		// Wait for the PVC to become available.
		err = wait.PollUntilWithContext(ctx, 3*time.Second, func(ctx context.Context) (bool, error) {
			pvc, err := b.clientset.CoreV1().PersistentVolumeClaims(cfg.Namespace).Get(ctx, restoredPVC.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return pvc.Status.Phase == corev1.ClaimBound, nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
