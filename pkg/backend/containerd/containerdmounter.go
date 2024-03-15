package containerd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/containerd/containerd/reference/docker"
	"github.com/warm-metal/container-image-csi-driver/pkg/backend"
	"k8s.io/klog/v2"
	k8smount "k8s.io/utils/mount"
)

type SnapshotMounter struct {
	runtime backend.ContainerRuntimeMounterContainerD
	guard   *sync.Mutex
}

func NewContainerdMounter(runtime backend.ContainerRuntimeMounterContainerD, o *Options) *SnapshotMounter {
	mounter := &SnapshotMounter{
		runtime: runtime,
		guard:   &sync.Mutex{},
	}

	mounter.buildSnapshotCacheOrDie(o.StartupTimeout)
	return mounter
}

func (s *SnapshotMounter) buildSnapshotCacheOrDie(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	if err := s.runtime.MigrateOldSnapshotFormat(ctx); err != nil {
		klog.Fatalf("unable to migrate old snapshot format: %s", err)
	}

	snapshots, err := s.runtime.ListSnapshotsWithFilter(ctx, managedFilter)
	if err != nil {
		klog.Fatalf("unable to list snapshots: %s", err)
	}

	klog.Infof("load %d snapshots from runtime", len(snapshots))

	mounter := k8smount.New("")

	for _, metadata := range snapshots {
		key := metadata.GetSnapshotKey()
		if key == "" {
			klog.Fatalf("found a snapshot with a empty key")
		}

		for target := range metadata.GetTargets() {
			if notMount, err := mounter.IsLikelyNotMountPoint(string(target)); err != nil || notMount {
				klog.Errorf("target %q is not a mountpoint yet. trying to release the ref of snapshot %q",
					key)

				_ = s.runtime.RemoveLease(ctx, string(target))
				continue
			}

			klog.Infof("snapshot %q mounted to %s", key, target)
		}
	}
}

func (s *SnapshotMounter) refROSnapshot(
	ctx context.Context, _ backend.MountTarget, image string, key backend.SnapshotKey,
) (err error) {
	s.guard.Lock()
	defer s.guard.Unlock()

	currentSnapshots, err := s.runtime.ListSnapshotsWithFilter(ctx, "name==\""+string(key)+"\"")
	if err != nil {
		return err
	}
	snapshotExists := len(currentSnapshots) > 0
	if snapshotExists {
		return s.runtime.UpdateSnapshotMetadata(ctx, key, buildSnapshotMetaData())
	} else {
		return s.runtime.PrepareReadOnlySnapshot(ctx, image, key, buildSnapshotMetaData())
	}
}

func (s *SnapshotMounter) unrefROSnapshot(ctx context.Context, target backend.MountTarget) {
	s.guard.Lock()
	defer s.guard.Unlock()
	s.runtime.RemoveLease(ctx, string(target))
}

func buildSnapshotMetaData() backend.SnapshotMetadata {
	return backend.SnapshotMetadata{}
}

func (s *SnapshotMounter) Mount(
	ctx context.Context, volumeId string, target backend.MountTarget, image docker.Named, ro bool) (err error) {

	leaseCtx, err := s.runtime.AddLeaseToContext(ctx, string(target))
	if err != nil {
		return err
	}

	var key backend.SnapshotKey
	if ro {
		key = GenSnapshotKey(image.String())
		klog.Infof("refer read-only snapshot of image %q with key %q", image.String(), key)
		if err := s.refROSnapshot(leaseCtx, target, image.String(), key); err != nil {
			return err
		}

		defer func() {
			if err != nil {
				klog.Infof("unref read-only snapshot because of error %s", err)
				s.unrefROSnapshot(leaseCtx, target)
			}
		}()
	} else {
		// For read-write volumes, they must be ephemeral volumes, that which volumeIDs are unique strings.
		key = GenSnapshotKey(volumeId)
		klog.Infof("create read-write snapshot of image %q with key %q", image, key)
		if err := s.runtime.PrepareRWSnapshot(leaseCtx, image.String(), key, nil); err != nil {
			return err
		}

		defer func() {
			if err != nil {
				klog.Infof("unref read-write snapshot because of error %s", err)
				_ = s.runtime.RemoveLease(leaseCtx, string(target))
			}
		}()
	}

	err = s.runtime.Mount(leaseCtx, key, target, ro)
	return err
}

func (s *SnapshotMounter) Unmount(ctx context.Context, volumeId string, target backend.MountTarget, force bool) error {
	klog.Infof("unmount volume %q at %q", volumeId, target)
	if err := s.runtime.Unmount(ctx, target, force); err != nil {
		if !force {
			return err
		}
	}

	s.unrefROSnapshot(ctx, target)
	return nil
}

func (s *SnapshotMounter) ImageExists(ctx context.Context, image docker.Named) bool {
	return s.runtime.ImageExists(ctx, image.String())
}

func GenSnapshotKey(parent string) backend.SnapshotKey {
	return backend.SnapshotKey(fmt.Sprintf("csi-image.warm-metal.tech-%s", parent))
}
