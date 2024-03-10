package wmbuildah

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/reference/docker"
	"github.com/containers/buildah"
	"github.com/containers/common/libimage"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage"
	"github.com/containers/storage/pkg/reexec"
	"github.com/warm-metal/container-image-csi-driver/pkg/backend"
	"k8s.io/klog/v2"
	k8smount "k8s.io/mount-utils"
)

var (
	buildAhPrepareSnapshot = "buildah-prepare-snapshot"
)

func init() {
	reexec.Register(buildAhPrepareSnapshot, prepareSnapshotMain)
}

type snapshotMounter struct {
	store     storage.Store
	mountLock *sync.Mutex
	mounter   k8smount.Interface
}

type Options struct {
	StartupTimeout time.Duration
}

func NewMounter(o *Options) backend.Mounter {

	storageOptions, err := storage.DefaultStoreOptions()
	if err != nil {
		klog.Fatalf("unable to get default store options: %s", err)
	}

	store, err := storage.GetStore(storageOptions)
	if err != nil {
		klog.Fatalf("unable to create image store: %s", err)
	}

	return backend.NewMounter(&snapshotMounter{
		store:     store,
		mounter:   k8smount.New(""),
		mountLock: &sync.Mutex{},
	})
}

func (s snapshotMounter) Mount(ctx context.Context, key backend.SnapshotKey, target backend.MountTarget, ro bool) error {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	builder, err := buildah.OpenBuilder(s.store, string(key))
	if err != nil {
		klog.Errorf("unable to open builder for snapshot %q: %s", key, err)
		return err
	}

	isMounted, err := builder.Mounted()
	if !isMounted || err != nil {
		klog.Errorf("snapshot %q is not mounted: %s", key, err)
		return err
	}

	mountPoint := builder.MountPoint

	mountOpts := []string{"bind"}
	if ro {
		mountOpts = append(mountOpts, "ro")
	}

	err = s.mounter.Mount(mountPoint, string(target), "", mountOpts)
	if err != nil {
		klog.Errorf("unable to bind %q to %q: %s", mountPoint, target, err)
		return err
	}

	return nil

}

func (s snapshotMounter) Unmount(_ context.Context, target backend.MountTarget, force bool) error {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	if err := s.mounter.Unmount(string(target)); err != nil {
		klog.Errorf("unable to unmount %q: %s", target, err)
		if !force {
			return err
		}
	}

	return nil
}

func (s snapshotMounter) ImageExists(ctx context.Context, image docker.Named) bool {
	systemContext := &types.SystemContext{}
	runtime, err := libimage.RuntimeFromStore(s.store, &libimage.RuntimeOptions{SystemContext: systemContext})

	if err != nil {
		klog.Fatalf("unable to create runtime: %s", err)
	}

	i, _, err := runtime.LookupImage(image.String(), &libimage.LookupImageOptions{})
	return i != nil && err == nil
}

func (s snapshotMounter) GetImageIDOrDie(ctx context.Context, image docker.Named) string {
	systemContext := &types.SystemContext{}
	runtime, err := libimage.RuntimeFromStore(s.store, &libimage.RuntimeOptions{SystemContext: systemContext})

	if err != nil {
		klog.Fatalf("unable to create runtime: %s", err)
	}

	i, _, err := runtime.LookupImage(image.String(), &libimage.LookupImageOptions{})
	if err != nil {
		klog.Fatalf("unable to lookup image %q: %s", image, err)
	}

	return i.ID()
}

func (s snapshotMounter) AddLeaseToContext(ctx context.Context, target string) (context.Context, error) {
	return ctx, nil
}

func (s snapshotMounter) RemoveLease(ctx context.Context, target string) error {
	return nil
}

func (s snapshotMounter) ListSnapshotsWithFilter(context.Context, ...string) ([]backend.SnapshotMetadata, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s snapshotMounter) MigrateOldSnapshotFormat(_ context.Context) error {
	return nil
}

func (s snapshotMounter) PrepareReadOnlySnapshot(
	ctx context.Context, imageID string, key backend.SnapshotKey, metadata backend.SnapshotMetadata,
) error {
	return s.prepareSnapshot(ctx, imageID, key, metadata)
}

func (s snapshotMounter) PrepareRWSnapshot(
	ctx context.Context, imageID string, key backend.SnapshotKey, metadata backend.SnapshotMetadata,
) error {
	return s.prepareSnapshot(ctx, imageID, key, metadata)
}

func prepareSnapshotMain() {
	// this is executed in a child process as it sometimes leaks memory
	// and it should also not be cancelled by the parent process
	flag.Parse()
	if len(flag.Args()) != 3 {
		klog.Fatal("usage: buildah-prepare-snapshot <image> <container> <metadata>")
	}

	defaultOptions, err := storage.DefaultStoreOptions()
	if err != nil {
		klog.Fatalf("unable to get default store options: %s", err)
	}

	store, err := storage.GetStore(defaultOptions)
	if err != nil {
		klog.Fatalf("unable to create image store: %s", err)
	}

	builder, err := buildah.NewBuilder(context.Background(), store, buildah.BuilderOptions{
		FromImage:  flag.Arg(0),
		Container:  flag.Arg(1),
		PullPolicy: buildah.PullNever,
	})

	if err != nil {
		klog.Fatalf("unable to create builder for snapshot %q: %s", flag.Arg(1), err)
	}

	defer func() {
		if err != nil {
			if err := builder.Delete(); err != nil {
				klog.Errorf("unable to delete builder for snapshot %q: %s", flag.Arg(1), err)
			}
		}
	}()
	
	builder.SetLabel(labelLabel, flag.Arg(2))
	_, err = builder.Mount("")
	if err != nil {
		klog.Fatalf("unable to mount snapshot %q: %s", flag.Arg(1), err)
	}
	err = builder.Save()
	if err != nil {
		klog.Fatalf("unable to save snapshot %q: %s", flag.Arg(1), err)
	}
	os.Exit(0)
}

func (s snapshotMounter) prepareSnapshot(_ context.Context,
	imageID string, key backend.SnapshotKey, metadata backend.SnapshotMetadata,
) error {
	var metaString string
	if metadata != nil {
		metaString = metadata.Encode()
	}

	cmd := reexec.Command(buildAhPrepareSnapshot, imageID, string(key), metaString)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		klog.Errorf("unable to start prepare snapshot %q: %s", key, err)
		return err
	}
	err = cmd.Wait()
	if err != nil {
		klog.Errorf("unable to prepare snapshot %q: %s", key, err)
		return err
	}

	return nil
}

func (s snapshotMounter) UpdateSnapshotMetadata(
	_ context.Context, key backend.SnapshotKey, metadata backend.SnapshotMetadata,
) error {
	metaString := metadata.Encode()

	builder, err := buildah.OpenBuilder(s.store, string(key))
	if err != nil {
		klog.Errorf("unable to open builder for snapshot %q to update metadata: %s", key, err)
		return err
	}

	builder.SetLabel(labelLabel, metaString)

	err = builder.Save()

	if err != nil {
		klog.Errorf("unable to update metadata of snapshot %q: %s", key, err)
		return err
	}

	klog.Infof("updated metadata of snapshot %q to %#v(compressed length %d)", key, metadata, len(metaString))

	return nil
}

func (s snapshotMounter) DestroySnapshot(ctx context.Context, key backend.SnapshotKey) error {
	builder, err := buildah.OpenBuilder(s.store, string(key))
	if err != nil {
		if err.Error() == "container not known" {
			klog.Infof("snapshot %q not found", key)
			return nil
		}
		klog.Errorf("unable to open builder for snapshot %q: %s", key, err)
		return err
	}

	err = builder.Unmount()
	if err != nil {
		klog.Errorf("unable to unmount snapshot %q: %s", key, err)
		return err
	}

	err = builder.Delete()
	if err != nil {
		klog.Errorf("unable to destroy snapshot %q: %s", key, err)
		return err
	}

	return nil
}

func (s snapshotMounter) ListSnapshots(ctx context.Context) ([]backend.SnapshotMetadata, error) {
	var ss []backend.SnapshotMetadata
	builders, err := buildah.OpenAllBuilders(s.store)
	if err != nil {
		klog.Errorf("unable to list snapshots: %s", err)
		return nil, err
	}

	klog.Infof("list %d builders", len(builders))

	for _, builder := range builders {
		if !strings.HasPrefix(builder.Container, "csi-") {
			klog.Infof("skip snapshot %q", builder.Container)
			continue
		}

		metaString, ok := builder.Labels()[labelLabel]
		metadata := make(backend.SnapshotMetadata)
		metadata.SetSnapshotKey(builder.Container)
		if metaString != "" && ok {
			if err := metadata.Decode(metaString); err == nil {
				ss = append(ss, metadata)
				klog.Infof("got ro snapshot %q with targets %#v", builder.Container, metadata.GetTargets())
			} else {
				klog.Warningf("unable to decode the metadata of snapshot %q: %s. it may be not a snapshot",
					builder.Container, err)
			}
		} else {
			klog.Infof("snapshot %q has no metadata %#v", builder.Container, builder.Labels())
		}
	}

	klog.Infof("list %d snapshots", len(ss))

	return ss, nil
}

const (
	labelPrefix = "csi-image.warm-metal.tech"
	labelLabel  = labelPrefix + "/label"
)
