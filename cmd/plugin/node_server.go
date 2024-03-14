package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/containerd/containerd/reference/docker"
	"github.com/warm-metal/container-image-csi-driver/pkg/backend"
	"github.com/warm-metal/container-image-csi-driver/pkg/metrics"
	"github.com/warm-metal/container-image-csi-driver/pkg/remoteimage"
	"github.com/warm-metal/container-image-csi-driver/pkg/remoteimageasync"
	"github.com/warm-metal/container-image-csi-driver/pkg/secret"
	"github.com/warm-metal/container-image-csi-driver/pkg/utils"
	csicommon "github.com/warm-metal/csi-drivers/pkg/csi-common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	k8smount "k8s.io/mount-utils"
)

const (
	ctxKeyVolumeHandle    = "volumeHandle"
	ctxKeyImage           = "image"
	ctxKeyPullAlways      = "pullAlways"
	ctxKeyEphemeralVolume = "csi.storage.k8s.io/ephemeral"
)

type ImagePullStatus int

type Options struct {
	AsyncImagePullTimeout time.Duration
	AsyncCannelSize       int
}

func NewNodeServer(driver *csicommon.CSIDriver, mounter backend.Mounter, imageSvc cri.ImageServiceClient, secretStore secret.Store, o *Options) *NodeServer {
	ns := NodeServer{
		DefaultNodeServer:     csicommon.NewDefaultNodeServer(driver),
		mounter:               mounter,
		imageSvc:              imageSvc,
		secretStore:           secretStore,
		asyncImagePullTimeout: o.AsyncImagePullTimeout,
		asyncImagePuller:      nil,
		k8smounter:            k8smount.New(""),
		volumeLocks:           utils.NewNamedLocks(),
		imageLocks:            utils.NewNamedLocks(),
	}
	if o.AsyncImagePullTimeout >= time.Duration(30*time.Second) {
		klog.Infof("Starting node server in Async mode with %v timeout", asyncImagePullTimeout)
		ns.asyncImagePuller = remoteimageasync.StartAsyncPuller(context.TODO(), o.AsyncCannelSize)
	} else {
		klog.Info("Starting node server in Sync mode")
		ns.asyncImagePullTimeout = 0 // set to default value
	}
	return &ns
}

type NodeServer struct {
	*csicommon.DefaultNodeServer
	mounter               backend.Mounter
	imageSvc              cri.ImageServiceClient
	secretStore           secret.Store
	asyncImagePullTimeout time.Duration
	asyncImagePuller      remoteimageasync.AsyncPuller
	k8smounter            k8smount.Interface
	volumeLocks           *utils.NamedLocks
	imageLocks            *utils.NamedLocks
}

func (n NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (resp *csi.NodePublishVolumeResponse, err error) {
	valuesLogger := klog.LoggerWithValues(klog.NewKlogr(), "pod-name", req.VolumeContext["pod-name"], "namespace", req.VolumeContext["namespace"], "uid", req.VolumeContext["uid"])
	valuesLogger.Info("Incoming NodePublishVolume request", "request string", req.String())
	if len(req.VolumeId) == 0 {
		err = status.Error(codes.InvalidArgument, "VolumeId is missing")
		return
	}

	if len(req.TargetPath) == 0 {
		err = status.Error(codes.InvalidArgument, "TargetPath is missing")
		return
	}

	if req.VolumeCapability == nil {
		err = status.Error(codes.InvalidArgument, "VolumeCapability is missing")
		return
	}

	if _, isBlock := req.VolumeCapability.AccessType.(*csi.VolumeCapability_Block); isBlock {
		err = status.Error(codes.InvalidArgument, "unable to mount as a block device")
		return
	}

	if len(req.VolumeContext) == 0 {
		err = status.Error(codes.InvalidArgument, "VolumeContext is missing")
		return
	}

	if req.VolumeContext[ctxKeyEphemeralVolume] != "true" &&
		req.VolumeCapability.AccessMode.Mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY &&
		req.VolumeCapability.AccessMode.Mode != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		err = status.Error(codes.InvalidArgument, "AccessMode of PV can be only ReadOnlyMany or ReadOnlyOnce")
		return
	}

	if acquired := n.volumeLocks.TryAcquire(req.VolumeId); !acquired {
		return nil, status.Errorf(codes.Aborted, utils.NamedOperationAlreadyExistsFmt, req.VolumeId)
	}
	defer n.volumeLocks.Release(req.VolumeId)

	notMnt, err := n.k8smounter.IsLikelyNotMountPoint(req.TargetPath)
	if err != nil {
		if !os.IsNotExist(err) {
			err = status.Error(codes.Internal, err.Error())
			return
		}

		if err = os.MkdirAll(req.TargetPath, 0o755); err != nil {
			err = status.Error(codes.Internal, err.Error())
			return
		}

		notMnt = true
	}

	if !notMnt {
		klog.Infof("NodePublishVolume succeeded on volume %v to %s, mount already exists.", req.VolumeId, req.TargetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// For PVs, VolumeId is the image. For ephemeral volumes, it is a string.
	image := req.VolumeId

	if len(req.VolumeContext[ctxKeyVolumeHandle]) > 0 {
		image = req.VolumeContext[ctxKeyVolumeHandle]
	} else if len(req.VolumeContext[ctxKeyImage]) > 0 {
		image = req.VolumeContext[ctxKeyImage]
	}

	// because downloading an preparing an image can take a long time, we need to lock
	// this is only required for ephemeral volumes, as PVs would already be locked on the volumeid
	// we are not only locking on the image id, because we have to lock the unmount operation as well
	// and we don't have the image id there
	if acquired := n.imageLocks.TryAcquire(image); !acquired {
		return nil, status.Errorf(codes.Aborted, utils.NamedOperationAlreadyExistsFmt, image)
	}
	defer n.imageLocks.Release(image)

	pullAlways := strings.ToLower(req.VolumeContext[ctxKeyPullAlways]) == "true"

	keyring, err := n.secretStore.GetDockerKeyring(ctx, req.Secrets)
	if err != nil {
		err = status.Errorf(codes.Aborted, "unable to fetch keyring: %s", err)
		return
	}

	namedRef, err := docker.ParseDockerRef(image)
	if err != nil {
		klog.Errorf("unable to normalize image %q: %s", image, err)
		return
	}

	//NOTE: we are relying on n.mounter.ImageExists() to return false when
	//      a first-time pull is in progress, else this logic may not be
	//      correct. should test this.
	if pullAlways || !n.mounter.ImageExists(ctx, namedRef) {
		klog.Errorf("pull image %q", image)
		puller := remoteimage.NewPuller(n.imageSvc, namedRef, keyring)

		if n.asyncImagePuller != nil {
			var session *remoteimageasync.PullSession
			session, err = n.asyncImagePuller.StartPull(image, puller, n.asyncImagePullTimeout)
			if err != nil {
				err = status.Errorf(codes.Aborted, "unable to pull image %q: %s", image, err)
				metrics.OperationErrorsCount.WithLabelValues("pull-async-start").Inc()
				return
			}

			waitForPullCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			if err = n.asyncImagePuller.WaitForPull(session, waitForPullCtx); err != nil {
				err = status.Errorf(codes.Aborted, "unable to pull image %q: %s", image, err)
				metrics.OperationErrorsCount.WithLabelValues("pull-async-wait").Inc()
				return
			}
		} else {
			if err = puller.Pull(ctx); err != nil {
				err = status.Errorf(codes.Aborted, "unable to pull image %q: %s", image, err)
				metrics.OperationErrorsCount.WithLabelValues("pull-sync-call").Inc()
				return
			}
		}
	}

	mountStartTime := time.Now()

	ro := req.Readonly ||
		req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY ||
		req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	if err = n.mounter.Mount(ctx, req.VolumeId, backend.MountTarget(req.TargetPath), namedRef, ro); err != nil {
		err = status.Error(codes.Internal, err.Error())
		metrics.OperationErrorsCount.WithLabelValues("mount").Inc()
		return
	}
	mountDuration := time.Since(mountStartTime)
	valuesLogger.Info("Successfully completed NodePublishVolume request", "request string", req.String(), "mountDuration", mountDuration.String())

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (resp *csi.NodeUnpublishVolumeResponse, err error) {
	valuesLogger := klog.LoggerWithValues(klog.NewKlogr())
	valuesLogger.Info("Incoming NodeUnpublishVolume request", "request string", req.String())

	if len(req.VolumeId) == 0 {
		err = status.Error(codes.InvalidArgument, "VolumeId is missing")
		return
	}

	if len(req.TargetPath) == 0 {
		err = status.Error(codes.InvalidArgument, "TargetPath is missing")
		return
	}

	if acquired := n.volumeLocks.TryAcquire(req.VolumeId); !acquired {
		return nil, status.Errorf(codes.Aborted, utils.NamedOperationAlreadyExistsFmt, req.VolumeId)
	}
	defer n.volumeLocks.Release(req.VolumeId)

	unmountStartTime := time.Now()

	mnt, err := n.k8smounter.IsMountPoint(req.TargetPath)
	force := false
	if !mnt || os.IsNotExist(err) {
		force = true
		klog.Warningf("mount cleanup skipped: %s is not a mount point", req.TargetPath)
	} else if err != nil {
		return &csi.NodeUnpublishVolumeResponse{}, err
	}

	if err = n.mounter.Unmount(ctx, req.VolumeId, backend.MountTarget(req.TargetPath), force); err != nil {
		metrics.OperationErrorsCount.WithLabelValues("unmount").Inc()
		return &csi.NodeUnpublishVolumeResponse{}, status.Error(codes.Internal, err.Error())
	}

	unmountDuration := time.Since(unmountStartTime)

	valuesLogger.Info("Successfully completed NodeUnpublishVolume request", "request string", req.String(), "unmountDuration", unmountDuration.String())

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n NodeServer) NodeStageVolume(ctx context.Context, _ *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (n NodeServer) NodeUnstageVolume(ctx context.Context, _ *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (n NodeServer) NodeExpandVolume(ctx context.Context, _ *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
