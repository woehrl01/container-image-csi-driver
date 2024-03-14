package cri

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/containers/buildah"
	"github.com/containers/common/libimage"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

type remoteImageServiceBuildah struct {
	maxConcurrency *semaphore.Weighted
	store          storage.Store
}

func NewRemoteImageServiceBuildah(connectionTimeout time.Duration) (cri.ImageServiceClient, error) {
	storageOptions, err := storage.DefaultStoreOptions()
	if err != nil {
		klog.Fatalf("unable to get storage options: %s", err)
	}

	store, err := storage.GetStore(storageOptions)
	if err != nil {
		klog.Fatalf("unable to get storage store: %s", err)
	}

	return &remoteImageServiceBuildah{
		maxConcurrency: semaphore.NewWeighted(1),
		store:          store,
	}, nil
}

func (r *remoteImageServiceBuildah) ListImages(ctx context.Context, in *cri.ListImagesRequest, opts ...grpc.CallOption) (*cri.ListImagesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *remoteImageServiceBuildah) ImageStatus(ctx context.Context, in *cri.ImageStatusRequest, opts ...grpc.CallOption) (*cri.ImageStatusResponse, error) {
	storageOptions, err := storage.DefaultStoreOptions()
	if err != nil {
		return nil, err
	}

	store, err := storage.GetStore(storageOptions)
	if err != nil {
		return nil, err
	}

	systemContext := &types.SystemContext{}
	runtime, err := libimage.RuntimeFromStore(store, &libimage.RuntimeOptions{SystemContext: systemContext})

	if err != nil {
		return nil, err
	}

	image, name, err := runtime.LookupImage(in.Image.Image, &libimage.LookupImageOptions{})
	if err != nil {
		return nil, err
	}

	size, err := image.Size()

	if err != nil {
		return nil, err
	}

	return &cri.ImageStatusResponse{
		Image: &cri.Image{
			Id:    name,
			Size_: uint64(size),
			Spec: &cri.ImageSpec{
				Image: name,
			},
		},
	}, nil
}

func (r *remoteImageServiceBuildah) PullImage(ctx context.Context, in *cri.PullImageRequest, opts ...grpc.CallOption) (*cri.PullImageResponse, error) {
	if err := r.maxConcurrency.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer r.maxConcurrency.Release(1)

	systemContext := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username:      in.Auth.Username,
			Password:      in.Auth.Password,
			IdentityToken: in.Auth.IdentityToken,
		},
	}

	options := buildah.PullOptions{
		Store:         r.store,
		SystemContext: systemContext,
		PullPolicy:    buildah.PullAlways,
		MaxRetries:    0,
	}

	id, err := buildah.Pull(ctx, in.Image.Image, options)
	if err != nil {
		klog.Errorf("unable to pull image %q: %s", flag.Arg(0), err)
		return nil, err
	}

	return &cri.PullImageResponse{
		ImageRef: id,
	}, nil
}

func (r *remoteImageServiceBuildah) RemoveImage(ctx context.Context, in *cri.RemoveImageRequest, opts ...grpc.CallOption) (*cri.RemoveImageResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *remoteImageServiceBuildah) ImageFsInfo(ctx context.Context, in *cri.ImageFsInfoRequest, opts ...grpc.CallOption) (*cri.ImageFsInfoResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
