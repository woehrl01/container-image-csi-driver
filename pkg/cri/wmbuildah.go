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
	"github.com/containers/storage/pkg/reexec"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

var (
	buildAhPullImage = "buildah-pull-image"
)

func init() {
	reexec.Register(buildAhPullImage, buildAhPullImageMain)
}
type remoteImageServiceBuildah struct {
	maxConcurrency *semaphore.Weighted
}

func NewRemoteImageServiceBuildah(connectionTimeout time.Duration) (cri.ImageServiceClient, error) {
	return &remoteImageServiceBuildah{
		maxConcurrency: semaphore.NewWeighted(2),
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

	cmd := reexec.Command(buildAhPullImage, in.Image.Image, in.Auth.Username, in.Auth.Password, in.Auth.IdentityToken)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	var id string
	_, err = fmt.Fscan(stdout, &id)
	if err != nil {
		return nil, err
	}

	err = cmd.Wait()
	if err != nil {
		return nil, err
	}
	
	return &cri.PullImageResponse{
		ImageRef: id,
	}, nil

}

func buildAhPullImageMain() {
	flag.Parse()
	if len(flag.Args()) != 4 {
		klog.Fatal("usage: buildah-prepare-snapshot <image> <container> <metadata>")
	}

	storageOptions, err := storage.DefaultStoreOptions()
	if err != nil {
		klog.Fatalf("unable to get storage options: %s", err)
	}

	store, err := storage.GetStore(storageOptions)
	if err != nil {
		klog.Fatalf("unable to get storage store: %s", err)
	}

	systemContext := &types.SystemContext{
		DockerAuthConfig: &types.DockerAuthConfig{
			Username:      flag.Arg(1),
			Password:      flag.Arg(2),
			IdentityToken: flag.Arg(3),
		},
	}

	options := buildah.PullOptions{
		Store:         store,
		SystemContext: systemContext,
		PullPolicy:    buildah.PullAlways,
	}

	id, err := buildah.Pull(context.Background(), flag.Arg(0), options)
	if err != nil {
		klog.Fatalf("unable to pull image %q: %s", flag.Arg(0), err)
	}

	fmt.Printf("%s\n", id)
}


func (r *remoteImageServiceBuildah) RemoveImage(ctx context.Context, in *cri.RemoveImageRequest, opts ...grpc.CallOption) (*cri.RemoveImageResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *remoteImageServiceBuildah) ImageFsInfo(ctx context.Context, in *cri.ImageFsInfoRequest, opts ...grpc.CallOption) (*cri.ImageFsInfoResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
