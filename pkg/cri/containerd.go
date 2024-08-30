package cri

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/pkg/kmutex"
	"github.com/containerd/containerd/remotes/docker"
	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func NewRemoteImageServiceContainerd(endpoint string, connectionTimeout time.Duration) (cri.ImageServiceClient, error) {
	c, err := containerd.New(endpoint, containerd.WithDefaultNamespace("k8s.io"))
	if err != nil {
		return nil, err
	}
	return &remoteImageServiceContainerd{
		client:       c,
		unpackLocker: kmutex.New(),
	}, nil
}

type remoteImageServiceContainerd struct {
	client       *containerd.Client
	unpackLocker kmutex.KeyedLocker
}

func (r *remoteImageServiceContainerd) ListImages(ctx context.Context, in *cri.ListImagesRequest, opts ...grpc.CallOption) (*cri.ListImagesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *remoteImageServiceContainerd) ImageStatus(ctx context.Context, in *cri.ImageStatusRequest, opts ...grpc.CallOption) (*cri.ImageStatusResponse, error) {
	o, err := r.client.GetImage(ctx, in.Image.Image)
	if err != nil {
		return nil, err
	}

	size, err := o.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &cri.ImageStatusResponse{
		Image: &cri.Image{
			Id:    o.Name(),
			Size_: uint64(size),
			Spec: &cri.ImageSpec{
				Image: o.Name(),
			},
		},
	}, nil

}

func (r *remoteImageServiceContainerd) PullImage(ctx context.Context, in *cri.PullImageRequest, opts ...grpc.CallOption) (*cri.PullImageResponse, error) {
	maxConcurrency := 1

	resolver := docker.NewResolver(docker.ResolverOptions{
		Authorizer: docker.NewDockerAuthorizer(
			docker.WithAuthCreds(func(host string) (string, string, error) {
				if in.Auth == nil {
					return "", "", nil
				}
				return in.Auth.Username, in.Auth.Password, nil
			}),
		),
	})

	o, err := r.client.Pull(ctx, in.Image.Image,
		containerd.WithResolver(resolver),
		containerd.WithMaxConcurrentDownloads(maxConcurrency),
		containerd.WithPullUnpack,
		containerd.WithUnpackOpts([]containerd.UnpackOpt{containerd.WithUnpackDuplicationSuppressor(r.unpackLocker)}),
	)

	if err != nil {
		return nil, err
	}

	return &cri.PullImageResponse{
		ImageRef: o.Name(),
	}, nil
}

func (r *remoteImageServiceContainerd) RemoveImage(ctx context.Context, in *cri.RemoveImageRequest, opts ...grpc.CallOption) (*cri.RemoveImageResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *remoteImageServiceContainerd) ImageFsInfo(ctx context.Context, in *cri.ImageFsInfoRequest, opts ...grpc.CallOption) (*cri.ImageFsInfoResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
