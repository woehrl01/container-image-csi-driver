package cri

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/remotes/docker"
	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/util"
)

const maxMsgSize = 1024 * 1024 * 16

func NewRemoteImageService(endpoint string, connectionTimeout time.Duration) (cri.ImageServiceClient, error) {
	addr, dialer, err := util.GetAddressAndDialer(endpoint)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx, addr, grpc.WithInsecure(), grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithBlock(),
	)

	if err != nil {
		klog.Errorf("Connect remote image service %s failed: %v", addr, err)
		return nil, err
	}

	return cri.NewImageServiceClient(conn), nil
}

func NewRemoteImageServiceContainerd(endpoint string, connectionTimeout time.Duration) (cri.ImageServiceClient, error) {
	c, err := containerd.New(endpoint, containerd.WithDefaultNamespace("warm-metal.tech"))
	if err != nil {
		return nil, err
	}
	return &remoteImageServiceContainerd{c}, nil
}

type remoteImageServiceContainerd struct {
	client *containerd.Client
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

	resolver := docker.NewResolver(docker.ResolverOptions{
		Authorizer: docker.NewDockerAuthorizer(
			docker.WithAuthCreds(func(host string) (string, string, error) {
				return in.Auth.Username, in.Auth.Password, nil
			}),
		),
	})

	o, err := r.client.Pull(ctx, in.Image.Image, containerd.WithResolver(resolver))
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
