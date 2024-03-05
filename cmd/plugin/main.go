package main

import (
	"context"
	goflag "flag"
	"fmt"
	"net/url"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	flag "github.com/spf13/pflag"
	"github.com/warm-metal/container-image-csi-driver/pkg/backend"
	"github.com/warm-metal/container-image-csi-driver/pkg/backend/containerd"
	"github.com/warm-metal/container-image-csi-driver/pkg/backend/crio"
	"github.com/warm-metal/container-image-csi-driver/pkg/cri"
	"github.com/warm-metal/container-image-csi-driver/pkg/metrics"
	"github.com/warm-metal/container-image-csi-driver/pkg/secret"
	"github.com/warm-metal/container-image-csi-driver/pkg/watcher"
	csicommon "github.com/warm-metal/csi-drivers/pkg/csi-common"
	kcri "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

const (
	driverName    = "csi-image.warm-metal.tech"
	driverVersion = "v1.0.0"

	containerdScheme = "containerd"
	criOScheme       = "cri-o"

	nodeMode       = "node"
	controllerMode = "controller"
)

var (
	endpoint = flag.String("endpoint", "unix:///csi/csi.sock",
		"The endpoint of the CSI driver usually shared with the driver registrar.")
	nodeID         = flag.String("node", "", "The node name that driver currently runs on.")
	containerdSock = flag.String(
		"containerd-addr", "",
		"The unix socket of containerd. Deprecated. Use --runtime-addr instead.")
	runtimeAddr = flag.String(
		"runtime-addr", "",
		fmt.Sprintf("The unix socket of the container runtime. Currently both containerd and cri-o are supported."+
			"Users need to replace the leading %q with %q or %q to indicate the working runtime.",
			"unix", containerdScheme, criOScheme),
	)
	icpConf = flag.String("image-credential-provider-config", "",
		"The path to the credential provider plugin config file.")
	icpBin = flag.String("image-credential-provider-bin-dir", "",
		"The path to the directory where credential provider plugin binaries are located.")
	enableCache = flag.Bool("enable-daemon-image-credential-cache", true,
		"Whether to save contents of imagepullsecrets of the daemon ServiceAccount in memory. "+
			"If set to false, secrets will be fetched from the API server on every image pull.")
	asyncImagePullTimeout = flag.Duration("async-pull-timeout", 0,
		"If positive, specifies duration allotted for async image pulls as measured from pull start time. If zero, negative, less than 30s, or omitted, the caller's timeout (usually kubelet: 2m) is used instead of this value. (additional time helps prevent timeout for larger images or slower image pull conditions)")
	watcherResyncPeriod      = flag.Duration("watcher-resync-period", 30*time.Minute, "The resync period of the pvc watcher.")
	mode                     = flag.String("mode", "", "The mode of the driver. Valid values are: node, controller")
	nodePluginSA             = flag.String("node-plugin-sa", "csi-image-warm-metal", "The name of the ServiceAccount used by the node plugin.")
	containerDMountRate      = flag.Int("containerd-mount-rate", 5, "The rate limit of containerd mount operations per second.")
	containerDUmountRate     = flag.Int("containerd-umount-rate", 10, "The rate limit of containerd umount operations per second.")
	containerDMountBurst     = flag.Int("containerd-mount-burst", 5, "The burst of containerd mount operations.")
	containerDUmountBurst    = flag.Int("containerd-umount-burst", 10, "The burst of containerd umount operations.")
	contaienrDStartupTimeout = flag.Duration("containerd-startup-timeout", 20*time.Second, "The timeout for containerd startup.")
	asyncRateLimit           = flag.Int("async-pull-rate-limit", 100, "The rate limit of async image pull operations per second.")
	asyncRateBurst           = flag.Int("async-pull-burst-limit", 100, "The burst of async image pull operations.")
	asyncChannelSize         = flag.Int("async-channel-size", 100, "The size of the async image pull channel.")
)

func main() {
	klog.InitFlags(nil)
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	if err := flag.Set("logtostderr", "true"); err != nil {
		panic(err)
	}

	flag.Parse()
	defer klog.Flush()

	driver := csicommon.NewCSIDriver(driverName, driverVersion, *nodeID)
	driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
	})
	driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	})

	if len(*mode) == 0 {
		klog.Fatalf("The mode of the driver is required.")
	}

	server := csicommon.NewNonBlockingGRPCServer()

	switch *mode {
	case nodeMode:
		if len(*runtimeAddr) == 0 {
			if len(*containerdSock) == 0 {
				klog.Fatalf("The unit socket of container runtime is required.")
			}

			klog.Warning("--containerd-addr is deprecated. Use --runtime-addr instead.")
			addr, err := url.Parse(*containerdSock)
			if err != nil {
				klog.Fatalf("invalid runtime address: %s", err)
			}
			addr.Scheme = containerdScheme
			*runtimeAddr = addr.String()
		}

		var mounter backend.Mounter
		var criClient kcri.ImageServiceClient
		if len(*runtimeAddr) > 0 {
			addr, err := url.Parse(*runtimeAddr)
			if err != nil {
				klog.Fatalf("invalid runtime address: %s", err)
			}

			klog.Infof("runtime %s at %q", addr.Scheme, addr.Path)
			switch addr.Scheme {
			case containerdScheme:
				mounter = containerd.NewMounter(&containerd.Options{
					SocketPath:     addr.Path,
					MountRate:      *containerDMountRate,
					MountBurst:     *containerDMountBurst,
					UmountRate:     *containerDUmountRate,
					UmountBurst:    *containerDUmountBurst,
					StartupTimeout: *contaienrDStartupTimeout,
				})

				addr.Scheme = "unix"
				*runtimeAddr = addr.String()
				criClient, err = cri.NewRemoteImageServiceContainerd(addr.Path, time.Second)
				if err != nil {
					klog.Fatalf(`unable to connect to cri daemon "%s": %s`, *endpoint, err)
				}

			case criOScheme:
				mounter = crio.NewMounter(&crio.Options{
					SocketPath: addr.Path,
				})
				addr.Scheme = "unix"
				*runtimeAddr = addr.String()
				criClient, err = cri.NewRemoteImageService(*runtimeAddr, time.Second)
				if err != nil {
					klog.Fatalf(`unable to connect to cri daemon "%s": %s`, *endpoint, err)
				}
			default:
				klog.Fatalf("unknown container runtime %q", addr.Scheme)
			}
		}

		secretStore := secret.CreateStoreOrDie(*icpConf, *icpBin, *nodePluginSA, *enableCache)

		server.Start(*endpoint,
			NewIdentityServer(driverVersion),
			nil,
			NewNodeServer(driver, mounter, criClient, secretStore, &Options{
				AsyncImagePullTimeout: *asyncImagePullTimeout,
				AsyncCannelSize:       *asyncChannelSize,
				AsyncRateLimit:        *asyncRateLimit,
				AsyncRateBurst:        *asyncRateBurst,
			}))
	case controllerMode:
		watcher, err := watcher.New(context.Background(), *watcherResyncPeriod)
		if err != nil {
			klog.Fatalf("unable to create PVC watcher: %s", err)
		}

		defer watcher.Stop()

		server.Start(*endpoint,
			NewIdentityServer(driverVersion),
			NewControllerServer(driver, watcher),
			nil,
		)
	}

	metrics.StartMetricsServer(metrics.RegisterMetrics())
	server.Wait()
}
