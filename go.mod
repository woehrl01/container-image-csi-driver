module github.com/warm-metal/container-image-csi-driver

go 1.23.3

require (
	github.com/BurntSushi/toml v1.3.2
	github.com/container-storage-interface/spec v1.6.0
	github.com/containerd/containerd v1.7.23
	github.com/containers/storage v1.53.0
	github.com/golang/protobuf v1.5.4
	github.com/mitchellh/go-ps v1.0.0
	github.com/opencontainers/image-spec v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.20.2
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	github.com/warm-metal/csi-drivers v0.5.0-alpha.0.0.20210404173852-9ec9cb097dd2
	golang.org/x/net v0.31.0
	golang.org/x/sync v0.9.0
	google.golang.org/grpc v1.68.0
	k8s.io/api v0.26.2
	k8s.io/apimachinery v0.26.2
	k8s.io/client-go v0.26.2
	k8s.io/cri-api v0.27.1
	k8s.io/klog/v2 v2.90.1
	k8s.io/kubernetes v1.25.16
	k8s.io/mount-utils v0.25.16
	k8s.io/utils v0.0.0-20230220204549-a5ecb0141aa5
	sigs.k8s.io/yaml v1.4.0
)

require (
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20240806141605-e8a1dd7889d6 // indirect
	github.com/AdamKorcz/go-118-fuzz-build v0.0.0-20231105174938-2b5cbb29f3e2 // indirect
	github.com/Azure/azure-sdk-for-go v55.0.0+incompatible // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.27 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.20 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.1.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Microsoft/hcsshim v0.12.9 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/aws/aws-sdk-go v1.38.49 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/cgroups/v3 v3.0.3 // indirect
	github.com/containerd/containerd/api v1.8.0 // indirect
	github.com/containerd/continuity v0.4.5 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/fifo v1.1.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/containerd/ttrpc v1.2.6 // indirect
	github.com/containerd/typeurl/v2 v2.2.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/go-events v0.0.0-20241114183025-9940093291f5 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/emicklei/go-restful/v3 v3.10.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.5 // indirect
	github.com/go-openapi/swag v0.19.14 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/gnostic v0.5.7-v3refs // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/go-intervals v0.0.2 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/kubernetes-csi/csi-lib-utils v0.9.1 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/mattn/go-shellwords v1.0.12 // indirect
	github.com/mistifyio/go-zfs/v3 v3.0.1 // indirect
	github.com/moby/locker v1.0.1 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/signal v0.7.1 // indirect
	github.com/moby/sys/user v0.3.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/runtime-spec v1.2.0 // indirect
	github.com/opencontainers/selinux v1.11.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cobra v1.4.0 // indirect
	github.com/syndtr/gocapability v0.0.0-20200815063812-42c35b437635 // indirect
	github.com/tchap/go-patricia/v2 v2.3.1 // indirect
	github.com/ulikunitz/xz v0.5.11 // indirect
	github.com/vbatts/tar-split v0.11.5 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.57.0 // indirect
	go.opentelemetry.io/otel v1.32.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/trace v1.32.0 // indirect
	golang.org/x/crypto v0.29.0 // indirect
	golang.org/x/oauth2 v0.23.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
	golang.org/x/term v0.26.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8 // indirect
	google.golang.org/genproto v0.0.0-20241113202542-65e8d215514f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241113202542-65e8d215514f // indirect
	google.golang.org/protobuf v1.35.2 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiserver v0.26.2 // indirect
	k8s.io/cloud-provider v0.25.16 // indirect
	k8s.io/component-base v0.26.2 // indirect
	k8s.io/kube-openapi v0.0.0-20220803162953-67bda5d908f1 // indirect
	k8s.io/kubelet v0.0.0 // indirect
	k8s.io/legacy-cloud-providers v0.0.0 // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.25.16
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.25.16
	k8s.io/apimachinery => k8s.io/apimachinery v0.25.16
	k8s.io/apiserver => k8s.io/apiserver v0.25.16
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.25.16
	k8s.io/client-go => k8s.io/client-go v0.25.16
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.25.16
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.25.16
	k8s.io/code-generator => k8s.io/code-generator v0.25.16
	k8s.io/component-base => k8s.io/component-base v0.25.16
	k8s.io/component-helpers => k8s.io/component-helpers v0.25.16
	k8s.io/controller-manager => k8s.io/controller-manager v0.25.16
	k8s.io/cri-api => k8s.io/cri-api v0.25.16
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.25.16
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.25.16
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.25.16
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.25.16
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.25.16
	k8s.io/kubectl => k8s.io/kubectl v0.25.16
	k8s.io/kubelet => k8s.io/kubelet v0.25.16
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.25.16
	k8s.io/metrics => k8s.io/metrics v0.25.16
	k8s.io/mount-utils => k8s.io/mount-utils v0.25.16
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.25.16
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.25.16
)
