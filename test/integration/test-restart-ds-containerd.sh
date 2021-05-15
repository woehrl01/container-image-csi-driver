#!/usr/bin/env bash

set -e
source $(dirname "${BASH_SOURCE[0]}")/../../hack/lib/cluster.sh
lib::start_cluster_containerd ${K8S_VERSION}
minikube cache reload -p csi-image-test
lib::install_driver_for_containerd

source $(dirname "${BASH_SOURCE[0]}")/restart-ds.sh

lib::uninstall_driver_for_containerd
echo "Destroying cluster"
minikube delete -p csi-image-test
set +e