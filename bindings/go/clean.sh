#!/bin/bash
#
set -Eeuo pipefail

self_dir=$(realpath "$(dirname "$0")")
project_dir=$(realpath "${self_dir}/../..")
install_dir=${self_dir}/install
deploy_dir=${self_dir}/deploy

if [ "${install_dir}" != "" ] && [ -d "${install_dir}" ]; then
    rm -rf "${install_dir}"
fi

if [ "${deploy_dir}" != "" ] && [ -d "${deploy_dir}" ]; then
    rm -rf "${deploy_dir}"
fi
