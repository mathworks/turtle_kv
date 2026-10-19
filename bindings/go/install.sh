#!/bin/bash
#
set -Eeuo pipefail

self_dir=$(realpath "$(dirname "$0")")
project_dir=$(realpath "${self_dir}/../..")
install_dir=${self_dir}/install
deploy_dir=${self_dir}/deploy

cd "${project_dir}"
pwd

package_version=$(cor pkg-version 2>/dev/null)
package_ref=turtle_kv/${package_version}

mkdir -p "${install_dir}"

cor export

if [ -d "${deploy_dir}" ]; then
    rm -rf "${deploy_dir}"
fi

cor conan install --requires=${package_ref} \
    --lockfile="${project_dir}/conan.lock" --lockfile-partial \
    --build=missing \
    --settings:host build_type=Release \
    --output-folder="${deploy_dir}" \
    --deployer=full_deploy

mkdir -p "${install_dir}/bin"
mkdir -p "${install_dir}/include"
mkdir -p "${install_dir}/lib"

find "${self_dir}/deploy/full_deploy/host" -type d -name bin -prune \
     | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
     | xargs -I {} cp -r "{}" "${install_dir}/bin"

find "${self_dir}/deploy/full_deploy/host" -type d -name include -prune \
     | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
     | xargs -I {} cp -r "{}" "${install_dir}/include"

find "${self_dir}/deploy/full_deploy/host" -type d -name lib -prune \
     | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
     | xargs -I {} cp -r "{}" "${install_dir}/lib"

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Create libturtlekv_bundle.a

# Make the temporary obj dir and cd into it.
#
if [ -d "${install_dir}/obj" ]; then
    rm -rf "${install_dir}/obj"
fi
mkdir -p "${install_dir}/obj"

function unpack_archive() {
    local filename=$(realpath "$1")
    local base=$(basename "$filename")
    local obj_subdir=${install_dir}/obj/${base}

    mkdir -p "${obj_subdir}"
    ( cd "${obj_subdir}" && ar x "$filename" )
}

function find_all_libs() {
    find "${install_dir}/lib" -type f -name '*.a' \
	 ! -name '*turtlekv_bundle.*' \
	 ! -name 'liburing-ffi.a' \
	 ! -name 'libunwind*' \
	 -print0
}

# Find all
#
find_all_libs | while read -r -d '' file; do
    unpack_archive "$file"
done

find "${install_dir}/obj" -type f | xargs ar rcs "${install_dir}/lib/libturtlekv_bundle.a"
#rm -rf "${install_dir}/obj"
