#!/bin/bash
#
set -Eeuo pipefail


flags_arg=$1
self_file=$(realpath "$0")
self_dir=$(realpath "$(dirname "$0")")
project_dir=$(realpath "${self_dir}/../../..")
build_dir=${self_dir}/build
install_dir=${build_dir}/install
obj_dir=${install_dir}/obj
deploy_dir=${build_dir}/deploy
ts_file=${build_dir}/_configured.ts
out_log=${build_dir}/out.log
err_log=${build_dir}/err.log
libturtlekv_bundle_name=turtlekv_bundle

mkdir -p "${install_dir}"
touch "${out_log}"
touch "${err_log}"

echo "$self_file $@" >>"${out_log}"

if [[ "${2:-}" != "--" ]] || [[ "${3:-}" != "turtlekv" ]]; then
    echo "  arg2=${2:-}" >>"${out_log}"
    echo "  arg3=${3:-}" >>"${out_log}"
    pkg-config "$@"
    exit $?
fi

function print_flags() {
    if [ "${flags_arg}" == "--cflags" ]; then
	echo "-march=native -I${install_dir}/include"
    elif [ "${flags_arg}" == "--libs" ]; then
	echo "-L${install_dir}/lib -l${libturtlekv_bundle_name} -lm -lstdc++"
    else
	echo ""
    fi
}

if [ -f "${ts_file}" ] && [ "${ts_file}" -nt "${self_file}" ]; then
    print_flags
    exit 0
fi

cd "${project_dir}"

package_version=$(cor pkg-version 2>/dev/null)
package_ref=turtle_kv/${package_version}


cor export >>"${out_log}" 2>>"${err_log}"

if [ -d "${deploy_dir}" ]; then
    rm -rf "${deploy_dir}"
fi

cor conan install --requires=${package_ref} \
    --lockfile="${project_dir}/conan.lock" --lockfile-partial \
    --build=missing \
    --settings:host build_type=Release \
    --output-folder="${deploy_dir}" \
    --deployer=full_deploy \
    --generator=MakeDeps \
    >>"${out_log}" 2>>"${err_log}"

#    --generator=PkgConfigDeps \


mkdir -p "${install_dir}/bin"
mkdir -p "${install_dir}/include"
mkdir -p "${install_dir}/lib"

find "${deploy_dir}/full_deploy/host" -type d -name bin -prune \
     | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
     | xargs -I {} cp -r "{}" "${install_dir}/bin"

find "${deploy_dir}/full_deploy/host" -type d -name include -prune \
     | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
     | xargs -I {} cp -r "{}" "${install_dir}/include"

find "${deploy_dir}/full_deploy/host" -type d -name lib -prune \
     | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
     | xargs -I {} cp -r "{}" "${install_dir}/lib"

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Create libturtlekv_bundle.a

# Make the temporary obj dir and cd into it.
#
if [ -d "${obj_dir}" ]; then
    rm -rf "${obj_dir}"
fi
mkdir -p "${obj_dir}"

function unpack_archive() {
    local filename=$(realpath "$1")
    local base=$(basename "$filename")
    local obj_subdir=${obj_dir}/${base}

    mkdir -p "${obj_subdir}"
    ( cd "${obj_subdir}" && ar x "$filename" )
}

function find_all_libs() {
    find "${install_dir}/lib" -type f -name '*.a' \
	 ! -name '*${libturtlekv_bundle_name}.*' \
	 ! -name 'liburing-ffi.a' \
	 ! -name 'libunwind*' \
	 -print0
}

# Find all deployed libraries, extract all object files, and then re-package as a single archive.
#
find_all_libs | while read -r -d '' file; do
    unpack_archive "$file"
done

find "${obj_dir}" -type f | xargs ar rcs "${install_dir}/lib/lib${libturtlekv_bundle_name}.a"

# Success! Update the timestamp to avoid future build churn.
#
touch "${ts_file}"

print_flags
