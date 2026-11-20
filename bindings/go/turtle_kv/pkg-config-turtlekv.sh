#!/bin/bash
#
set -Eeuo pipefail

flags_arg=$1
self_file=$(realpath "$0")
self_dir=$(realpath "$(dirname "$0")")
project_dir=$(realpath "${self_dir}/../../..")
build_dir=${self_dir}/build
install_dir=${build_dir}/install
obj_dir=${build_dir}/obj
deploy_dir=${build_dir}/deploy
ts_file=${build_dir}/_configured.ts
log_dir=${build_dir}
out_log=${log_dir}/out.log
err_log=${log_dir}/err.log
libturtlekv_bundle_name=turtlekv_bundle
use_bundle=${BUNDLE:-1}

# Create the log files.
#
mkdir -p "${log_dir}"

touch "${out_log}"
touch "${err_log}"

echo "${self_file} $@" >>"${out_log}"

if [[ "${2:-}" != "--" ]] || [[ "${3:-}" != "turtlekv" ]]; then
    echo "  arg2=${2:-}" >>"${out_log}"
    echo "  arg3=${3:-}" >>"${out_log}"
    pkg-config "$@"
    exit $?
fi

function print_flags() {
    if [ "${use_bundle}" == "0" ]; then
	if [ "${flags_arg}" == "--cflags" ]; then
            make BUILD_DIR="${build_dir}" -f "${self_dir}/config.mk" cflags
	elif [ "${flags_arg}" == "--libs" ]; then
            make BUILD_DIR="${build_dir}" -f "${self_dir}/config.mk" libs
	else
            echo ""
	fi
    else
	if [ "${flags_arg}" == "--cflags" ]; then
            echo "-march=native -I${install_dir}/include"
	elif [ "${flags_arg}" == "--libs" ]; then
            echo "-L${install_dir}/lib -l${libturtlekv_bundle_name} -lm -lstdc++"
	else
            echo ""
	fi
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

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Create libturtlekv_bundle.a
#
if [ "${use_bundle}" == "1" ]; then

    # Clean out the old install dir structure.
    #
    function rm_if_exists() {
	local file_path=$1

	if [ -e "${file_path}" ]; then
	    rm -rf "${file_path}"
	fi	
    }
    rm_if_exists "${obj_dir}"
    rm_if_exists "${install_dir}"
    
    # Copy the contents of all bin/include/lib directories to install.
    #
    function deploy_to_install() {
	local subdir_name=$1

	mkdir -p "${install_dir}/${subdir_name}"
	
	find "${deploy_dir}/full_deploy/host" -type d -name "${subdir_name}" -prune \
	    | xargs -I {} find "{}" -mindepth 1 -maxdepth 1 \
	    | xargs -I {} cp -r "{}" "${install_dir}/${subdir_name}"
    }
    
    #deploy_to_install bin
    #deploy_to_install include
    #deploy_to_install lib

    # Find all deployed libraries and extract their object files to scoped subdirs.
    #
    function find_all_libs() {
	find "${deploy_dir}" -type f -name '*.a' \
	     ! -name '*${libturtlekv_bundle_name}.*' \
             ! -name 'liburing-ffi.a' \
             ! -name 'libunwind*' \
             -print0
    }
    
    function unpack_archive() {
       local filename=$(realpath "$1")
       local base=$(basename "$filename")
       local obj_subdir=${obj_dir}/${base}

       mkdir -p "${obj_subdir}"
       ( cd "${obj_subdir}" && ar x "$filename" )
    }

    find_all_libs | while read -r -d '' file; do
	unpack_archive "$file"
    done

    # Re-archive the object files to create the combined bundle archive.
    #
    mkdir -p "${install_dir}/lib"
    find "${obj_dir}" -type f | xargs ar rcs "${install_dir}/lib/lib${libturtlekv_bundle_name}.a"

    # Copy turtlekv.h to install/include.
    #
    mkdir -p "${install_dir}/include"
    find "${deploy_dir}/full_deploy/host/turtle_kv" -name 'turtlekv.h' \
	 | xargs -I{} cp "{}" "${install_dir}/include"

fi

# Success! Update the timestamp to avoid future build churn.
#
touch "${ts_file}"

print_flags
