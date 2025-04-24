#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)
build_dir=$( cd "${script_dir}/../build" && realpath .)

mkdir -p ${build_dir}

if [ ! -f "${build_dir}/venv.ts" ]; then

    rm -rf "${build_dir}/venv"
    
    python3 -m venv "${build_dir}/venv"
    
    source "${build_dir}/venv/bin/activate"

    pip install --upgrade pip
    pip install matplotlib
    pip install numpy

    touch "${build_dir}/venv.ts"
fi

if [ "$*" != "" ]; then
    source "${build_dir}/venv/bin/activate"
    PYTHONPATH=${PYTHONPATH:-}:${script_dir} "$@"
fi
