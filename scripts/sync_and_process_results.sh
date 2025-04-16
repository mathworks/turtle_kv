#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)

cd "${script_dir}/.."

RBUILD_SYNC_DIRS=build/benchmark_results rbuild 'pwd'

scripts/using_venv.sh python scripts/process_chi_scalability.py build/benchmark_results/chi*.log
