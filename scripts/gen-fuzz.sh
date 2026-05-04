#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)

PYTHONPATH=${script_dir} "${script_dir}/using_venv.sh" python -m gen_fuzz_test_scripts "$@"
