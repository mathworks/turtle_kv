#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)

source "${script_dir}/../test.env"

storage_dir=${STORAGE_DIR:-${STORAGE_DEVICE:-/mnt/kv-bakeoff}/ycsb-c}
nvme_dev=$(findmnt --target "${storage_dir}" | grep nvme | awk '{print $2}')
runner_tool=

tmp_dir=$(mktemp -d)
trap "test ${tmp_dir} && test -d ${tmp_dir} && rm -rf ${tmp_dir}" EXIT

if [ "${PERF_STAT:-1}" != "0" ]; then
    runner_tool="${runner_tool} perf stat -o ${tmp_dir}/perf_out --no-big-num -e cache-references,cache-misses,cycles,instructions,branches,minor-faults,major-faults,page-faults,migrations"
fi

if [ "${GDB:-0}" != "0" ]; then
    runner_tool="${runner_tool} gdb"
fi

#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#
iostat_before=$(iostat ${nvme_dev})
echo "start_time_seconds $(date +%s.%N)"
#
${runner_tool} "$@"
#
echo "finish_time_seconds $(date +%s.%N)"
iostat_after=$(iostat ${nvme_dev})
#
#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

kb_rd_before=$(echo "${iostat_before}" | grep -E "^nvme" | awk '{print $(NF-2)}')
kb_rd_after=$(echo "${iostat_after}" | grep -E "^nvme" | awk '{print $(NF-2)}')

kb_wr_before=$(echo "${iostat_before}" | grep -E "^nvme" | awk '{print $(NF-1)}')
kb_wr_after=$(echo "${iostat_after}" | grep -E "^nvme" | awk '{print $(NF-1)}')

device_bytes_read=$(expr \( ${kb_rd_after} \- ${kb_rd_before} \) \* 1024)
device_bytes_written=$(expr \( ${kb_wr_after} \- ${kb_wr_before} \) \* 1024)

echo "device_bytes_read ${device_bytes_read}"
echo "device_bytes_written ${device_bytes_written}"

if [ "${N:-}" != "" ]; then
    user_input_bytes=$(pyexpr "130 * ${N}")
    write_amp=$(pyexpr "${device_bytes_written} / ${user_input_bytes}")
    echo "write_amplification ${write_amp}"
fi

perf_output=$(cat ${tmp_dir}/perf_out \
                  | sed -e 's,seconds time elapsed,time_elapsed_seconds,g' \
                  | sed -e 's,seconds user,time_user_seconds,g' \
                  | sed -e 's,seconds sys,time_sys_seconds,g' \
                  | grep -E '^[[:blank:]]+[0-9\.]+' \
                  | awk '{print "perf_stat." $2 " " $1}')

elapsed_s=$(echo "${perf_output}" | grep -E "^perf_stat.time_elapsed_seconds " | awk '{print $2}')
user_s=$(echo "${perf_output}" | grep -E "^perf_stat.time_user_seconds " | awk '{print $2}')
sys_s=$(echo "${perf_output}" | grep -E "^perf_stat.time_sys_seconds " | awk '{print $2}')

cpu_util=$(pyexpr "( ${user_s} + ${sys_s} ) * 100 / ${elapsed_s}")

echo "${perf_output}"
echo "cpu_utilization ${cpu_util}" 
