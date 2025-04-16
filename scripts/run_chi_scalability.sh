#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)

bench_exe=${script_dir}/../build/${BUILD_TYPE:-Release}/bench/turtle_kv_bench
data_file=/mnt/kv-bakeoff/random_data_file.small
result_dir=${script_dir}/../build/benchmark_results

export LOAD_ONLY=1
export N=${N:-$(pyexpr '160*1000*1000')}

if [ "${leaf_size_kb:-}" == "all" ]; then
    leaf_sizes=(512 1024 2048 4096 8192 16384 32768)
else
    leaf_sizes=(${leaf_size_kb:-16384})
fi

if [ "${value_size:-}" == "all" ]; then
    value_sizes=(2 34 98 226 354 482)
else
    value_sizes=(${value_size:-98})
fi

for value_size in "${value_sizes[@]}"; do
  for leaf_size_kb in "${leaf_sizes[@]}"; do

    export turtlekv_node_size_kb=${node_size_kb:-4}
    export turtlekv_leaf_size_kb=${leaf_size_kb}
    export turtlekv_wal_size_mb=${wal_size_mb:-32768}
    export turtlekv_storage_size_gb=${storage_size_gb:-256}
    export turtlekv_filter_bits=${filter_bits:-12}
    export turtlekv_key_size_hint=${key_size:-24}
    export turtlekv_value_size_hint=${value_size}
    export turtlekv_buffer_level_trim=${buffer_level_trim:-0}
    export turtlekv_min_flush_factor=${min_flush_factor:-1}
    export turtlekv_max_flush_factor=${max_flush_factor:-1}
    export turtlekv_checkpoint_pipeline=${checkpoint_pipeline:-1}
    export turtlekv_node_cache_size_mb=${node_cache_size_mb:-1024}
    export turtlekv_leaf_cache_size_mb=${leaf_cache_size_mb:-$(pyexpr '96*1024')}
    export turtlekv_filter_cache_size_mb=${filter_cache_size_mb:-4096}

    log_file=${result_dir}/chi_scaling_$(fts).log

    mkdir -p ${result_dir}
    {
        n_points=${n_points:-24}
        prev_chi=-3
        for i in $(seq 1 ${n_points});
        do
            export turtlekv_checkpoint_distance=$(pyexpr "int(2**((${i}-1)*9/(${n_points}-1)))")
            if [ "${prev_chi}" -ge "${turtlekv_checkpoint_distance}" ]; then
                export turtlekv_checkpoint_distance=$(expr ${prev_chi} \+ 1)
            fi
            prev_chi=${turtlekv_checkpoint_distance}

            MALLOCSTATS=1 ${script_dir}/run_bench.sh ${bench_exe} ${data_file}

            echo
            echo "=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------"
        done

    } 2>&1 | tee ${log_file}

    echo "Results written to ${log_file}"

  done  # leaf_size_kb
done  # value_size
