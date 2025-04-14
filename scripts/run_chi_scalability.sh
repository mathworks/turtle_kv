#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)

bench_exe=${script_dir}/../build/Release/bench/turtle_kv_bench
data_file=/mnt/kv-bakeoff/random_data_file.small

export LOAD_ONLY=1
export N=${N:-$(pyexpr '160*1000*1000')}
#export N=$(pyexpr '24*1000*1000')

export turtlekv_node_size_kb=${node_size_kb:-4}
export turtlekv_leaf_size_kb=${leaf_size_kb:-16384}
export turtlekv_wal_size_mb=${wal_size_mb:-32768}
export turtlekv_storage_size_gb=${storage_size_gb:-256}
export turtlekv_filter_bits=${filter_bits:-12}
export turtlekv_key_size_hint=${key_size:-24}
export turtlekv_value_size_hint=${value_size:-100}
export turtlekv_buffer_level_trim=${buffer_level_trim:-0}
export turtlekv_min_flush_factor=${min_flush_factor:-1}
export turtlekv_max_flush_factor=${max_flush_factor:-1}
export turtlekv_checkpoint_pipeline=${checkpoint_pipeline:-1}

result_dir=${script_dir}/../build/benchmark_results
log_file=${result_dir}/chi_scaling_$(fts).log

mkdir -p ${result_dir}

{
    n_points=${n_points:-24}
    prev_chi=-1
    for i in $(seq 1 ${n_points});
    do
        export turtlekv_checkpoint_distance=$(pyexpr "int(2**((${i}-1)*9/(${n_points}-1)))")
        if [ "${prev_chi}" -ge "${turtlekv_checkpoint_distance}" ]; then
            export turtlekv_checkpoint_distance=$(expr ${prev_chi} \+ 1)
        fi

        #echo "$i $prev_chi ${turtlekv_checkpoint_distance}"
        prev_chi=${turtlekv_checkpoint_distance}
        
        MALLOCSTATS=1 ${script_dir}/run_bench.sh ${bench_exe} ${data_file}
        
        echo
        echo "=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------"
    done

} 2>&1 | tee ${log_file}

echo "Results written to ${log_file}"
