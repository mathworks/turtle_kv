-- Create view of turtlekv parameters per run.
--
create view if not exists turtlekv_params as
  select run_id,
         --
         avg(param_value)
           filter (where param_name=='turtlekv.leaf_size_kb')
           as "leaf_size_kb",
         --
         group_concat(param_value)
           filter (where param_name=='turtlekv.chi')
           as "chi",
         --
         avg(param_value)
           filter (where param_name=='turtlekv.cache_size_mb')
           as "cache_size_mb",
         --
         avg(param_value)
           filter (where param_name=='turtlekv.cache_size_bytes')
           as "cache_size",
         --
         avg(param_value)
           filter (where param_name=='turtlekv.buffer_level_trim')
           as "buffer_trim",
         --
         avg(param_value)
           filter (where param_name=='turtlekv.big_mem_tables')
           as "big_mem_tables",
         --
         (case when '1' = (group_concat(distinct param_value)
                            filter(where param_name='turtlekv.config.bloom_filter'))
          then 'bloom' else
          (case when '1' = (group_concat(distinct param_value)
                             filter(where param_name='turtlekv.config.quotient_filter'))
           then 'quotient' else '--' end) end)
           as "filter_type",
         --
         avg(param_value)
           filter (where param_name=='turtlekv.filter_bits')
           as "filter_bits"
    from params
    group by run_id;


-- Create view of turtlekv metrics.
--
create view if not exists turtlekv_metrics as
  select
      run_id,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.kv_store.put.count')
        as puts,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.kv_store.put_retry.count')
        as put_retries,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.kv_store.checkpoint.count')
        as checkpoints,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.checkpoint.batch_update.flush.count')
        as in_tree_flushes,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.checkpoint.batch_update.merge_compact.count')
        as merge_compactions,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.checkpoint.batch_update.running_total.count')
        as running_totals,
      --
      sum(cast(metric_value as real))
        filter(where metric_name = 'turtlekv.checkpoint.batch_update.split.count')
        as tree_splits,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_latency.count'))
      ) as put_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_memtable_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_memtable_latency.count'))
      ) as put_memtable_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_memtable_create_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_memtable_create_latency.count'))
      ) as memtable_create_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_memtable_queue_push_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_memtable_queue_push_latency.count'))
      ) as memtable_queue_push_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.compact_batch_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.compact_batch_latency.count'))
      ) as compact_batch_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.apply_batch_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.apply_batch_latency.count'))
      ) as apply_batch_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.checkpoint.batch_update.merge_compact_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.apply_batch_latency.count'))
      ) as merge_compact_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.checkpoint.batch_update.running_total_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.apply_batch_latency.count'))
      ) as running_total_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.result_set.compact_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.apply_batch_latency.count'))
      ) as result_set_compact_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.finalize_checkpoint_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.finalize_checkpoint_latency.count'))
      ) as finalize_checkpoint_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.append_job_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.append_job_latency.count'))
      ) as write_checkpoint_latency,
      --
      ( (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_wait_trim_latency.seconds'))
      / (sum(cast(metric_value as real))
           filter(where metric_name = 'turtlekv.kv_store.put_wait_trim_latency.count'))
      ) as wait_trim_latency
      --
    from metrics
    group by run_id;

create view if not exists turtlekv_tree_metrics as select
    run_id,
    workload_basename as workload,
    (sum(metric_value) filter(where metric_name like '%.pivot_count'))
     / (sum(metric_value) filter(where metric_name like '%.tree.node_count'))
     as avg_pivots,
    (sum(metric_value) filter(where metric_name like '%.tree.buffer_segment_count'))
     / (sum(metric_value) filter(where metric_name like '%.tree.node_count'))
     as avg_segments,
    (sum(metric_value) filter(where metric_name like '%.tree.nonempty_level_count'))
     / (sum(metric_value) filter(where metric_name like '%.tree.node_count'))
     as avg_levels
  from metrics
  group by run_id, workload_basename;


create view if not exists turtlekv_memory_stats_events as
  select
      run_id,
      workload_basename as workload,
      start_time as timestamp,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.log_alloc_bytes')
        as log_alloc_bytes_total,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.log_free_bytes')
        as log_free_bytes_total,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_admit_bytes')
        as cache_admitted_bytes_total,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_evict_bytes')
        as cache_evicted_bytes_total,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_erase_bytes')
        as cache_removed_bytes_total,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_pin_bytes')
        as cache_pinned_bytes_total,
      --
      avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_unpin_bytes')
        as cache_unpinned_bytes_total,
      --
      ( avg(event_data) filter(where event_name='turtlekv.stats.memory.log_alloc_bytes')
      - avg(event_data) filter(where event_name='turtlekv.stats.memory.log_free_bytes')
      ) as log_bytes_in_use,
      --
      ( avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_admit_bytes')
      - avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_evict_bytes')
      ) as cache_bytes_in_use,
      --
      ( avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_pin_bytes')
      - avg(event_data) filter(where event_name='turtlekv.stats.memory.cache_unpin_bytes')
      ) as cache_bytes_pinned
      --
    from events
    where event_name like 'turtlekv.stats.memory.%'
    group by run_id, workload_basename, start_time;

-- Run/Workload summary of memory stats.
--
create view if not exists turtlekv_memory_stats as
  select
      run_id,
      workload,
      min(log_bytes_in_use) as min_log_bytes,
      avg(log_bytes_in_use) as avg_log_bytes,
      max(log_bytes_in_use) as max_log_bytes,
      min(cache_bytes_in_use) as min_cache_bytes,
      avg(cache_bytes_in_use) as avg_cache_bytes,
      max(cache_bytes_in_use) as max_cache_bytes,
      min(cache_bytes_pinned) as min_pinned_bytes,
      avg(cache_bytes_pinned) as avg_pinned_bytes,
      max(cache_bytes_pinned) as max_pinned_bytes
    from turtlekv_memory_stats_events
    group by run_id, workload;

-- Filter False Positive Rate
--
create view if not exists turtlekv_filter_stats as
   select
       run_id, filter_type, filter_bits,
       --
       ( (sum(metric_value) filter(where metric_name = 'turtlekv.key_query.filter_false_positive_count') )
       / (sum(metric_value) filter(where metric_name = 'turtlekv.key_query.filter_positive_count') )
       ) as false_positive_rate
       --
     from metrics join turtlekv_params using(run_id)
     group by run_id;
