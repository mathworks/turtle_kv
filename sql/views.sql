-- Create view of turtlekv parameters per run.
--
create view if not exists turtlekv_params as
  select run_id,
         --
         avg(param_value)
           filter (where param_name=='turtlekv.chi')
           as 'chi',
         --
         avg(param_value)
           filter (where param_name=='turtlekv.cache_size_mb')
           as 'cache_size_mb',
         --
         avg(param_value)
           filter (where param_name=='turtlekv.cache_size_bytes')
           as 'cache_size',
         --
         avg(param_value)
           filter (where param_name=='turtlekv.buffer_level_trim')
           as 'buffer_trim'
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
