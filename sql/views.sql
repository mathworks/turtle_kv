-- Create view of turtlekv parameters per run.
--
create view if not exists turtlekv_params as
  select run_id,
         group_concat(param_value)
           filter (where param_name=='turtlekv.chi')
           as 'turtlekv.chi'
    from params
    group by run_id;

-- View table for the duration of each named workload.
--
create view if not exists workload_duration as
  select run_id,
         workload_basename,
         min(start_time) as start_time,
         max(end_time) as end_time,
         (max(end_time) - min(start_time)) as duration
    from
      (select run_id,
              workload_basename,
              thread_id,
              start_time,
              (start_time + duration) as end_time,
              duration
        from events
        where event_name == 'workload')
    group by run_id, workload_basename;

-- View table for the total operation count for each named workload.
--
create view if not exists workload_op_count as
  select run_id, workload_basename, sum(metric_value) as op_count
    from metrics
    where metric_name in ('put.count', 'get.count', 'scan.count')
    group by run_id, workload_basename;

-- View table for the overall throughput for each named workload.
--
create view if not exists thruput as
  select * from (
    select workload_duration.run_id,
           workload_duration.workload_basename as workload,
           (op_count / duration) / 1000 as kops
      from workload_op_count inner join workload_duration
        on (workload_op_count.workload_basename == workload_duration.workload_basename and
            workload_op_count.run_id == workload_duration.run_id)
  ) where kops != 0;

-- View table for per-op latency percentiles.
--
create view if not exists latency as
  select run_id,
         event_name as op,
         avg(duration) as mean,
         percentile(duration, 0) as p0,
         percentile(duration, 50) as p50,
         percentile(duration, 90) as p90,
         percentile(duration, 99) as p99,
         percentile(duration, 99.9) as p99_9,
         percentile(duration, 100) as p100
  from events
  group by run_id, event_name;
