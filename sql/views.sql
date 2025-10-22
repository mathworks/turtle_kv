-- Create view of turtlekv parameters per run.
--
create view if not exists turtlekv_params as
  select run_id,
         group_concat(param_value)
           filter (where param_name=='turtlekv.chi')
           as 'turtlekv.chi'
    from params
    group by run_id;
