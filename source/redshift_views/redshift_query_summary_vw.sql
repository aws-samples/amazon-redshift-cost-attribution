/**********************************************************************************************
Purpose:         View to get information about a query execution at the query level

Columns:         db_user_id                   - ID of the user who executed the query
                 db_username                  - Username of the user who executed the query
                 query_type                   - A value that indicates the type of query being executed
                 is_superuser                 - A value that indicates whether the user is a superuser
                 label                        - Contains either the name of the file or the query group used to run the query or can be repurposed to define the application name or job name
                 query_id                     - ID of the Amazon Redshift Spectrum query. The query_id column can be used to join other system tables and views
                 database_name                - The name of the database the user was connected to when the query was issued
                 query_text                   - Actual query text for the query
                 start_date_utc               - Date in UTC (in ISO format, such as YYYY-MM-DD) that the query started executing
                 end_date_utc                 - Date in UTC (in ISO format, such as YYYY-MM-DD) that the query finished executing
                 start_time_utc               - Time in UTC that the query started executing
                 end_time_utc                 - Time in UTC that the query finished executing
                 alerts                       - A value that that might indicate performance issues on the query
                 aborted                      - A value that indicates if a query was aborted or canceled, this column contains 1. Otherwise, the column contains 0 (query ran to completion and returned results to the client)
                 used_concurrency_scaling     - A value that indicates whether the query ran on the concurrency scaling cluster, this column contains 1. Otherwise, the column contains 0 (query ran on the main cluster)
                 used_result_caching          - A value that indicates whether the query used result caching this column contains 1. Otherwise, the column contains 0 (result cache was not used)
                 query_execution_time_secs    - Elapsed execution time for a query (in seconds). Execution time doesn’t include time spent waiting in a queue
                 query_blocks_read_mb         - Number of 1 MB blocks read by the query
                 query_temp_blocks_to_disk_mb - The amount of disk space used by a query to write intermediate results, in MB
                 actual_spectrum_scan_size_mb - The amount of data, in MB, scanned by Amazon Redshift Spectrum in Amazon S3
                 rated_spectrum_scan_size_mb  - The amount of data, in MB, scanned by Amazon Redshift Spectrum in Amazon S3 that is set at 10MB minimum per query and used for the Spectrum billing
                 query_cpu_time_secs          - CPU time used by the query, in seconds
                 query_cpu_usage_percent      - The percentage of CPU time used by the query
                 query_queue_time_secs        - The amount of time in seconds that the query was queued
                 nested_loop_join_row_count   - The number of rows in a nested loop join
                 service_class                - ID for the WLM query queue (service class) defined in the WLM configuration
                 queue_name                   - The name of the service class. Not applicable if the query used result caching
                 service_class_category       - Category for the WLM query queue (service class) defined in the WLM configuration
                 query_slot_count             - Number of WLM query slots used by the query

				 
Current Version: 1.01

History:
Version          1.01
2021-08-27       jasonpedreza Created
**********************************************************************************************/
create or replace view redshift_query_summary_vw as
select sq.userid as db_user_id 
      ,pu.usename as db_username
      ,case
         when pu.usename like '%admin%' then 'admin'
         when pu.usename like '%etl%' or pu.usename like '%elt%' then 'data_pipeline'
		     when pu.usename like '%report%' or pu.usename like '%dashboard%' then 'reporting' 
         else 'user'
       end as query_type
      ,case when pu.usesuper = true then 'Y' else 'N' end as is_superuser  
       -- label contains either the name of the file or the query group used to run the query or can be repurposed to define the application name or job name
      ,trim(sq.label) as label 
      ,sq.query as query_id
      ,trim(sq.database) as database_name
      ,trim(sq.querytxt) as query_text
      ,cast(sq.starttime as date) as start_date_utc
      ,cast(sq.endtime as date) as end_date_utc
      ,sq.starttime as start_time_utc
      ,sq.endtime as end_time_utc
      ,case when alrt.num_events is null then 0 else alrt.num_events end as alerts
      ,sq.aborted
      ,case when sq.concurrency_scaling_status = 1 then 1 else 0 end as used_concurrency_scaling
      ,case when sl.source_query is null then 0 else 1 end as used_result_caching
      ,cast(sl.elapsed * 0.000001 as decimal(26,6)) as query_execution_time_secs	  
      ,case when sqms.query_blocks_read is null then 0 else sqms.query_blocks_read end as query_blocks_read_mb
      ,case when sqms.query_temp_blocks_to_disk is null then 0 else sqms.query_temp_blocks_to_disk end as query_temp_blocks_to_disk_mb
      ,case when ssv.spectrum_scan_size_mb is null then 0 else ssv.spectrum_scan_size_mb end as actual_spectrum_scan_size_mb
      -- Spectrum price is charged at 10MB minimum per query
      -- https://aws.amazon.com/redshift/pricing/#Redshift_Spectrum_pricing
      ,case when ssv.spectrum_scan_size_mb is null then 0 else case when ssv.spectrum_scan_size_mb < 10 then 10 else ssv.spectrum_scan_size_mb end end as rated_spectrum_scan_size_mb
      ,cast(case when sqms.query_cpu_time is null then 0 else sqms.query_cpu_time end as decimal(26,6)) as query_cpu_time_secs
      ,case when sqms.query_cpu_usage_percent is null then 0 else sqms.query_cpu_usage_percent end as query_cpu_usage_percent
      ,cast(case when sqms.query_queue_time is null then 0 else sqms.query_queue_time end as decimal(26,6)) as query_queue_time_secs
      ,case when sqms.nested_loop_join_row_count is null then 0 else sqms.nested_loop_join_row_count end as nested_loop_join_row_count
      ,swq.service_class
      ,case when sq.concurrency_scaling_status = 1 then 'burst' else rtrim(swsc.name) end as queue_name  
      ,case
         when sq.concurrency_scaling_status = 1 then 'Concurrency Scaling'
         when swq.service_class between 1 and 4 then 'System'
         when swq.service_class = 5 then 'Superuser'
         when swq.service_class between 6 and 13 then'Manual WLM queues'
         when swq.service_class = 14 then 'SQA'
         when swq.service_class = 15 then 'Redshift Maintenance'
         when swq.service_class between 100 and 107 then 'Auto WLM'
       end as service_class_category
      ,swq.slot_count as query_slot_count
      ,cast(sq.starttime as date) as event_date_utc   	  
  from pg_catalog.stl_query sq
  inner join pg_catalog.pg_user pu on (sq.userid = pu.usesysid)
  inner join pg_catalog.svl_qlog sl on (sl.userid = sq.userid and sl.query = sq.query)  
  left outer join pg_catalog.svl_query_metrics_summary sqms on (sqms.userid = sq.userid and sqms.query = sq.query)
  left outer join pg_catalog.stl_wlm_query swq on (sq.userid = swq.userid and sq.query = swq.query)
  left outer join pg_catalog.stv_wlm_service_class_config swsc on (swsc.service_class = swq.service_class)
  left outer join redshift_spectrum_scan_summary_vw ssv on (ssv.db_user_id = sq.userid and ssv.query_id = sq.query and cast(sq.starttime as date) = ssv.event_date_utc)
  left outer join (select sae.query
                         ,cast(1 as integer) as num_events
                     from pg_catalog.stl_alert_event_log sae
                   group by sae.query) as alrt on (alrt.query = sq.query)   				   
  where sq.userid <> 1                               -- exclude rdsdb user
    and sq.aborted = 0                               -- exlcude aborted or cancelled query                          
    and cast(sq.starttime as date) = current_date-1  -- date in UTC
;