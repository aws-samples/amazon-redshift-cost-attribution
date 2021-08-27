/**********************************************************************************************
Purpose:         View to get information about the estimated compute cost (in USD) of each query execution 

Columns:         db_user_id                              - ID of the user who executed the query
                 db_username                             - Username of the user who executed the query
                 is_superuser                            - A value that indicates whether the user is a superuser
                 label                                   - Contains either the name of the file or the query group used to run the query or can be repurposed to define the application name or job name
                 database_name                           - The name of the database the user was connected to when the query was issued
                 query_type                              - A value that indicates the type of query being executed				 
                 query_id                                - ID of the Amazon Redshift Spectrum query. The query_id column can be used to join other system tables and views
                 queue_name                              - The name of the service class. Not applicable if the query used result caching				 
                 query_cpu_time_secs                     - CPU time used by the query, in seconds
                 query_cpu_time_ratio                    - The decimal form of the percent of CPU time used by the query 
                 adj_query_cpu_time_ratio                - 
                 query_cpu_cost                          -
                 adj_query_cpu_cost                      -
                 query_execution_time_secs               - Elapsed execution time for a query (in seconds). Execution time doesn’t include time spent waiting in a queue
                 query_execution_time_ratio              - The decimal form of the percent of execution time used by the query 
                 adj_query_execution_time_ratio          -
                 daily_redshift_compute_cost             - The estimated compute cost (in USD) of the Amazon Redshift cluster for the day				 
                 query_execution_cost                    -
                 adj_query_execution_cost                -
                 query_disk_io_mb                        - Number of 1 MB blocks read by the query including the amount of disk space used by a query to write intermediate results
                 query_disk_io_ratio                     - The decimal form of the percent of number of 1 MB blocks used by the query
                 adj_query_disk_io_ratio                 -
                 query_disk_io_cost                      -
                 adj_query_disk_io_cost                  -
                 actual_spectrum_scan_size_mb            - The amount of data, in MB, scanned by Amazon Redshift Spectrum in Amazon S3
                 rated_spectrum_scan_size_mb             - The amount of data, in MB, scanned by Amazon Redshift Spectrum in Amazon S3 that is set at 10MB minimum per query and used for the Spectrum billing
                 redshift_spectrum_cost                  - The estimated cost (in USD) of Amazon Redshift Spectrum query being executed based on the rated Amazon Redshift Spectrum scan size
                 redshift_spectrum_price_per_tb          - The price per terabyte of data scanned in using Amazon Redshift Spectrum
                 redshift_query_cost                     - The estimated compute cost (in USD) of query being executed
                 total_main_cluster_cpu_time_secs        - Total CPU time for all queries that ran in completion (in seconds)
                 total_main_cluster_execution_time_secs  - Total elapsed execution time for all queries that ran in completion (in seconds) in the main cluster
                 total_main_cluster_disk_io_mb           - Total Number of 1 MB blocks for all queries that ran in completion (in seconds)
                 total_burst_cluster_execution_time_secs - Total elapsed execution time for all queries that ran in completion (in seconds) in the concurrency scaling cluster
                 total_rated_spectrum_scan_size_mb       - The total amount of data, in MB, scanned by Amazon Redshift Spectrum in Amazon S3 that is set at 10MB minimum per query and used for the Spectrum billing
                 redshift_compute_utilization            - A snapshot view of the percentage of compute used by the Amazon Redshift cluster
                 redshift_storage_utilization            - A snapshot view of the percentage of disk space used by the Amazon Redshift cluster
                 used_concurrency_scaling                - A value that indicates whether the query ran on the concurrency scaling cluster, this column contains 1. Otherwise, the column contains 0 (query ran on the main cluster)
                 used_result_caching                     - A value that indicates whether the query used result caching this column contains 1. Otherwise, the column contains 0 (result cache was not used)			 
				 
Current Version: 1.01

History:
Version          1.01
2021-08-27       jasonpedreza Created
**********************************************************************************************/
create or replace view redshift_query_attribution_vw as
with redshift_cluster_node as (
select count(1) as node_count
      -- Check the pricing based on the AWS region in which the Amazon Redshift cluster is located
      ,3.26 as price_per_node_per_hour                                                  -- Modify this based on the price per hour of the compute node (Check https://aws.amazon.com/redshift/pricing/)
      ,24 as daily_operation_hour                                                       -- Modify this based on the number of hours the Amazon Redshift cluster is available (default is 24 hours if using a Reserved instance)
      ,5.0 as spectrum_price_per_tb                                                     -- Check https://aws.amazon.com/redshift/pricing/#Redshift_Spectrum_pricing
      ,cast(0.013 as decimal(26,6)) as concurrency_price_per_second                     -- Check https://aws.amazon.com/redshift/pricing/#Concurrency_Scaling_pricing
      -- Sum of rated score should be equivalent to 1
      -- cpu_rated_score + disk_io_rated_score + execution_rated_score = 1
      ,cast(0.25 as decimal(26,6)) as cpu_rated_score                                   -- Modify this based on the percentage that should be allocated to the cost
      ,cast(0.25 as decimal(26,6)) as disk_io_rated_score                               -- Modify this based on the percentage that should be allocated to the cost
      ,cast(0.5 as decimal(26,6)) as execution_rated_score                              -- Modify this based on the percentage that should be allocated to the cost
      ,cast(count(1) 
      * 3.26                                                                            -- Modify this based on the price per hour of the compute node (Check https://aws.amazon.com/redshift/pricing/)
      * 24 as decimal(26,6)) as daily_redshift_compute_cost                             -- Modify this based on the number of hours the Amazon Redshift cluster is available (default is 24 hours if using a Reserved instance)
      ,sum(snsc.used) as cluster_storage_used_mb
      ,sum(snsc.capacity) as cluster_storage_capacity_mb
      ,sum(snsc.used * .000001) as cluster_storage_used_tb
      ,sum(snsc.capacity * .000001) as cluster_storage_capacity_tb      
      ,sum(cast(snsc.used as decimal(26,6))) / sum(cast(snsc.capacity as decimal(26,6))) * 100 as cluster_storage_utilization
  from pg_catalog.stv_node_storage_capacity snsc
),
redshift_cluster_summary as (
select count(1)                                                                                         as total_query_count
      ,sum(rqs.query_cpu_time_secs)                                                                     as total_main_cluster_cpu_time_secs
      ,sum(case when rqs.used_concurrency_scaling = 0 then rqs.query_execution_time_secs else 0 end) as total_main_cluster_execution_time_secs
      ,sum(rqs.query_blocks_read_mb) + sum(rqs.query_temp_blocks_to_disk_mb)                            as total_main_cluster_disk_io_mb
      ,sum(case when rqs.used_concurrency_scaling = 1 then rqs.query_execution_time_secs else 0 end) as total_burst_cluster_execution_time_secs
      ,sum(rqs.rated_spectrum_scan_size_mb)                                                             as total_rated_spectrum_scan_size_mb
      ,sum(rqs.actual_spectrum_scan_size_mb)                                                            as total_actual_spectrum_scan_size_mb
      ,max(rcn.daily_redshift_compute_cost)                                                             as daily_redshift_compute_cost
      ,max(rcn.spectrum_price_per_tb)                                                                   as redshift_spectrum_price_per_tb
      ,max(rcn.concurrency_price_per_second)                                                            as redshift_burst_price_per_second
      ,max(rcn.daily_operation_hour)                                                                    as redshift_daily_operation_hour
      ,max(rcn.cluster_storage_utilization)                                                             as redshift_storage_utilization
      ,max(rcn.cpu_rated_score)                                                                         as cpu_rated_score
      ,max(rcn.disk_io_rated_score)                                                                     as disk_io_rated_score
      ,max(rcn.execution_rated_score)                                                                   as execution_rated_score
  from redshift_query_summary_vw rqs
  cross join redshift_cluster_node rcn 
),
redshift_cost_attribution as (
select rqs.db_user_id
      ,rqs.db_username
      ,rqs.is_superuser
      ,rqs.label
      ,rqs.database_name
      ,rqs.query_type      
      ,rqs.query_id
      ,rqs.queue_name
      ,rqs.query_cpu_time_secs
      ,rqs.query_cpu_time_secs / case when rcs.total_main_cluster_cpu_time_secs > 0 then rcs.total_main_cluster_cpu_time_secs else 1 end as query_cpu_time_ratio
      ,cast((rqs.query_cpu_time_secs / case when rcs.total_main_cluster_cpu_time_secs > 0 then rcs.total_main_cluster_cpu_time_secs else 1 end) * rcs.daily_redshift_compute_cost as decimal(26,6)) as query_cpu_cost
      ,rqs.query_execution_time_secs
      ,rqs.query_execution_time_secs / case when rcs.total_main_cluster_execution_time_secs > 0 then rcs.total_main_cluster_execution_time_secs else 1 end as query_execution_time_ratio 
      ,cast((rqs.query_execution_time_secs / case when rcs.total_main_cluster_execution_time_secs > 0 then rcs.total_main_cluster_execution_time_secs else 1 end) * rcs.daily_redshift_compute_cost as decimal(26,6)) as query_execution_cost
      ,rqs.query_blocks_read_mb + rqs.query_temp_blocks_to_disk_mb as query_disk_io_mb
      ,cast((rqs.query_blocks_read_mb + rqs.query_temp_blocks_to_disk_mb) as decimal(26,6)) / case when rcs.total_main_cluster_disk_io_mb > 0 then rcs.total_main_cluster_disk_io_mb else 1 end as query_disk_io_ratio
      ,cast((cast((rqs.query_blocks_read_mb + rqs.query_temp_blocks_to_disk_mb) as decimal(26,6)) / case when rcs.total_main_cluster_disk_io_mb > 0 then rcs.total_main_cluster_disk_io_mb else 1 end) * rcs.daily_redshift_compute_cost as decimal(26,6)) as query_disk_io_cost
      ,rqs.actual_spectrum_scan_size_mb
      ,rqs.rated_spectrum_scan_size_mb
      ,cast((rqs.rated_spectrum_scan_size_mb / 1024.0 / 1024.0) * rcs.redshift_spectrum_price_per_tb as decimal(26,6)) as redshift_spectrum_cost   
      ,rcs.redshift_spectrum_price_per_tb
      ,rcs.total_main_cluster_cpu_time_secs
      ,rcs.total_main_cluster_execution_time_secs
      ,rcs.total_main_cluster_disk_io_mb
      ,rcs.total_burst_cluster_execution_time_secs
      ,rcs.total_rated_spectrum_scan_size_mb
      ,(rcs.total_main_cluster_execution_time_secs / (3600.0 * rcs.redshift_daily_operation_hour)) * 100 redshift_compute_utilization
      ,rcs.redshift_storage_utilization
      ,rcs.cpu_rated_score
      ,rcs.disk_io_rated_score
      ,rcs.execution_rated_score
      ,rcs.daily_redshift_compute_cost
      ,rqs.used_concurrency_scaling
      ,rqs.used_result_caching
      ,rqs.event_date_utc
  from redshift_query_summary_vw rqs
  cross join redshift_cluster_summary rcs
),
adjusted_redshift_cost_attribution as (
select rca.*   
      ,rca.query_cpu_time_ratio * rca.cpu_rated_score as adj_query_cpu_time_ratio
      ,(rca.query_cpu_time_ratio * rca.cpu_rated_score) * rca.daily_redshift_compute_cost as adj_query_cpu_cost    
      ,rca.query_execution_time_ratio * (rca.execution_rated_score + case when rca.total_main_cluster_cpu_time_secs = 0 then rca.cpu_rated_score else 0 end + case when rca.total_main_cluster_disk_io_mb = 0 then rca.disk_io_rated_score else 0 end) as adj_query_execution_time_ratio                 
      ,(rca.query_execution_time_ratio * (rca.execution_rated_score + case when rca.total_main_cluster_cpu_time_secs = 0 then rca.cpu_rated_score else 0 end + case when rca.total_main_cluster_disk_io_mb = 0 then rca.disk_io_rated_score else 0 end)) * rca.daily_redshift_compute_cost as adj_query_execution_cost
      ,rca.query_disk_io_ratio * rca.disk_io_rated_score as adj_query_disk_io_ratio
      ,(rca.query_disk_io_ratio * rca.disk_io_rated_score) * rca.daily_redshift_compute_cost as adj_query_disk_io_cost
  from redshift_cost_attribution rca
)
select arca.db_user_id
      ,arca.db_username                         
      ,arca.is_superuser        
      ,arca.label              
      ,arca.database_name                           
      ,arca.query_type                              
      ,arca.query_id                              
      ,arca.queue_name                              
      ,cast(arca.query_cpu_time_secs as decimal(12,4)) as query_cpu_time_secs                 
      ,cast(arca.query_cpu_time_ratio as decimal(12,6)) as query_cpu_time_ratio      
      ,cast(arca.adj_query_cpu_time_ratio as decimal(12,6)) as adj_query_cpu_time_ratio 
      ,cast(arca.query_cpu_cost as decimal(26,6)) as query_cpu_cost 
      ,cast(arca.adj_query_cpu_cost as decimal(26,6)) as adj_query_cpu_cost
      ,cast(arca.query_execution_time_secs as decimal(12,4)) as query_execution_time_secs             
      ,cast(arca.query_execution_time_ratio as decimal(12,6)) as query_execution_time_ratio    
      ,cast(arca.adj_query_execution_time_ratio as decimal(12,6)) as adj_query_execution_time_ratio
      ,cast(arca.daily_redshift_compute_cost as decimal(12,6)) as daily_redshift_compute_cost
      ,cast(arca.query_execution_cost as decimal(26,6)) as query_execution_cost                     
      ,cast(arca.adj_query_execution_cost as decimal(26,6)) as adj_query_execution_cost       
      ,arca.query_disk_io_mb                        
      ,cast(arca.query_disk_io_ratio as decimal(12,6)) as query_disk_io_ratio 
      ,cast(arca.adj_query_disk_io_ratio as decimal(12,6)) as adj_query_disk_io_ratio 
      ,cast(arca.query_disk_io_cost as decimal(26,6)) as query_disk_io_cost
      ,cast(arca.adj_query_disk_io_cost as decimal(26,6)) as adj_query_disk_io_cost
      ,arca.actual_spectrum_scan_size_mb            
      ,arca.rated_spectrum_scan_size_mb             
      ,cast(arca.redshift_spectrum_cost as decimal(26,6)) as redshift_spectrum_cost                  
      ,cast(arca.redshift_spectrum_price_per_tb as decimal(12,4)) as redshift_spectrum_price_per_tb 
      ,cast(arca.adj_query_cpu_cost
     + arca.adj_query_disk_io_cost
     + arca.adj_query_execution_cost
     + arca.redshift_spectrum_cost as decimal(26,6)) as redshift_query_cost                     
      ,cast(arca.total_main_cluster_cpu_time_secs as decimal(26,6)) as total_main_cluster_cpu_time_secs       
      ,cast(arca.total_main_cluster_execution_time_secs as decimal(26,6)) as total_main_cluster_execution_time_secs  
      ,arca.total_main_cluster_disk_io_mb           
      ,cast(arca.total_burst_cluster_execution_time_secs as decimal(26,6)) as total_burst_cluster_execution_time_sec 
      ,arca.total_rated_spectrum_scan_size_mb       
      ,cast(arca.redshift_compute_utilization as decimal(12,6)) as redshift_compute_utilization            
      ,cast(arca.redshift_storage_utilization as decimal(12,6)) as redshift_storage_utilization 
      ,arca.used_concurrency_scaling
      ,arca.used_result_caching      
      ,arca.event_date_utc     
  from adjusted_redshift_cost_attribution arca
;