/**********************************************************************************************
Purpose:         View to get information about the Amazon Redshift Spectrum query

Columns:         db_user_id                  - ID of the user who executed the Amazon Redshift Spectrum query
                 db_username                 - Username of the user who executed the Amazon Redshift Spectrum query
                 query_id                    - ID of the Amazon Redshift Spectrum query. The query_id column can be used to join other system tables and views
                 file_format                 - The file format of the external table data
                 start_date_utc              - Date in UTC (in ISO format, such as YYYY-MM-DD) that the Amazon Redshift Spectrum query started executing
                 end_date_utc                - Date in UTC (in ISO format, such as YYYY-MM-DD) that the Amazon Redshift Spectrum query finished executing
                 start_time_utc              - Time in UTC that the Amazon Redshift Spectrum query started executing
                 end_time_utc                - Time in UTC that the Amazon Redshift Spectrum query finished executing
                 total_s3_scanned_rows       - The total number of rows scanned from Amazon S3 and sent to the Amazon Redshift Spectrum layer
                 total_s3query_returned_rows - The total number of rows returned from the Amazon Redshift Spectrum layer to the cluster
                 spectrum_return_size_mb     - The number of megabytes returned from the Amazon Redshift Spectrum layer to the cluster
                 spectrum_scan_size_mb       - The number of megabytes scanned from Amazon S3 and sent to the Amazon Redshift Spectrum layer, based on compressed data
                 total_elapsed_sec           - The length of time (in seconds) that it took the Amazon Redshift Spectrum query to execute
                 total_files_processed       - The number of files that were processed for this Amazon Redshift Spectrum query
                 max_request_parallelism     - The maximum number of files processed on one slice.
                 avg_request_parallelism     - The average number of files processed on one slice
                 total_slowdown_count        - The total number of Amazon S3 requests with a slow down error that occurred during the external table scan
				 
Current Version: 1.01

History:
Version          1.01
2021-08-27       jasonpedreza Created
**********************************************************************************************/
create or replace view redshift_spectrum_scan_summary_vw as
select ss.userid as db_user_id
      ,u.usename as db_username 
      ,ss.query as query_id     
      ,ss.file_format      
      ,cast(ss.starttime as date) as start_date_utc
      ,cast(ss.endtime as date)  as end_date_utc
      ,ss.starttime as start_time_utc
      ,ss.endtime as end_time_utc
      ,ss.s3_scanned_rows as total_s3_scanned_rows
      ,ss.s3query_returned_rows as total_s3query_returned_rows
      ,ss.s3query_returned_bytes/1024/1024  as spectrum_return_size_mb
      ,ss.s3_scanned_bytes/1024/1024 as spectrum_scan_size_mb
      ,cast(ss.elapsed * 0.000001 as decimal(26,6)) as total_elapsed_sec
      ,ss.files as total_files_processed
      ,cast(ss.max_request_parallelism as bigint) as max_request_parallelism
      ,cast(ss.avg_request_parallelism as decimal(26,4)) as avg_request_parallelism
      ,ss.total_slowdown_count as total_slowdown_count
      ,cast(ss.starttime as date) as event_date_utc      
  from pg_catalog.svl_s3query_summary ss 
  inner join pg_catalog.pg_user u on (ss.userid=u.usesysid) 
  inner join (select sq.xid as x_xid,max(sq.aborted) as x_aborted 
                from pg_catalog.svl_qlog sq
               where cast(sq.starttime as date) = current_date-1
              group by sq.xid) q on (ss.xid=q.x_xid)          
 where ss.userid <> 1                              -- exclude rdsdb user
   and q.x_aborted = 0                             -- exlcude aborted or cancelled query
   and cast(ss.starttime as date) = current_date-1 -- date in UTC
;