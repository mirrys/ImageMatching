-- DDL to create an external table that exposes samples of the 
-- production dataset.
-- The default HDFS location and Hive database are relative to a developer's.
-- username. Example hdfs://analytics-hadoop/user/gmodena/imagerec_prod/data.
--
-- The dataset will be available at https://superset.wikimedia.org/superset/sqllab via the 
-- `presto_analytics` database.
--
-- Execution
-- hive -hiveconf username=<username> -f external_imagerec_prod.hql

USE ${hiveconf:username};

CREATE EXTERNAL TABLE IF NOT EXISTS `imagerec_prod`(
  `page_id` string,
  `page_title` string,
  `image_id` string,
  `confidence_rating` string,
  `source` string,
  `dataset_id` string,
  `insertion_ts` float)
PARTITIONED BY (`wiki` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t',
  'serialization.null.format'='""')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/user/${hiveconf:username}/imagerec_prod/data';


-- Update partition metadata
MSCK REPAIR TABLE `imagerec_prod`;
