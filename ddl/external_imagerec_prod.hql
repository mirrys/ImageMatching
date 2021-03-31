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
  `instance_of` string,
  `is_article_page` boolean,
  `dataset_id` string,
  `insertion_ts` double, 
  `found_on` array<string>)
PARTITIONED BY (`wiki` string, `snapshot` string)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/user/${hiveconf:username}/imagerec_prod';

-- Update partition metadata
MSCK REPAIR TABLE `imagerec_prod`;

