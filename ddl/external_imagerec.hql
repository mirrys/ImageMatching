-- DDL to create an external table that exposes samples of the
-- production dataset.
-- The default HDFS location and Hive database are relative to a developer's.
-- username. Example hdfs://analytics-hadoop/user/clarakosi/imagerec/data.
--
-- The dataset will be available at https://superset.wikimedia.org/superset/sqllab via the
-- `presto_analytics` database.
--
-- Execution
-- hive -hiveconf username=<username> -f external_imagerec.hql

USE ${hiveconf:username};

CREATE EXTERNAL TABLE IF NOT EXISTS `imagerec` (
  `pandas_idx` string,
  `item_id` string,
  `page_id` string,
  `page_title` string,
  `top_candidates` string,
  `instance_of` string)
PARTITIONED BY (
  `wiki_db` string,
  `snapshot` string)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/user/${hiveconf:username}/imagerec';

-- Update partition metadata
MSCK REPAIR TABLE `imagerec`;

