-- DDL to create an external table that exposes the
-- production dataset for the search team.
-- The default HDFS location and Hive database are relative to a developer's.
-- username. Example hdfs://analytics-hadoop/user/clarakosi/search_imagerec/data.
--
-- The dataset will be available at https://superset.wikimedia.org/superset/sqllab via the
-- `presto_analytics` database.
--
-- Execution
-- hive -hiveconf username=<username> -f external_search_imagerec.hql
USE ${hiveconf:username};

CREATE EXTERNAL TABLE IF NOT EXISTS `search_imagerec`(
  `wikiid` string,
  `page_id` int,
  `page_namespace` int,
  `recommendation_type` string)
PARTITIONED BY (`year` int, `month` int, `day` int)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/user/${hiveconf:username}/search_imagerec';
-- Update partition metadata
MSCK REPAIR TABLE `search_imagerec`;