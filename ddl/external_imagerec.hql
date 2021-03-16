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

CREATE EXTERNAL TABLE IF NOT EXISTS `imagerec`(
  `pandas_idx` string,
  `item_id` string,
  `page_id` string,
  `page_title` string,
  `top_candidates` string)
PARTITIONED BY (
  `wiki_db` string,
  `snapshot` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/user/${hiveconf:username}/imagerec/data'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Update partition metadata
MSCK REPAIR TABLE `imagerec`;