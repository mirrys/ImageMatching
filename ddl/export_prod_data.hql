-- This script is used to export production datasets, 
-- in a format consumable by the APIs.
--
-- Run with:
-- hive -hiveconf username=${username} -hiveconf wiki=${wiki} -hiveconf snapshot=${monthly_snapshot} -f export_prod_data.hql
--
--
-- Format
--   * Include header: yes
--   * Field delimiter: "\t"
--   * Null value for missing recommendations 
--	(image_id, confidence_rating, source fields): ""
-- 
-- Changelog:
--   * 2021-03-08: schema and format freeze.
-- 
use ${hiveconf:username};
set hivevar:null_value="";

select page_id,
	page_title,
	nvl(image_id, ${null_value}) as image_id,
	nvl(confidence_rating, ${null_value}) as confidence_rating,
	nvl(source, ${null_value}) as source,
	dataset_id,
	insertion_ts, 
	wiki
from imagerec_prod 
where wiki = '${hiveconf:wiki}' and snapshot='${hiveconf:snapshot}'
