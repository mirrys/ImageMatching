use ${hiveconf:username};
select page_id, page_title, image_id, confidence_rating, source, dataset_id, insertion_ts, wiki 
from imagerec_prod 
where wiki = '${hiveconf:wiki}' and snapshot='${hiveconf:snapshot}'
