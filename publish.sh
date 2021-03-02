#!/usr/bin/env bash

# 2020-12-28 Output
snapshot=$1
outputdir=$2

wikis="enwiki arwiki kowiki cswiki viwiki frwiki fawiki ptwiki ruwiki trwiki plwiki hewiki svwiki ukwiki huwiki hywiki srwiki euwiki arzwiki cebwiki dewiki bnwiki"
monthly_snapshot=$(echo ${snapshot} | awk -F'-' '{print $1"-"$2}')
username=$(whoami)

test -d venv || make venv 

for wiki in ${wikis}; do
	# 1. Run the algo and generate data locally
	echo "Generating recommendations for ${wiki}"
	python algorunner.py ${snapshot} ${wiki} ${outputdir}

	# 2. Upload to HDFS
        echo "Publishing raw data to HDFS for ${wiki}"
	hadoop fs -rm -r imagerec/data/wiki_db=${wiki}/snapshot=${monthly_snapshot}/
	hadoop fs -mkdir -p imagerec/data/wiki_db=${wiki}/snapshot=${monthly_snapshot}/

	hadoop fs -copyFromLocal ${outputdir}/${wiki}_${snapshot}_wd_image_candidates.tsv imagerec/data/wiki_db=${wiki}/snapshot=${monthly_snapshot}/

	# 3. Update hive external table metadata
	echo "Updating Hive medatada for ${wiki}" 
	hive -hiveconf username=${username} -f ddl/external_imagerec.hql
done

# 4. Submit the Spark production data ETL
echo "Generating production data"
hadoop fs -rm -r imagerec_prod/data/

## Generate spark config
spark_config=conf/regular.spark.properties
cat conf/spark.properties.template /usr/lib/spark2/conf/spark-defaults.conf > ${spark_config}

spark2-submit --properties-file ${spark_config} etl/transform.py ${snapshot} imagerec/data/ imagerec_prod/data/

# 5. Update hive external table metadata (production)
hive -hiveconf username=${username} -f ddl/external_imagerec_prod.hql

# 6. Export production datasets
mkdir imagerec_prod_${snapshot}/
for wiki in ${wikis}; do
	hive -hiveconf username=${username} -hiveconf wiki=${wiki} -f export_prod_data.hql >  imagerec_prod/prod-${wiki}-${snapshot}-wd_image_candidates.tsv
done
