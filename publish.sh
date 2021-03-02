#!/usr/bin/env bash
# Run the ImageRecommendation algo via algorunner.py, and generate production datasets
# for all languages defined in `wikis`.
#  
# The intermediate algo output and the production datasets will be stored in HDFS
# and exposed as Hive external tables:
#
# - <username>.imagerec: raw datasets (algo output). Maps to hdfs:///users/<username>/imagerec
# - <username>.imagerec_prod: production datasets. Maps to hdfs:///users/<username>/imagerec_prod
# 
# Where <username> is the user currently running the publish.sh script.
#
# Production datasets will be exported locally, in tsv format, under ./imagerec_prod_${snapshot}.
#
# Usage: ./publish.sh <snapshot>
# Example: ./publish.sh 2021-01-25

# Target wikis to train ImageMatching on
wikis="enwiki arwiki kowiki cswiki viwiki frwiki fawiki ptwiki ruwiki trwiki plwiki hewiki svwiki ukwiki huwiki hywiki srwiki euwiki arzwiki cebwiki dewiki bnwiki"

# YYYY-MM
monthly_snapshot=$(echo ${snapshot} | awk -F'-' '{print $1"-"$2}')
username=$(whoami)

# Path were raw dataset (Jupyter algo output) will be stored
algo_outputdir=Output

make venv 
source venv/bin/activate
# TODO(gmodena, 2021-02-02): 
# Passing one wiki at a time to get a feeling for runtime deltas (to some degree, we could
get this info from parsing hdfs snapshots).
# We could pass the whole list at algorunner.py at once,
# and have the pipeline run on a single (long running) spark job. Instead, here we
# are spinning up one spark cluster per wiki. This needs checking with AE, in order
# to better understand which workload better fits our Hadoop cluster.
for wiki in ${wikis}; do
	# 1. Run the algo and generate data locally
	echo "Generating recommendations for ${wiki}"
	STARTIME=${SECONDS}
	python algorunner.py ${snapshot} ${wiki} ${algo_outputdir}
	ENDTIME=${SECONDS}
	echo "metrics.algorunner.${wiki}.${snapshot}.seconds,$(($ENDTIME - $STARTTIME))"
	

	# 2. Upload to HDFS
        echo "Publishing raw data to HDFS for ${wiki}"
	STARTIME=${SECONDS}
	hadoop fs -rm -r imagerec/data/wiki_db=${wiki}/snapshot=${monthly_snapshot}/
	hadoop fs -mkdir -p imagerec/data/wiki_db=${wiki}/snapshot=${monthly_snapshot}/

	hadoop fs -copyFromLocal ${algo_outputdir}/${wiki}_${snapshot}_wd_image_candidates.tsv imagerec/data/wiki_db=${wiki}/snapshot=${monthly_snapshot}/
	ENDTIME=${SECONDS}
	echo "metrics.hdfs.copyrawdata.${wiki}.${snapshot}.seconds,$(($ENDTIME - $STARTTIME))"

	# 3. Update hive external table metadata
	echo "Updating Hive medatada for ${wiki}" 
	STARTIME=${SECONDS}
	hive -hiveconf username=${username} -f ddl/external_imagerec.hql
done
	ENDTIME=${SECONDS}
	echo "metrics.hive.imagerec.${wiki}.${snapshot}.seconds,$(($ENDTIME - $STARTTIME))"
# 4. Submit the Spark production data ETL
echo "Generating production data"
hadoop fs -rm -r imagerec_prod/data/

## Generate spark config
spark_config=conf/regular.spark.properties
cat conf/spark.properties.template /usr/lib/spark2/conf/spark-defaults.conf > ${spark_config}

STARTIME=${SECONDS}
spark2-submit --properties-file ${spark_config} etl/transform.py ${snapshot} imagerec/data/ imagerec_prod/data/
ENDTIME=${SECONDS}
echo "metrics.etl.transfrom.${snapshot}.seconds,$(($ENDTIME - $STARTTIME))"


# 5. Update hive external table metadata (production)
STARTIME=${SECONDS}
hive -hiveconf username=${username} -f ddl/external_imagerec_prod.hql
ENDTIME=${SECONDS}
echo "hive.imagerec_prod.${snapshot}    $(($ENDTIME - $STARTTIME))"

# 6. Export production datasets
STARTIME=${SECONDS}
mkdir imagerec_prod_${snapshot}/
for wiki in ${wikis}; do
	hive -hiveconf username=${username} -hiveconf wiki=${wiki} -f export_prod_data.hql >  imagerec_prod/prod-${wiki}-${snapshot}-wd_image_candidates.tsv
done
ENDTIME=${SECONDS}
echo "Datasets are available at $(pwd)/imagerec_prod_${snapshot}/"
echo "metrics.etl.export_prod_data.${snapshot}.seconds,$(($ENDTIME - $STARTTIME))"
