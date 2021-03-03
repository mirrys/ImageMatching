![](https://github.com/mirrys/ImageMatching/workflows/build/badge.svg?branch=main)

# ImageMatching
Image recommendation for unillustrated Wikipedia articles

## Getting started

Connect to stat1005 through ssh (the remote machine that will host your notebooks)
```
ssh stat1005.eqiad.wmnet
```

### Installation
First, clone the repository
```shell
git clone https://github.com/clarakosi/ImageMatching.git
```

Setup and activate the virtual environment
```shell
cd ImageMatching
virtualenv -p python3 venv
source venv/bin/activate
```

Install the dependencies
```shell
export=http_proxy=http://webproxy.eqiad.wmnet:8080
export=https_proxy=http://webproxy.eqiad.wmnet:8080
python3 setup.py install
```

### Running the script

To run the script pass in the **snapshot** (required), **language** (defaults to all wikis),
and **output directory** (defaults to Output)
```shell
python3 algorunner.py 2020-12-28 hywiki Output
```

The output .ipynb and .tsv files can be found in your output directory
```shell
ls Output
hywiki_2020-12-28.ipynb  hywiki_2020-12-28_wd_image_candidates.tsv
```

## Production data ETL

`etl` contains [pyspark](https://spark.apache.org/docs/latest/api/python/index.html) utilities to transform the 
algo raw output into a _production dataset_ that will be consumed by a service. 

The transform etl can be run on a local cluster with:
```python
spark2-submit etl/transform.py <raw data> <production data>
```

`conf/spark.properties` provides default settings to run the ETL as a [regular size spark job](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Cluster/Spark#Spark_Resource_Settings) on WMF's Analytics cluster.

```python
spark2-submit --properties-file conf/spark.properties etl/transform.py <raw data> <production data>
```

## Metrics collection
On WMF's cluster the Hadoop Resource Manager (and Spark History) is available at `https://yarn.wikimedia.org/cluster`.
Additional instrumentation can be enabled by passing `metrics.properites` file to the Notebook or ETL jobs. A template
metrics files, that outpus to the driver and executors stdout, can be found at `conf/metrics.properties.template`.

The easiest way to do it by setting `PYSPARK_SUBMISSION_ARGS`. For example
```bash
export PYSPARK_SUBMIT_ARGS="--files ./conf/metrics.properties --conf spark.metrics.conf=metrics.properties pyspark-shell"
python3 algorunner.py 2020-12-28 hywiki Output
```
Will submit the `algorunner` job, with additional instrumentation.

For more information refer to https://spark.apache.org/docs/latest/monitoring.html.
### Get dataset Metrics
To get the dataset metrics run the dataset_metrics_python script. The script expects the **snapshot** (required)
and **output directory** (defaults to Output)
```shell
cd Dataset_Metrics/
python3 dataset_metrics_runner.py 2021-01 Output
```