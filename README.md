![](https://github.com/mirrys/ImageMatching/workflows/build/badge.svg?branch=main)

# ImageMatching
Image recommendation for unillustrated Wikipedia articles

# Production data ETL

`etl` contains [pyspark](https://spark.apache.org/docs/latest/api/python/index.html) utilities to transform the 
algo raw output into a _production dataset_ that will be consumed by a service. 

```python
spark-submit etl/transform.py <raw data> <production data>
=======
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
