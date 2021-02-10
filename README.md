# ImageMatching
Image recommendation for unillustrated Wikipedia articles

# Production data ETL

`etl` contains [pyspark](https://spark.apache.org/docs/latest/api/python/index.html) utilities to transform the 
algo raw output into a _production dataset_ that will be consumed by a service. 

```python
spark-submit etl/transform.py <raw data> <production data>
```
