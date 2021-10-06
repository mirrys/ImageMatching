#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
import pickle
import pandas as pd
import math
import numpy as np
import random
import requests
#from bs4 import BeautifulSoup
import json
import os
import getpass
import logging
import time

class MyAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '%s\t%s' % (self.extra['app_id'], msg), kwargs

# In[ ]:

logging.basicConfig(filename='ima_udfs.log', filemode='a', format='%(asctime)s\t%(levelname)s\t%(message)s')

# In[ ]:




# In[ ]:


qids_and_properties={}


# In[ ]:


# Pass in directory to place output files
output_dir = 'Output'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    
# Pass in the full snapshot date
snapshot = '2021-07-26'

# Allow the passing of a single language as a parameter
language = 'kowiki'

# A spark session type determines the resource pool
# to initialise on yarn
spark_session_type = 'regular'

# Name of placeholder images parquet file
image_placeholders_file = 'image_placeholders'


# In[ ]:


# """
# Will be used for findspark.init().
# """
# SPARK_HOME = os.environ.get("SPARK_HOME", "/usr/lib/spark2")

# import findspark
# findspark.init(SPARK_HOME)

# import pyspark
# import pyspark.sql
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("yarn").appName("ImageRec-DEV Training").config("spark.submit.deployMode","cluster").getOrCreate()


# In[ ]:


# We use wmfdata boilerplate to init a spark session.
# Under the hood the library uses findspark to initialise
# Spark's environment. pyspark imports will be available 
# after initialisation

import pyspark
from  pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

app_id = spark.sparkContext.applicationId 
logger = MyAdapter(logging.getLogger(__name__), {'app_id': app_id})
logger.setLevel(logging.DEBUG)

# In[ ]:


# languages=['enwiki','arwiki','kowiki','cswiki','viwiki','frwiki','fawiki','ptwiki','ruwiki','trwiki','plwiki','hewiki','svwiki','ukwiki','huwiki','hywiki','srwiki','euwiki','arzwiki','cebwiki','dewiki','bnwiki'] #language editions to consider
#val=100 #threshold above which we consider images as non-icons

languages=[language]


# In[ ]:


reg = r'^([\w]+-[\w]+)'
short_snapshot = re.match(reg, snapshot).group()
short_snapshot


# In[ ]:


reg = r'.+?(?=wiki)'
label_lang = re.match(reg, language).group()
label_lang


# In[ ]:


len(languages)


# In[ ]:


image_placeholders = spark.read.parquet(image_placeholders_file)
image_placeholders.createOrReplaceTempView("image_placeholders")


# In[ ]:


def get_threshold(wiki_size):
    #change th to optimize precision vs recall. recommended val for accuracy = 5
    sze, th, lim = 50000, 15, 4 
    if (wiki_size >= sze):
        #if wiki_size > base size, scale threshold by (log of ws/bs) + 1
        return (math.log(wiki_size/sze, 10)+1)*th
    #else scale th down by ratio bs/ws, w min possible val of th = th/limiting val
    return max((wiki_size/sze) * th, th/lim)


# In[ ]:


val={}
total={}
for wiki in languages:
    querytot="""SELECT COUNT(*) as c
     FROM wmf_raw.mediawiki_page
     WHERE page_namespace=0 
     AND page_is_redirect=0
     AND snapshot='"""+short_snapshot+"""' 
     AND wiki_db='"""+wiki+"""'"""
    logger.info(f"Retrieving placeholder data")
    start_time = time.time()
    wikisize = spark.sql(querytot).toPandas()
    val[wiki]=get_threshold(int(wikisize['c']))
    total[wiki]=int(wikisize['c'])
    end_time = time.time()
    logger.info(f"Placeholder data retrieved in {end_time - start_time}s")
# In[ ]:


val


# In[ ]:


total


# In[ ]:


wikisize


# The query below retrieves, for each unillustrated article: its Wikidata ID, the image of the Wikidata ID (if any), the Commons category of the Wikidata ID (if any), and the lead images of the articles in other languages (if any).
# 
# `allowed_images` contains the list of icons (images appearing in more than `val` articles)
# 
# `image_pageids` contains the list of illustrated articles (articles with images that are not icons)
# 
# `noimage_pages` contains the pageid and Qid of unillustrated articles
# 
# `qid_props` contains for each Qid in `noimage_pages`, the values of the following properties, when present:
# * P18: the item's image
# * P373: the item's Commons category
# * P31: the item's "instance of" property
# 
# `category_image_list` contains the list of all images in a Commons category in `qid_props`
# 
# `lan_page_images` contains the list of lead images in Wikipedia articles in all languages linked to each Qid
# 
# `qid_props_with_image_list` is qid_props plus the list of images in the Commons category linked to the Wikidata item
# 
# 

# In[ ]:


for wiki in languages:
    print(wiki)
    queryd="""WITH allowed_images AS 
     (
     SELECT il_to
     FROM wmf_raw.mediawiki_imagelinks
     WHERE il_from_namespace=0 
     AND snapshot='"""+short_snapshot+"""'  
     AND wiki_db='"""+wiki+"""' 
     AND il_to not like '%\"%' AND il_to not like '%,%'
     GROUP BY il_to  
     HAVING COUNT(il_to)>"""+str(val[wiki])+"""),
     image_pageids AS 
     (SELECT DISTINCT il_from as pageid
     FROM wmf_raw.mediawiki_imagelinks il1 
     LEFT ANTI JOIN allowed_images
     ON allowed_images.il_to=il1.il_to
     WHERE il1.il_from_namespace=0 
     AND il1.wiki_db='"""+wiki+"""' 
     AND il1.snapshot='"""+short_snapshot+"""'
     ),
     pageimage_pageids AS 
     (
     SELECT DISTINCT pp_page as pageid
     FROM wmf_raw.mediawiki_page_props pp
     WHERE pp.wiki_db ='"""+wiki+"""'
     AND pp.snapshot='"""+short_snapshot+"""'
     AND pp_propname in ('page_image','page_image_free')),
     all_image_pageids as(
     SELECT pageid 
     FROM image_pageids 
     UNION
     SELECT pageid
     FROM pageimage_pageids
     ),
     noimage_pages as 
     (
     SELECT wipl.item_id,p.page_id,p.page_title,page_len
     FROM wmf_raw.mediawiki_page p 
     JOIN wmf.wikidata_item_page_link wipl
     ON p.page_id=wipl.page_id
     LEFT ANTI JOIN all_image_pageids
     on all_image_pageids.pageid=wipl.page_id
     WHERE p.page_namespace=0 
     AND page_is_redirect=0 AND p.wiki_db='"""+wiki+"""' 
     AND p.snapshot='"""+short_snapshot+"""' 
     AND wipl.snapshot='"""+snapshot+"""'
     AND wipl.page_namespace=0
     AND wipl.wiki_db='"""+wiki+"""'
     ORDER BY page_len desc
     ),
     qid_props AS 
     (
     SELECT we.id,label_val, 
     MAX(CASE WHEN claim.mainSnak.property = 'P18' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS hasimage,
     MAX(CASE WHEN claim.mainSnak.property = 'P373' THEN REPLACE(REPLACE(claim.mainSnak.datavalue.value,'\"',''),' ','_') ELSE NULL END) AS commonscategory,
     MAX(CASE WHEN claim.mainSnak.property = 'P31' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS instanceof
     FROM wmf.wikidata_entity we
     JOIN noimage_pages
     ON we.id=noimage_pages.item_id
     LATERAL VIEW explode(labels) t AS label_lang,label_val
     LATERAL VIEW OUTER explode(claims) c AS claim
     WHERE typ='item'
     AND t.label_lang='"""+label_lang+"""'
     AND snapshot='"""+snapshot+"""'
     AND claim.mainSnak.property in ('P18','P31','P373')
     GROUP BY id,label_val
     ),
     category_image_list AS
     (
     SELECT cl_to,concat_ws(';',collect_list(mp.page_title)) as category_imagelist
     from qid_props
     left join wmf_raw.mediawiki_categorylinks mc
     on qid_props.commonscategory=mc.cl_to
     join wmf_raw.mediawiki_page mp
     on mp.page_id=mc.cl_from
     LEFT ANTI JOIN image_placeholders
     on image_placeholders.page_title = mp.page_title
     WHERE mp.wiki_db ='commonswiki'
     AND mp.snapshot='"""+short_snapshot+"""'
     AND mp.page_namespace=6
     AND mp.page_is_redirect=0
     AND mc.snapshot='"""+short_snapshot+"""'
     AND mc.wiki_db ='commonswiki'
     AND mc.cl_type='file'
     group by mc.cl_to
     ),
     qid_props_with_image_list AS
     (
     SELECT id, label_val, hasimage, commonscategory, instanceof,category_imagelist
     from qid_props
     left join category_image_list
     on qid_props.commonscategory=category_image_list.cl_to
     ),
     lan_page_images AS
     (
     SELECT nip.item_id,nip.page_id,nip.page_title,nip.page_len,collect_list(concat(pp.wiki_db,': ',pp.pp_value)) as lan_images
     FROM noimage_pages nip
     LEFT JOIN  wmf.wikidata_item_page_link wipl
     LEFT JOIN wmf_raw.mediawiki_page_props pp
     LEFT JOIN wmf_raw.mediawiki_page mp
     ON nip.item_id=wipl.item_id
     AND wipl.page_id=pp.pp_page
     AND wipl.wiki_db=pp.wiki_db
     AND mp.page_title=pp.pp_value
     LEFT ANTI JOIN image_placeholders
     ON image_placeholders.page_title = pp.pp_value
     WHERE wipl.wiki_db !='"""+wiki+"""'
     AND wipl.snapshot='"""+snapshot+"""'
     AND wipl.page_namespace=0
     AND pp.snapshot='"""+short_snapshot+"""'
     AND pp_propname in ('page_image','page_image_free')
     AND mp.wiki_db ='commonswiki'
     AND mp.snapshot='"""+short_snapshot+"""'
     AND mp.page_namespace=6
     AND mp.page_is_redirect=0
     GROUP BY nip.item_id,nip.page_id,nip.page_title,nip.page_len
     ),
     joined_lan_page_images AS
     (
     SELECT nip.item_id,nip.page_id,nip.page_title,nip.page_len, lpi.lan_images
     from noimage_pages nip
     LEFT JOIN lan_page_images lpi
     on nip.item_id=lpi.item_id
     )
     SELECT * from joined_lan_page_images
     LEFT JOIN qid_props_with_image_list
     on qid_props_with_image_list.id=joined_lan_page_images.item_id
     
    """
    logger.info(f"Triggering qids_and_properties SQL for {wiki} {snapshot}")
    start_time = time.time()
    qid_props = spark.sql(queryd).cache()
    end_time = time.time()
    logger.info(f"Cached {qid_props.count()} rows qids_and_properties[{wiki}] in {end_time - start_time}s")
    qids_and_properties[wiki]=qid_props


# Below I am just creating different tables according to whether an image is retrieved from a specific source (Wikidata image, Commons Category, or interlingual links)

# In[ ]:


# hasimage={}
# commonscategory={}
# lanimages={}
# allimages={}
# for wiki in languages:
#     print(wiki)
#     hasimage[wiki]=qids_and_properties[wiki][qids_and_properties[wiki]['hasimage'].astype(str).ne('None')]
#     commonscategory[wiki]=qids_and_properties[wiki][qids_and_properties[wiki]['category_imagelist'].astype(str).ne('None')]
#     lanimages[wiki]=qids_and_properties[wiki][qids_and_properties[wiki]['lan_images'].astype(str).ne('None')]
#     print("number of unillustrated articles: "+str(len(qids_and_properties[wiki])))
#     print("number of articles items with Wikidata image: "+str(len(hasimage[wiki])))
#     print("number of articles items with Wikidata Commons Category: "+str(len(commonscategory[wiki])))
#     print("number of articles items with Language Links: "+str(len(lanimages[wiki])))
#     ####
#     allimages[wiki]=qids_and_properties[wiki]


# Below the two functions to select images depending on the source:
# * `select_image_language` takes as input the list of images from articles in multiple languages and selects the one which is used more often across languages (after some major filtering)
# * `select_image_category` selects at random one of the images in the Commons category linked to the Wikidata item.

# Below the priority assignment process:
# * If the article has a Wikidata image (not a flag, as this is likely a duplicate), give it priority 1
# * Choose up to 3 images among the ones from related Wikipedia articles  in other languages, using the `select_image_language` function, and give priority 2.x where `x` is a ranking given by the number of languages using that image 
# * If the article has an associated Commons category, call the `select_image_category` function, randomly selecting up to 3 images form that category
# 

# In[ ]:


from pyspark.sql import functions as F
from pyspark.sql import Column
from pyspark.sql.functions import udf, pandas_udf,  PandasUDFType
from pyspark.sql.types import ArrayType, MapType, StringType
import itertools


# In[ ]:


# Rewrite helper functions as panda udfs

@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def select_image_language_udf(imagelist: pd.Series) -> pd.Series:     
    results=[]

    for row in imagelist.values:
        if row is not None:
            languages={} #contains which languages cover a given image
            counts={} #contains counts of image occurrences across languages
            for image in row:
                data=image.strip().split(' ')#this contains the language and image name data
                ###
                if len(data)==2: #if we actually have 2 fields
                    iname=data[1].strip()
                    lan=data[0].strip()[:-1]
                    ###
                    if iname not in counts: #if this image does not exist in our counts yet, initialize counts
                        substring_list=['.svg','flag','noantimage','no_free_image','image_manquante',
                            'replace_this_image','disambig','regions','map','map','default',
                            'defaut','falta_imagem_','imageNA','noimage','noenzyimage']

                        if any(map(iname.lower().__contains__, substring_list)): #if the image name is not valid
                            continue
                       # urll = 'https://commons.wikimedia.org/wiki/File:'+iname.replace(' ','_')+'?uselang='+language
                        #page = requests.get(urll)
                        #if page.status_code == 404:
                         #   print (urll)
                         #   continue
                        counts[iname]=1
                        languages[iname]=[]
                    else:
                        counts[iname]+=1
                    languages[iname].append(lan)
            results.append(json.dumps(languages))
        else:
            results.append(None)
    return pd.Series(results)

@pandas_udf(returnType=ArrayType(StringType())) 
def select_commons_images_udf(commons_column: pd.Series) -> pd.Series:
    results=[]
    for row in commons_column.values:
        if row is not None:
            commons_images=[]
            def select_image_category(imagelist):
                languages={}
                data=list(imagelist.strip().split(';'))
                data=[d for d in data if d.find('.')!=-1]
                return random.choice(data)

            for i in range(min(len(list(row.strip().split(';'))),3)):
                image=select_image_category(row)
                rating=3
                note='image was found in the Commons category linked in the Wikidata item'
                commons_images.append(json.dumps({'image':image,'rating':rating,'note':note}))
            results.append(commons_images)
        else:
            results.append(None)
    return pd.Series(results)

@pandas_udf(returnType=ArrayType(StringType()))
def select_wikipedia_images_udf(wikipedia_column: pd.Series) -> pd.Series:
    results=[]
    for row in wikipedia_column.values:
        if row is not None:
            wikipedia=json.loads(row)
            wikipedia_images=[]
            index=np.argsort([len(l) for l in list(wikipedia.values())])

            for i in range(min(len(wikipedia),3)):
                image=list(wikipedia.keys())[index[-(i+1)]]
                rating=2+(float(i)/10)
                note='image was found in the following Wikis: '+', '.join(wikipedia[image])
                wikipedia_images.append(json.dumps({'image':image,'rating':rating,'note':note}))
            results.append(wikipedia_images)
        else:
            results.append(None)
    return pd.Series(results)

@pandas_udf(returnType=ArrayType(StringType()))
def select_wikidata_images_udf(wikidata_column: pd.Series) -> pd.Series:
    results=[]
    
    for row in wikidata_column.values:
        if row is not None and row.lower().find('flag') ==-1:
            image=row[1:-1]
            rating=1
            note='image was in the Wikidata item'
            results.append([json.dumps({'image':image,'rating':rating,'note':note})])
        else:
            results.append(None)
    return pd.Series(results)
    
@pandas_udf(returnType=ArrayType(StringType()))
def select_top_candidates_udf(wikidata_col: pd.Series, wikipedia_col: pd.Series, commons_col: pd.Series) -> pd.Series:
    results=[]
    
    for wikidata, wikipedia, commons in zip(wikidata_col.values, wikipedia_col.values, commons_col.values):
        top_candidates=[]
        
        if wikidata is not None:
            for image in wikidata:
                if len(top_candidates) < 3:
                    top_candidates.append(image)
                else:
                    break
        
        if wikipedia is not None:
            for image in wikipedia:
                if len(top_candidates) < 3:
                    top_candidates.append(image)
                else:
                    break
        
        if commons is not None:
            for image in commons:
                if len(top_candidates) < 3:
                    top_candidates.append(image)
                else:
                    break
        results.append(top_candidates)
        
    return pd.Series(results)


# In[ ]:


for wiki in languages:
    df = qids_and_properties[wiki].withColumn(
        "wikipedia_imagelist", select_image_language_udf(F.col("lan_images"))
    ).withColumn(
         "wikipedia_images",  select_wikipedia_images_udf(F.col("wikipedia_imagelist"))
    ).withColumn(
        "commons_images", select_commons_images_udf(F.col("category_imagelist"))
    ).withColumn(
        "wikidata_images", select_wikidata_images_udf(F.col("hasimage"))
    ).withColumn(
        "top_candidates", select_top_candidates_udf(F.col("wikidata_images"), F.col("wikipedia_images"), F.col("commons_images"))
    ).select(
        "item_id",
        "page_id",
        "page_title",
        "instanceof",
#         "wikipedia_imagelist",
#         "commons_images",
#         "wikipedia_images",
#         "wikidata_images",
        "top_candidates"
    )
    logger.info("Applying UDFs on {qids_and_properties[wiki].count()} rows")
    start_time = time.time()
    df.cache()
    end_time = time.time()
    logger.info(f"Applied UDFs in {end_time - start_time}s")

    logger.info(f"Saving data to hdfs for {wiki}")
    start_time = time.time()
    fname = f'{wiki}_{snapshot}_wd_image_candidates_test'
    df.write.mode("overwrite").save(fname, format="parquet")
    end_time = time.time()
    logger.info(f"Saved {fname} in {end_time - start_time}s")
    df.write.save(f'{wiki}_{snapshot}_wd_image_candidates_test', format="parquet")


# In[ ]:


spark.stop()

