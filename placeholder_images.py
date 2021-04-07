from pyspark.sql import SparkSession
import argparse
import requests
import pandas as pd
import numpy as np

spark = SparkSession.builder.getOrCreate()


class PlaceholderImages:
    def __init__(self, snapshot, username):
        self.snapshot = snapshot
        self.username = username

    @staticmethod
    def get_placeholder_category():
        try:
            r = requests.get('https://petscan.wmflabs.org/', params={'psid': '18699732', 'format': 'json'})
            r.raise_for_status()
            response_json = r.json()
            results = response_json['*'][0]['a']['*']
            plhd_cat = pd.DataFrame(np.array([[r['id'], r['title']] for r in results]),
                                    columns=['id', 'title'])
        except requests.exceptions.RequestException:
            return None
        else:
            return plhd_cat

    def run(self):
        plhd_cat = self.get_placeholder_category()

        if plhd_cat is None:
            plhd_table = f'{self.username}.placeholder_category'
        else:
            sdf = spark.createDataFrame(plhd_cat)
            sdf.createOrReplaceTempView('placeholder_category')
            plhd_table = 'placeholder_category'

        image_placeholders = spark.sql("""SELECT cl_from, cl_to, cl_type, page_title
                             FROM wmf_raw.mediawiki_categorylinks mc
                             JOIN wmf_raw.mediawiki_page mp
                             ON mp.page_id = cl_from
                             JOIN """ + plhd_table + """ pc
                             ON mc.cl_to = pc.title
                             WHERE mp.wiki_db = 'commonswiki'
                             AND mp.snapshot='""" + self.snapshot + """'
                             AND mp.page_namespace=6
                             AND mp.page_is_redirect=0
                             AND mc.snapshot='""" + self.snapshot + """'
                             AND mc.wiki_db ='commonswiki'
                            """)
        image_placeholders.coalesce(1).write.mode("overwrite").parquet("image_placeholders")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Executes placeholder_images with parameters. ' +
                                                 'Ex: python3 placeholder_images.py 2021-02')
    parser.add_argument('snapshot', help='Snapshot date. Ex: 2021-02')
    parser.add_argument('username', nargs='?', default='aikochou',
                        help='Fallback database name to get placeholder_category table. Defaults to: aikochou')

    args = parser.parse_args()
    placeholder_images = PlaceholderImages(args.snapshot, args.username)

    placeholder_images.run()
