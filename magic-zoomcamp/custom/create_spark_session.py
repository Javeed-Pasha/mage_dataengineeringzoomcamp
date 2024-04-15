import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@custom
def load_data(*args, **kwargs):
    credentials_location = '/home/src/my-creds.json'
    bucket_name='zoomcamp_b'
    project_id = 'forward-ace-411913'
    bigquery_dataset = 'zoomcamp_bigquery'

    #com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", "/home/src/lib/gcs-connector-hadoop3-2.2.5.jar,/home/src/lib/spark-3.3-bigquery-0.37.0.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
        .set("temporaryGcsBucket",bucket_name)

    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

    kwargs['context']['spark'] = spark
    kwargs['context']['bucket_name'] = bucket_name
    kwargs['context']['project_id'] = project_id
    kwargs['context']['bigquery_dataset'] = bigquery_dataset
    ...