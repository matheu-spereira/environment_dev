from pyspark.sql import SparkSession
from pyspark import SparkConf
import uuid

def get_spark_session(_appName="Default AppName") -> SparkSession:
    conf = SparkConf() \
        .setAppName(_appName)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark
