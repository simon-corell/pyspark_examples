#!/usr/bin/python3
import pyspark
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.master("local") \
                    .appName('SimonSparkExample') \
                    .getOrCreate()

sc = spark_session.sparkContext

rdd = sc.textFile("./test")
print(rdd.count())

print(sc)
print("Spark App Name : "+ sc.appName)

