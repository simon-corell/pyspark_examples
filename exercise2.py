#!/usr/bin/python3

import pyspark
from pyspark.sql import Row, SparkSession, functions as func, Spark


spark = SparkSession.builder.appName("Exercise2").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("./fakefriends-header.csv")

people_filtered = people.select(people.age, people.friends)

people_filtered.groupBy(people_filtered.age).avg("friends").sort("avg(friends)").show()

people_filtered.groupBy(people_filtered.age).agg(func.round(func.avg("friends"),2).alias("avg_friends")).sort("avg_friends").show()

spark.stop()