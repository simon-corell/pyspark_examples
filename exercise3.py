#!/usr/bin/python3

from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Exercise3").getOrCreate()

schema = StructType([StructField("ID", IntegerType(), True),
                     StructField("timestamp", IntegerType(), True),
                     StructField("amount", FloatType(), True)])

inputDF = spark.read.schema(schema).csv("customer-orders.csv")

filtered_inputDF = inputDF.select(inputDF.ID, inputDF.amount)

agg_filtered_inputDF = filtered_inputDF.groupBy(inputDF.ID).agg(func.round(func.sum(inputDF.amount), 2).alias("Total amount Spent")).orderBy("Total amount Spent")

agg_filtered_inputDF.show(agg_filtered_inputDF.count())

spark.stop()