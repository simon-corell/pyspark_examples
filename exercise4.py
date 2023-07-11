#!/usr/bin/python3

### Find superheros with only 1 connection. Find the superhero with the least amount of connections.

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

spark_session = SparkSession.builder.appName("Exercise4").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True),
                     StructField("Name", StringType(), True)])

inputDF_graph = spark_session.read.text("Marvel-Graph.txt")
inputDF_names = spark_session.read.schema(schema).option("sep", " ").csv("Marvel-Names.txt")

connections = inputDF_graph.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
              .withColumn("size", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
              .groupBy("id").agg(func.sum("size").alias("size")) \
              .filter(func.col("size") <= 1) \

def get_ids_dict(ids_names):
    ids_dict = {}
    for id, name in ids_names:
        ids_dict[str(id)] = name
    return ids_dict

ids_dict = get_ids_dict(inputDF_names.collect())

def lookupName(movieID):
    return ids_dict[movieID]

lookupNameUDF = func.udf(lookupName)

names = connections.withColumn("Name", lookupNameUDF(func.col("id")))

names = inputDF_names.join(connections, "id")

names_with_one_connections = names.filter(func.col("size") == 1)
#names_with_zero_connections = names.filter(func.col("size") == 0)
#name_with_least_connections = names.filter(func.col("size") == 0).first()

print("Super heroes with ONE connections!: ")
names_with_one_connections.show()

#print("Super heroes with ZERO connections!: ")
#names_with_zero_connections.show()

#print("The most obscere Super hero with ZERO connections!: " + str(name_with_least_connections[2]))


spark_session.stop()


#print(ids)

#connections.show()
#inputDF_graph.show()
#inputDF_names.show()
