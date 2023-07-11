#!/usr/bin/python3

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("linesHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
print(f"Lines: {lines.take(20)}")

lines = lines.map(lambda x: x.split()[2])

#def split(x):
#    return x.split()[2]

#lines = lines.map(split)


print(f"lines: {lines.take(20)}")
result = lines.countByValue()

print("Type for result: " + str(type(result)))
print(result.items())

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
