#!/usr/bin/python3

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Exercise1")

sc = SparkContext(conf = conf)

lines = sc.textFile("./customer-orders.csv")

def parse_lines(line):
    fields = line.split(",")
    return (fields[0], float(fields[2]))

sum_of_amounts = lines.map(parse_lines).reduceByKey(lambda x,y: x+y)
sum_of_amounts_sorted = sum_of_amounts.map(lambda x: (x[1], x[0])).sortByKey().collect()
#sum_of_amounts_sorted = sorted(sum_of_amounts.collect(), key=lambda item: item[1])

for amount_spent, customer in sum_of_amounts_sorted:
    print(customer + ": " + str(amount_spent))