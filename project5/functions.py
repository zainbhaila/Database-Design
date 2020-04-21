import json
import re
from pyspark import SparkContext

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd):
	global dummyrdd
	dummyrdd = rdd

def task1(playRDD):
        return playRDD.map(lambda line: (line.split(" ")[0], (line, len(line.strip().split(" "))))).filter(lambda x: x[1][1] > 10)

def task2_flatmap(x):
        return map(lambda x: x.get("surname"), x["laureates"])

def task3(nobelRDD):
        return nobelRDD.map(json.loads).map(lambda x: (x["category"], task2_flatmap(x))).reduceByKey(lambda x, y: x + y)

def task4(logsRDD, l):
        return logsRDD.map(lambda x: (x.split(" ")[0], [x.split(" ")[3][1:12]])).reduceByKey(lambda x, y: x + y).filter(lambda x: all(elem in x[1] for elem in l)).map(lambda x: x[0])

def task5(bipartiteGraphRDD):
        return bipartiteGraphRDD.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)

def task6(logsRDD, day1, day2):
        reduced_by_name_date = logsRDD.map(lambda x: ((x.split(" ")[0], x.split(" ")[3][1:12]), [x.split(" ")[6]])).reduceByKey(lambda x, y: x + y)
        merged_dates = reduced_by_name_date.filter(lambda x: x[0][1] in [day1, day2]).map(lambda x: (x[0][0], (x[0][1], x[1]))).reduceByKey(lambda x, y: (x[1], y[1]) if x[0] == day1 else (y[1], x[1]))
        return merged_dates.filter(lambda x: isinstance(x[1][0], list))

def task7(nobelRDD):
        return dummyrdd

def task8(bipartiteGraphRDD, currentMatching):
        return dummyrdd
