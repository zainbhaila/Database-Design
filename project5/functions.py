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
        return dummyrdd

def task5(bipartiteGraphRDD):
        return dummyrdd

def task6(logsRDD, day1, day2):
        return dummyrdd

def task7(nobelRDD):
        return dummyrdd

def task8(bipartiteGraphRDD, currentMatching):
        return dummyrdd
