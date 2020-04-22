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
	first = logsRDD.map(lambda x: ((x.split(" ")[0], x.split(" ")[3][1:12]), [x.split(" ")[6]])).filter(lambda x: x[0][1] == day1).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], x[1]))
	second = logsRDD.map(lambda x: ((x.split(" ")[0], x.split(" ")[3][1:12]), [x.split(" ")[6]])).filter(lambda x: x[0][1] == day2).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], x[1]))
	return first.join(second)

def bigrams(l): # compress list into bigram counter
 	bi_dict = {}
	for i in range(len(l) - 1):
		if (l[i], l[i+1]) in bi_dict:
			bi_dict[(l[i], l[i+1])] += 1
		else:
			bi_dict[(l[i], l[i+1])] = 1
	return bi_dict

def compress_dicts(l): # compress bigram dict list into one list of bigrams and counts
	compressed = {}
	for i in range(len(l)):
		for k in l[i]:
			if k in compressed:
				compressed[k] += l[i][k]
			else:
				compressed[k] = l[i][k]
	return list(map(lambda x: (x, compressed[x]), list(compressed)))

def task7(nobelRDD):
	split_motivations = nobelRDD.map(json.loads).map(lambda x: map(lambda x: x.get("motivation", "").strip().split(" "), x["laureates"]))
	cleaned = split_motivations.map(lambda x: filter(lambda x: len(x) > 1, x)).filter(lambda x: len(x) > 0)
	return cleaned.map(lambda x: compress_dicts(map(bigrams, x))).flatMap(lambda x: x).reduceByKey(lambda x, y: x + y)

def task8(bipartiteGraphRDD, currentMatching):
	return dummyrdd
