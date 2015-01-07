from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('hw3').setMaster('local')
sc = SparkContext(conf=conf)
file = sc.textFile('pg5000.in')

counts = file.flatMap(lambda line: line.split(' '))\
        .map(lambda word: ('word', 1))\
        .reduceByKey(lambda a, b: a + b)

x = counts.collect()

f = open('wordcount.out', 'w')
f.write(str(x[0][1]))
f.close()


