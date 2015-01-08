import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

myRatedMovieIds = set()

def parseRating(line):
    fields = line.split('::')
    return int(fields[3]) % 10, (int(fields[0]), int(fields[1]), int(fields[2]))

def parseMovie(line):
    fields = line.split('::')
    return int(fields[0]), fields[1]
    
def parseUserRating(line):
    fields = line.split('::')
    myRatedMovieIds.add(int(fields[1]))
    return 0, (int(fields[0]), int(fields[1]), int(fields[2]))
    
# set up environment
conf = SparkConf() \
    .setAppName("MovieLensALS").setMaster('local') \
    .set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)

# load personal ratings
myRatingsRDD = sc.textFile("userrating.dat").map(parseUserRating)

# ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
ratings = sc.textFile("ratings.dat").map(parseRating)

# movies is an RDD of (movieId, movieTitle)
movies = dict(sc.textFile("movies.dat").map(parseMovie).collect())

# your code here
numPartitions = 4
training = ratings.filter(lambda x: x[0] < 6) \
           .values() \
           .union(myRatingsRDD) \
           .repartition(numPartitions) \
            .cache()

model = ALS.train(training, rank=10, iterations=10, lambda_=10)

candidates = sc.parallelize([m for m in movies if m not in myRatedMovieIds])
predictions = model.predictAll(candidates.map(lambda x: (0, x))).collect()
recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]

print len(myRatedMovieIds)
print "Movies recommended for you:"
for i in xrange(len(recommendations)):
    print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

# clean up
sc.stop()
