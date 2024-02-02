import pyspark
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
# Load the text file
lines = sc.textFile("/home/lplab/Desktop/210962034/Week2/word.txt")
counts = lines.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
# Create a SparkSession
spark = SparkSession.builder.getOrCreate()
# Load the text file
lines = spark.read.text("/home/lplab/Desktop/210962034/Week2/word.txt")
# Split the lines into words
words = lines.withColumn('word', f.explode(f.split(f.col('value'), ' ')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)\
    .show()
# Stop the SparkSession
spark.stop()