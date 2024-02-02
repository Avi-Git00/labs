import pyspark
import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.memory", "16g").appName('chapter_2').getOrCreate()
prev = spark.read.csv("/home/lplab/Desktop/210962034/Week3/databases/block_1.csvter")
prev