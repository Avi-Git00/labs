import pyspark
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('DATA ANALYSIS').getOrCreate()
df=spark.read.csv('/home/lplab/Desktop/210962034/Week1/data.csv',header="True", inferSchema="True")

df.printSchema()
df.show()
df.head()
df.summary().show()
df.select('age').summary().show()