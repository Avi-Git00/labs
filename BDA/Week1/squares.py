import pyspark
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable

from pyspark.sql import SparkSession

spark=SparkSession.builder.config("spark.driver.memory","16g").appName('square').getOrCreate()

import pandas as pd
from pyspark.sql import functions as F
df_pd = pd.DataFrame(
    data={'integers': [1, 2, 3],
     'floats': [-1.0, 0.5, 2.7],
     'integer_arrays': [[1, 2], [3, 4, 5], [6, 7, 8, 9]]}
)
df = spark.createDataFrame(df_pd)
df.printSchema()
df.show()

from pyspark.sql.functions import udf
@udf
def square(x):
    return x*x

df.select('integers',square('integers').alias('int_squared')).show()