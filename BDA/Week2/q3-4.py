from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("AggregationExample").getOrCreate()

# Sample data
data = [("Alice", 100),
        ("Bob", 150),
        ("Alice", 200),
        ("Bob", 250),
        ("Charlie", 300)]

# Define schema for the DataFrame
schema = ["name", "score"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Display the original DataFrame
print("Original DataFrame:")
df.show()

# Perform aggregation: Sum and Average
aggregated_df = df.groupBy("name").agg(F.sum("score").alias("total_score"), F.avg("score").alias("average_score"))

# Display the aggregated DataFrame
print("\nAggregated DataFrame:")
aggregated_df.show()



csv_file_path = "/home/lplab/Desktop/210962034/Week2/Data.csv"
df.write.option("header", True).option("delimiter",",").csv(csv_file_path)