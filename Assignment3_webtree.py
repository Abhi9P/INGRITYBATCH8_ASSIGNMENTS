from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, concat_ws
from pyspark.sql.window import Window
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WebTreeCumulative") \
    .master("local[*]") \
    .getOrCreate()

# Input and output paths
input_path = "C:/Ingrity Training/python/day 1 new/Input_data.csv"
output_path = "C:/Users/abhin/Downloads/final_output_webtree.csv"

# Read input CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Create  Web_TREE
window_spec = Window.partitionBy("DVISION_ID", "CLASS_ID", "BRAND_ID") \
    .orderBy("PARENT_CATEGORY_ID") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_path = df.withColumn("Web_TREE", concat_ws("_", collect_list("PARENT_CATEGORY_ID").over(window_spec)))

# Convert to Pandas and save as CSV in Downloads
df_with_path.toPandas().to_csv(output_path, index=False)

# Stop Spark
spark.stop()

print(f"File successfully saved to: {output_path}")
