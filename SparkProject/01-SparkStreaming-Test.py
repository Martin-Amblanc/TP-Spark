# mon_script.py
import sys
from pyspark.sql import SparkSession

# Vérifiez la version de Python utilisée par le driver et les workers
print(f"Python version being used: {sys.version}")

spark = SparkSession.builder \
    .appName("1-SparkStreaming-Test.py") \
    .getOrCreate()

# On demande à Spark de se taire et de ne montrer que les ERREURS
spark.sparkContext.setLogLevel("ERROR")

data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["Name", "ID"])

df.show()

spark.stop()
