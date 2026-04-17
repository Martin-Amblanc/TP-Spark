from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Création d'une session Spark
spark = SparkSession.builder.appName("28-SparkWorkWithNull.py").getOrCreate()

# Création d'un schéma et d'un DataFrame avec des valeurs manquantes
data = [("Alice", None, 25), ("Bob", "paris", None), (None, "london", 30)]
schema = StructType([
    StructField("nom", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
print("DataFrame initial avec des valeurs manquantes :")
df.show()

# 1. Utilisation de .na.drop()
df_dropped = df.na.drop(subset=["age"])
print("DataFrame après la suppression des lignes avec des valeurs manquantes dans 'age' :")
df_dropped.show()

# 2. Utilisation de .na.fill()
df_filled = df.na.fill("Inconnu", subset=["ville"]) \
                  .na.fill(0, subset=["age"])
print("DataFrame après le remplacement des valeurs manquantes :")
df_filled.show()

spark.stop()
