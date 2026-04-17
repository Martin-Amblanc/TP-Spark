from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("16-SparkCast.py").getOrCreate()

# Création d'un DataFrame avec une colonne 'age' en type String
df = spark.createDataFrame([("Alice", "25"), ("Bob", "30")], ["name", "age"])
df.printSchema()

# Conversion de la colonne 'age' en Integer
df_correct = df.withColumn("age", col("age").cast("integer"))
df_correct.printSchema()

# Arrêt de la session Spark
spark.stop()
