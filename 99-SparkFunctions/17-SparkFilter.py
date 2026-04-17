from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("17-SparkFilter.py").getOrCreate()

# Création d'un DataFrame
df = spark.createDataFrame([("Alice", 25), ("Bob", 30), ("Charlie", 35)], ["name", "age"])

# Filtrage pour ne garder que les personnes de plus de 30 ans
df_filtre = df.filter(col("age") > 30)
df_filtre.show()

# Utilisation de where() qui est un alias
df_filtre_where = df.where(col("age") > 30)
df_filtre_where.show()

# Arrêt de la session Spark
spark.stop()
