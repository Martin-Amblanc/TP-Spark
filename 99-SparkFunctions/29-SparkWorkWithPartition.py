from pyspark.sql import SparkSession

# Création d'une session Spark
spark = SparkSession.builder.appName("30-SparkWorkWithPartition.py").getOrCreate()

# Création d'un DataFrame avec un déséquilibre
data = [("A", 1), ("B", 2), ("A", 3), ("C", 4), ("B", 5), ("A", 6)]
columns = ["clé", "valeur"]
df = spark.createDataFrame(data, columns)

# Vérifier le nombre de partitions initial
print(f"Nombre de partitions initial : {df.rdd.getNumPartitions()}")

# Repartition par la colonne "clé"
df_repart = df.repartition(3, "clé")

# Vérifier le nouveau nombre de partitions
print(f"Nombre de partitions après repartition : {df_repart.rdd.getNumPartitions()}")

# Montrer le contenu de chaque partition (pour démonstration)
df_repart.rdd.glom().map(len).collect()
# Le résultat serait, par exemple, [2, 2, 2], ce qui est plus équilibré
df_repart.show()

spark.stop()