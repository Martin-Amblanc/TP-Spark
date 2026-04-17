from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("18-SparkDistinct.py").getOrCreate()

# Créez un DataFrame avec des lignes en double
data = [("A", 1), ("B", 2), ("A", 1), ("C", 3), ("B", 2)]
columns = ["clé", "valeur"]
df = spark.createDataFrame(data, columns)

print("DataFrame initial avec doublons :")
df.show()

# Utilisez la méthode distinct() pour obtenir les lignes uniques
df_distinct = df.distinct()

print("DataFrame après l'utilisation de distinct() :")
df_distinct.show()

# Comptez le nombre de lignes dans les deux DataFrames pour voir la différence
print(f"Nombre de lignes dans le DataFrame initial : {df.count()}")
print(f"Nombre de lignes dans le DataFrame sans doublons : {df_distinct.count()}")

# Arrêt de la session Spark
spark.stop()
