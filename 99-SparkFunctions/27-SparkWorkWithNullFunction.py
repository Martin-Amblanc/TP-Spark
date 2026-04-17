from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, ifnull, nullif, nvl, nvl2, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Création d'une session Spark
spark = SparkSession.builder.appName("29-SparkWorkWithNullFunction.py").getOrCreate()

# 1. Définir le schéma du DataFrame
my_schema = StructType([
    StructField("nom", StringType(), True),
    StructField("ville_preferee", StringType(), True),
    StructField("ville_secours", StringType(), True),
    StructField("valeur_nvl2", IntegerType(), True)
])

# Création d'un DataFrame avec des valeurs NULL pour le test
data = [
    ("Alice", "Paris", None, 1),
    ("Bob", None, "Londres", 2),
    (None, None, "Berlin", 3),
    ("David", "New York", "New York", 4)
]
columns = ["nom", "ville_preferee", "ville_secours", "valeur_nvl2"]
df = spark.createDataFrame(data, columns)

print("DataFrame initial:")
df.show()

# Utilisation de coalesce pour fusionner deux colonnes
df_coalesce = df.withColumn(
    "ville_coalesce", 
    coalesce(col("ville_preferee"), col("ville_secours"), lit("Ville inconnue"))
)
print("Après coalesce (fusion des villes):")
df_coalesce.show()

# Utilisation de ifnull et nvl (identiques)
df_ifnull = df.withColumn(
    "ville_ifnull", 
    ifnull(col("ville_preferee"), lit("N/A"))
)
print("Après ifnull (remplace les NULL par 'N/A'):")
df_ifnull.show()

# Utilisation de nullif
# Si la ville preferee est la même que la ville de secours, on renvoie NULL
df_nullif = df.withColumn(
    "villes_differentes", 
    #nullif(col("ville_preferee"), col("ville_secours"))
    when(col("ville_preferee") == col("ville_secours"), None).otherwise(col("ville_preferee"))
)
print("Après nullif (si ville_preferee == ville_secours, alors NULL):")
df_nullif.show()

# Utilisation de nvl2
# Si ville_preferee n'est pas NULL, on renvoie la valeur_nvl2
# Sinon, on renvoie la valeur -1
df_nvl2 = df.withColumn(
    "resultat_nvl2", 
    nvl2(col("ville_preferee"), col("valeur_nvl2"), lit(-1))
    #when(col("ville_preferee").isNotNull(), col("valeur_nvl2")).otherwise(lit(-1))
)
print("Après nvl2 (si ville_preferee non NULL, renvoie la valeur_nvl2, sinon -1):")
df_nvl2.show()

spark.stop()