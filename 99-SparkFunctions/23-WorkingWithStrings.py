from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower, upper, trim, split, length, substring, concat, instr

# Création d'une session Spark
spark = SparkSession.builder.appName("23-WorkingWithStrings").getOrCreate()

# Création d'un DataFrame de test
data = [
    ("  Alice   ", "  Paris, France  "),
    ("  bob  ", "Londres, Royaume-Uni")
]
columns = ["nom", "adresse"]
df = spark.createDataFrame(data, columns)

print("DataFrame initial :")
df.show(truncate=False)

# 1. Nettoyage des données : supprimer les espaces et mettre en majuscules
df_clean = df.withColumn("nom_propre", trim(initcap(col("nom")))) \
             .withColumn("adresse_propre", trim(col("adresse")))

print("DataFrame après nettoyage :")
df_clean.show(truncate=False)

# 2. Extraction et découpage de l'adresse
df_split = df_clean.withColumn("ville", split(col("adresse_propre"), ",")[0]) \
                   .withColumn("pays", split(col("adresse_propre"), ",")[1])

print("DataFrame avec ville et pays extraits :")
df_split.show(truncate=False)

# 3. Utilisation d'instr() pour trouver une sous-chaîne et de concat()
df_final = df_split.withColumn(
    "contient_france",
    instr(col("adresse_propre"), "France") >= 1
).withColumn(
    "info_complete",
    concat(col("nom_propre"), lit(" vit à "), col("ville"))
)

print("DataFrame final avec colonnes créées :")
df_final.show(truncate=False)
df_final.printSchema()

spark.stop()