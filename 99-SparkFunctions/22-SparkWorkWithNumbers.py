from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, when

# Création d'un DataFrame de test
spark = SparkSession.builder.appName("22-SparkWorkWithNumbers.py").getOrCreate()
df = spark.createDataFrame([(10, 2), (20, 5), (15, 3)], ["quantite", "prix_unitaire"])

# Opérations de base et agrégation
df_calculs = df.withColumn("prix_total_ht", col("quantite") * col("prix_unitaire"))
df_calculs.show()

# Conversion de type et arrondi pour un prix TTC avec 20% de TVA
df_final = df_calculs.withColumn(
    "prix_total_ttc",
    round(col("prix_total_ht") * 1.20, 2).cast("decimal(10, 2)")
)

df_final.show()
df_final.printSchema()

# Agrégation (somme du prix total HT)
total_ht = df_final.agg(sum("prix_total_ht")).collect()[0][0]
print(f"Le prix total HT de toutes les commandes est de {total_ht}.")

spark.stop()