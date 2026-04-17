from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("10-SparkLit.py").getOrCreate()

# Création d'un DataFrame de base
df = spark.createDataFrame([("Pomme", 1.5), ("Banane", 0.8)], ["fruit", "prix_ht"])

# Ajout de colonnes de littéraux
df_avec_litteraux = df.withColumn("monnaie", lit("EUR")) \
                      .withColumn("taux_tva", lit(0.20))
                      
df_avec_litteraux.show()

# Arrêt de la session Spark
spark.stop()

