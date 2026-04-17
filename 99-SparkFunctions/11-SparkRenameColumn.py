from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("11-SparkRenameColumn.py").getOrCreate()

# Création d'un DataFrame de base
df = spark.createDataFrame([("Pomme", 1.5), ("Banane", 0.8)], ["fruit", "prix_ht"])

# Ajout de colonnes de littéraux
df_avec_litteraux = df.withColumn("monnaie", lit("EUR")) \
                      .withColumn("taux_tva", lit(0.20))

# Ajout d'une colonne 'prix_ttc' en utilisant une expression
df_avec_ttc = df_avec_litteraux.withColumn("prix_ttc", expr("prix_ht * (1 + taux_tva)"))
df_avec_ttc.show()

# Arrêt de la session Spark
spark.stop()

