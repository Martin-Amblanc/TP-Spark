from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("15-SparkRemovingColumn.py").getOrCreate()

# Création d'un DataFrame de base
df = spark.createDataFrame([("Pomme", 1.5), ("Banane", 0.8)], ["fruit", "prix_ht"])

# Ajout de colonnes de littéraux
df_avec_litteraux = df.withColumn("monnaie", lit("EUR")) \
                      .withColumn("taux_tva", lit(0.20))

# Ajout d'une colonne 'prix_ttc' en utilisant une expression
df_avec_ttc = df_avec_litteraux.withColumn("prix_ttc", expr("prix_ht * (1 + taux_tva)"))
df_avec_ttc.show()

# Renommage d'une colonne avec withColumnRenamed
df_renomme = df_avec_ttc.withColumnRenamed("fruit", "nom_du_fruit")

# Suppression de la colonne "prix_ht"
df_sans_prix_ht = df_avec_ttc.drop("prix_ht")
df_sans_prix_ht.show()

# Arrêt de la session Spark
spark.stop()

