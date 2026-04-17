from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr

# ---------------------------------------------------------------
# ---  Création d'une sessions spark Insensitive (par défaut) ---
# ---------------------------------------------------------------

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("14-SparkCaseSensitive.py").getOrCreate()

# Création d'un DataFrame de base
df = spark.createDataFrame([("Pomme", 1.5), ("Banane", 0.8)], ["fruit", "prix_ht"])

# Ajout de colonnes de littéraux
df_avec_litteraux = df.withColumn("monnaie", lit("EUR")) \
                      .withColumn("taux_tva", lit(0.20))

# Spark est insensible à la casse par défaut
df.select("fruit", "FRUIT").show() 
# Affiche la même colonne deux fois

# Arrêt de la session Spark
spark.stop()

# ----------------------------------------------------------
# ---  Création d'une sessions spark Sensitive (Erreur)  ---
# ----------------------------------------------------------

# Création de la session et du DataFrame de test
spark = SparkSession.builder \
    .config("spark.sql.caseSensitive", "true") \
    .appName("CaseSensitiveExample") \
    .getOrCreate()

# Création d'un DataFrame de base
df = spark.createDataFrame([("Pomme", 1.5), ("Banane", 0.8)], ["fruit", "prix_ht"])

# Ajout de colonnes de littéraux
df_avec_litteraux = df.withColumn("monnaie", lit("EUR")) \
                      .withColumn("taux_tva", lit(0.20))

# Spark est insensible à la casse par défaut
df.select("fruit", "FRUIT").show() 
# Affiche la même colonne deux fois

# Arrêt de la session Spark
spark.stop()


