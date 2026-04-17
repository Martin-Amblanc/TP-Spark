from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("13-SparkReservedCharacters.py").getOrCreate()

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

# Utilisation de l'alias() avec select
df_avec_alias = df.select(df.fruit.alias("nom_du_fruit"), df.prix_ht)

# Exemple 1 : Pas de caractères spéciaux, pas besoin d'échappement
df_no_escape = df.withColumn("prix-en-euros", expr("prix_ht"))
df_no_escape.show()

# Exemple 2 : Caractères réservés, échappement nécessaire avec ` 
# car le tiret pourrait être interprété comme un moins
df_with_escape = df_no_escape.withColumn("prix-total", expr("`prix-en-euros` * 1.2"))
# Affichage sans troncature
df_with_escape.show(df_with_escape.count(), False)

# Arrêt de la session Spark
spark.stop()

