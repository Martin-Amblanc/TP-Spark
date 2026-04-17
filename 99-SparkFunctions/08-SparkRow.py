from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import expr


# Création d'une session Spark
spark = SparkSession.builder.appName("8-SparkRow.py").getOrCreate()

# Création d'un DataFrame à partir de Rows
row_data = [
    Row(nom="pomme", couleur="rouge", prix=1.2),
    Row(nom="banane", couleur="jaune", prix=0.8),
    Row(nom="orange", couleur="orange", prix=1.5)
]

# Notez que Spark infère le schéma ici.
print(f"Création d'un DataFrame à partir de Rows")
df = spark.createDataFrame(row_data)

# Affichage du DataFrame et de son schéma
print(f"Affichage du DataFrame et de son schéma")
df.show()
df.printSchema()

# Utilisation de expr() pour calculer le Prix TTC
# On utilise la formule Prix HT * (1 + 20%)
# On ajoute également le Prix HT pour la comparaison
df_with_ttc = df.withColumn(
    "prix_ht",
    expr("prix")
).withColumn(
    "prix_ttc",
    expr("CAST(prix * 1.2 AS DECIMAL(10, 2))")
)

print("DataFrame avec Prix HT et TTC :")
df_with_ttc.show()

# Arrêt de la session Spark
spark.stop()