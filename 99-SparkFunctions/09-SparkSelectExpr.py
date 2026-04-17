from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Création de la session et du DataFrame de test
spark = SparkSession.builder.appName("9-SparkSelectExpr.py").getOrCreate()
data = [("Alice", 25, "New York"), ("Bob", 32, "London"), ("Charlie", 35, "Paris")]
columns = ["name", "age", "city"]
df = spark.createDataFrame(data, columns)

print("DataFrame original :")
df.show()

### Exemple 1 : Utilisation de `select`
# On sélectionne les colonnes, on en renomme une et on crée une nouvelle colonne
# avec une logique conditionnelle.
df_select = df.select(
    col("name"),
    col("age").alias("age_du_client"),
    when(col("age") > 30, lit("Adulte")).otherwise(lit("Jeune")).alias("categorie_age")
)

print("Résultat avec select :")
df_select.show()

### Exemple 2 : Utilisation de `selectExpr`
# On fait les mêmes opérations mais avec la syntaxe de chaîne de caractères
df_select_expr = df.selectExpr(
    "name",
    "age as age_du_client",
    "CASE WHEN age > 30 THEN 'Adulte' ELSE 'Jeune' END as categorie_age"
)

print("Résultat avec selectExpr :")
df_select_expr.show()

# Arrêt de la session Spark
spark.stop()
