from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Création d'une session Spark
spark = SparkSession.builder.appName("7-SparkExpression.py").getOrCreate()

# Création d'un DataFrame de test
print(f"Création d'un DataFrame de test")
data = [("Alice", 25, "F"), ("Bob", 30, "M"), ("Charlie", 35, "M")]
columns = ["name", "age", "gender"]
df = spark.createDataFrame(data, columns)
df.show()

# Utilisation de expr() pour ajouter une nouvelle colonne
# L'expression "age + 5" est passée sous forme de chaîne de caractères
print(f"L'expression ""age + 5"" est passée sous forme de chaîne de caractères")
df_plus_5 = df.withColumn("age_plus_5", expr("age + 5"))
df_plus_5.show()

# Utilisation de expr() pour une condition plus complexe
# L'expression "CASE WHEN age >= 30 THEN 'Adulte' ELSE 'Jeune' END" est passée
print(f"L'expression ""CASE WHEN age >= 30 THEN 'Adulte' ELSE 'Jeune' END"" est passée")
df_category = df_plus_5.withColumn("category", expr("CASE WHEN age >= 30 THEN 'Adulte' ELSE 'Jeune' END"))
df_category.show()

# Arrêt de la session Spark
spark.stop()