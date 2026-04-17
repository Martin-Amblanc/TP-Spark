from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace

# Création d'une session Spark
spark = SparkSession.builder.appName("24-SparkWorkWithRegularExpression.py").getOrCreate()

# Création d'un DataFrame de test
data = [
    ("10 rue de la Liberté, 75001 Paris", "Contact: 06-12-34-56-78"),
    ("Avenue Charles de Gaulle, 69002 Lyon", "Contact: 07.89.01.23.45")
]
columns = ["adresse", "telephone"]
df = spark.createDataFrame(data, columns)

print("DataFrame initial :")
df.show(truncate=False)

# 1. Extraction du code postal
# On cherche 5 chiffres consécutifs
df_extract = df.withColumn(
    "code_postal",
    regexp_extract(col("adresse"), r"(\d{5})", 1)
)

# 2. Remplacement pour normaliser le numéro de téléphone
# On remplace les tirets et les points par des espaces
df_final = df_extract.withColumn(
    "telephone_propre",
    regexp_replace(col("telephone"), r"[-.]", " ")
)

print("DataFrame final avec extraction et remplacement :")
df_final.show(truncate=False)

spark.stop()