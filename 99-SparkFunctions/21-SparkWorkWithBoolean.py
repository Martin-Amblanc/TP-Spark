#*****************************************
# *** 21-SparkWorkWithBooleanSimple.py ***
#*****************************************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Création d'une session Spark
spark = SparkSession.builder.appName("21-SparkWorkWithBooleanSimple.py").getOrCreate()

# Création d'un DataFrame de base
df = spark.createDataFrame([
    ("Alice", 25, 50000),
    ("Bob", 32, 75000),
    ("Charlie", 35, 90000),
    ("Diana", 28, 60000)
], ["name", "age", "salary"])

# Création d'une colonne booléenne pour savoir si une personne a plus de 30 ans
df_avec_booleen = df.withColumn(
    "est_plus_de_30_ans",
    col("age") > 30
)

# Utilisation du booléen dans une expression conditionnelle avec when()
df_final = df_avec_booleen.withColumn(
    "categorie_salaire",
    when(col("est_plus_de_30_ans"), "Senior").otherwise("Junior")
)

df_final.show()

spark.stop()

#*******************************************
# *** 21-SparkWorkWithBooleanComplexe.py ***
#*******************************************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, instr

# Création d'une session Spark
spark = SparkSession.builder.appName("21-SparkWorkWithBoolean.py").getOrCreate()

# Données de test
# Row 1: StockCode = DOT, UnitPrice > 600 -> isExpensive = True
# Row 2: StockCode = DOT, Description contient POSTAGE -> isExpensive = True
# Row 3: StockCode = DOT, mais ni le prix ni la description ne correspondent -> isExpensive = False
# Row 4: StockCode != DOT -> isExpensive = False
data = [
    ("DOT", 650, "Ceci est un article de qualité"),
    ("DOT", 500, "FRAIS DE PORTAGE"),
    ("DOT", 50, "Petit article"),
    ("ABC", 1000, "Autre article")
]
columns = ["StockCode", "UnitPrice", "Description"]
df = spark.createDataFrame(data, columns)
df.show()

# Définition des filtres booléens
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1

# Création de la colonne booléenne complexe 'isExpensive'
df_with_filter = df.withColumn(
    "isExpensive",
    DOTCodeFilter & (priceFilter | descripFilter)
)
df_with_filter.show()

# Filtrage du DataFrame pour ne conserver que les lignes où 'isExpensive' est True
df_final = df_with_filter.where("isExpensive")
df_final.show()

spark.stop()