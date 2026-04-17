import os
import sys
from pyspark.sql import SparkSession
import glob, datetime, time
from pyspark.sql.functions import col, lit, substr, count, avg, window, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
print("--------------------------------- Création de la session Spark --------------------------")
spark = SparkSession.builder \
    .appName("exo2.py")\
    .getOrCreate()
print("="*40)


# ----- lecture simple d'un dossier -----

print("-"*35)
print("--- 1 Lecture simple d'un dossier 1 passage ----")
print("-"*35)

spark.conf.set("spark.sql.shuffle.partitions", "5")

heure_debut = datetime.datetime.now()
print("1.1 Chargment des fichiers ----")
file_paths = glob.glob("data/retail-data/by-day/*.csv*", recursive=True)
staticDataFrame = spark.read.csv(file_paths, header=True, inferSchema=True)
print("-"*35)
print("1.2 Création d'une vue temporaire")
staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

print("-"*35)
print("1.3.CA des Magasins par Client ---")
staticDataFrame\
    .selectExpr(
        "CustomerId",
        "CAST(InvoiceDate AS DATE) as InvoiceDay",
        "(UnitPrice * Quantity) as total_cost")\
    .filter("CustomerId IS NOT NULL")\
    .groupBy(
        col("CustomerId"), col("InvoiceDay"))\
    .sum("total_cost")\
    .sort(desc("sum(total_cost)"))\
    #.show()
duree = datetime.datetime.now() - heure_debut
print(f"\nLe code a mis {duree} pour s'exécuter.")

print("="*40)
print("----- 2.1 Refaison le même calcul avec un stream ----")
print("="*40)

heure_debut = datetime.datetime.now()
print("2.1 Création de la définition de structure de fichier ----- ")

# Définition de la structure
StaticSchema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", DoubleType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerId", DoubleType(), True),
    StructField("Country", StringType(), True)
])
print("2.2 Création de la définition du Stream ----- ")
StreamingDataFrame = spark.readStream \
    .schema(StaticSchema) \
    .option("maxFilesPerTrigger", 20) \
    .option("header", "true") \
    .format("csv") \
    .load("data/retail-data/by-day/*.csv")

print("2.3 Création de la définition de la requete d'interrogation du CA des Magasins par client ----- ")

PurchaseByCustomerPerHour = StreamingDataFrame \
.selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate") \
    .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
    .sum("total_cost")\
    .sort(desc("sum(total_cost)"))\

print("2.4 Appelons une opération afin d'exécuter les 3 actions paresseuses ----- ")
query = PurchaseByCustomerPerHour.writeStream \
    .format("memory") \
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()

time.sleep(30)
print("Le flux de streaming est en cours d'exécution. Ctrl+C pour l'arrêter.")
#Stop

heure_start = time.time()
duree_max_secondes = 600

print("les résultates seront affichés pendnat 10 minutes")

while (time.time() - heure_start) < duree_max_secondes:
    print(f"\n--- Affichage des résultats à {datetime.datetime.now().strftime(' %H:%M:%S')} ---")
    spark.sql("""SELECT * FROM customer_purchases ORDER BY 'sum(total_cost)';""").show(10)
    time.sleep(30)

query.awaitTermination(timeout=300)
spark.stop()