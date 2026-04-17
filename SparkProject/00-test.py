from pyspark.sql import SparkSession
import time

print(">>> INITIALISATION DE LA SESSION SPARK...")

spark = SparkSession.builder \
    .appName("Test Installation") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# On demande à Spark de se taire et de ne montrer que les ERREURS
spark.sparkContext.setLogLevel("ERROR")

print(">>> CONNEXION RÉUSSIE !")
print(f">>> VERSION DE SPARK : {spark.version}")

# Création d'une petite donnée pour tester le calcul
data = [("Installation", 1), ("Terminé", 2)]
df = spark.createDataFrame(data, ["Nom", "ID"])
df.show()

print(">>> LE SCRIPT EST TERMINÉ....")
spark.stop()