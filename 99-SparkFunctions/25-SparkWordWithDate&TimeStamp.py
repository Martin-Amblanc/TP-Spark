from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, col, to_timestamp, year, month, dayofmonth, date_add, datediff, lit, last_day

# Création d'une session Spark
spark = SparkSession.builder.appName("25-SparkWordWithDate&TimeStamp.py").getOrCreate()

# Création d'un DataFrame de test avec des chaînes de caractères
data = [("2023-01-15 10:30:22",), ("2023-02-20 15:15:15",), ("2023-04-25 20:17:32",), ("2023-04-31 20:17:32",)]
columns = ["event_time_str"]
df = spark.createDataFrame(data, columns)

print("DataFrame initial avec chaîne de caractères :")
df.show(truncate=False)
df.printSchema()

# 1. Convertir la chaîne en un type Timestamp
df_converted = df.withColumn("date_du_jour", lit(current_date())) \
                 .withColumn("timestamp_actuel", lit(current_timestamp())) \
                 .withColumn("event_time", to_timestamp(col("event_time_str")))

# 2. Extraire les parties de la date
df_extracted = df_converted.withColumn("annee", year(col("event_time"))) \
                           .withColumn("mois", month(col("event_time"))) \
                           .withColumn("jour", dayofmonth(col("event_time")))

# 3. Effectuer un calcul : ajouter 30 jours à la date
df_with_calc = df_extracted.withColumn("date_plus_30_jours", date_add(col("event_time"), 30)) \
                           .withColumn("date_moins_30_jours", date_add(col("event_time"), -30))

# 4. Calculer la différence entre une date et une date fixe
df_final = df_with_calc.withColumn("jours_depuis_debut", datediff(col("event_time"), lit("2023-01-01"))) \
                       .withColumn("dernier jour du mois", last_day(col("event_time")))

print("DataFrame final après toutes les transformations :")
df_final.show(truncate=False)
df_final.printSchema()

spark.stop()