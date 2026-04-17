#-----------------
#--- unpersist ---
#-----------------

from pyspark.sql import SparkSession

# Créez une session Spark
spark = SparkSession.builder.appName("30-SparkWorkWithDataframe.py").getOrCreate()
print(f"---Début unpersist exemple")

# Créez un DataFrame et mettez-le en cache
df_unpersist = spark.range(100).cache()

# L'action .count() force l'exécution et met les données en cache
df_unpersist.count()
print(f"1.1) Le DataFrame est-il en cache ? {df_unpersist.is_cached}")

# Libérez la mémoire de ce DataFrame
df_unpersist.unpersist()

print(f"1.2) Le DataFrame est-il en cache après unpersist() ? {df_unpersist.is_cached}")
print(f"---Fin unpersist exemple\n")

spark.stop()

#-----------------------------
#--- spark.range().cache() ---
#-----------------------------

from pyspark.sql import SparkSession

# Créez une session Spark
spark = SparkSession.builder.appName("30-SparkWorkWithDataframe.py").getOrCreate()
print(f"---Début clearCache exemple")

# Créez et mettez en cache deux DataFrames
df_1 = spark.range(10).cache()
df_2 = spark.range(20).cache()

# Les actions forcent le cache
df_1.count()
df_2.count()

print(f"2.1) df_1 est en cache ? {df_1.is_cached}")
print(f"2.2) df_2 est en cache ? {df_2.is_cached}")

# Videz tout le cache de la session
print(f"---Videz tout le cache de la session")
spark.catalog.clearCache()

print(f"2.3) df_1 est en cache après clearCache() ? {df_1.is_cached}")
print(f"2.4) df_2 est en cache après clearCache() ? {df_2.is_cached}")

print(f"---Fin clearCache exemple\n")

spark.stop()

#-----------
#--- del ---
#-----------

from pyspark.sql import SparkSession

# Créez une session Spark
spark = SparkSession.builder.appName("DelExample").getOrCreate()
print(f"---Début Del exemple")

# Créez un DataFrame et mettez-le en cache
df_del = spark.range(100).cache()
df_del.count()

print(f"3.1) df_del est en cache ? {df_del.is_cached}")

# Supprimez la référence Python
del df_del

# Le DataFrame n'est plus accessible en Python, mais les données peuvent rester en cache sur Spark
# Si vous essayez de l'appeler, vous obtiendrez une NameError
try:
    print(df_del.is_cached)
except NameError as e:
    print(f"3.2) Erreur : {e}. La variable df_del a été supprimée.")

print(f"---Fin Del exemple")

spark.stop()