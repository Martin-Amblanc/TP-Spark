import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, substr, count, avg
print("--------------------------------- Création de la session Spark --------------------------")
spark = SparkSession.builder \
    .appName("PySpark read 1 file")\
    .getOrCreate()
print("----------------------------------------")

DEST_FILENAME = 'Auteurs.txt'

if os.path.exists(DEST_FILENAME):
    print(f"Lecture du fichier '{DEST_FILENAME}' avec Spark ...")
    df= spark.read.format("csv").option("header","true").option("delimiter",";").load(DEST_FILENAME)
else :
    print(f"le fichier '{DEST_FILENAME}' n'existe pas ou n'a pas été téléchargé")

#print("Exemple de la clause col et alias")
#df_new = df.select(col("auteur_id").alias("ID"), col("nom"))
#df_new.show()



#print ("Exemple de clause filter / where")

#df_new1 = df.filter(df["auteur_id"] < 10)
#df_new2 = df.where(df["auteur_id"] >= 10)
#df_new1.show()
#df_new2.show()

#print ("Ajout d'une nouvelle colonne")
#df_new3 = df.withColumn("Sexe", lit("Homme"))
#df_new3.show()

#print ("Filtre ")
#df_new4 = df.withColumn("FirtsLetter", df.nom.substr(2,1)).groupby("FirtsLetter").agg(count(df.auteur_id).alias("Nombre d'auteur"))
#df_new4.show()


# print(f"-------Exemple : Compter le nombre d'individus-----------")
# df_new5 = df.withColumn("firstLetter", df.nom.substr(2,1)) \
#     .groupBy("firstLetter") \
#     .agg(count(df.auteur_id).alias("Nombre_personnes")) \
#     .orderBy("firstLetter") \
#     .show()

# print(f"-------Exemple : Ajout d'un individus via UNION-----------")
# df_new_data = spark.createDataFrame([(15, "King", "Stephen")], ["auteur_id", "nom", "prenom"])
# df_combined = df.union(df_new_data) \
#     .show()

print(f"-------Exemple : Jointure de DataFrame-----------")

# Ajout d'une nouvelle bibliothèque
DEST_FILENAME2 = 'Livres.txt'

if os.path.exists(DEST_FILENAME2):
    print(f"Lecture du fichier '{DEST_FILENAME2}' avec Spark ...")
    dfl = spark.read.format("csv").option("header","true").option("delimiter",";").load(DEST_FILENAME2)
else :
    print(f"le fichier '{DEST_FILENAME2}' n'existe pas ou n'a pas été téléchargé")
# jointure avec la fonction Join de Spark
# dfl_joined = dfl.join(df, on="auteur_id", how="inner")
# print(f"\n Résultat de la jointure interne (INNER JOIN) avec la fonction join de spark (partition : {dfl_joined.rdd.getNumPartitions()}) :")
# dfl_joined.show()

# jointure avec SQL
df.createOrReplaceTempView("auteurs")
dfl.createOrReplaceTempView("livres")

# jointure interne en inner join
sql_inner_join = "SELECT livre_id, titre, nom, prenom FROM livres INNER JOIN auteurs ON auteurs.auteur_id = livres.auteur_id"
df_result_inner = spark.sql(sql_inner_join)
print(f"------ Résultat de la jointure avec 2 vue temporaire ( partition {df_result_inner.rdd.getNumPartitions()}) :")
df_result_inner.show()

# Ecrire le résultat de l'inner join dans une partition à 5 cluster

df_result_inner5 = spark.sql(sql_inner_join).repartition(5)
print(f"------ Résultat de la jointure avec 2 vue temporaire ( partition {df_result_inner5.rdd.getNumPartitions()}) :")
#df_result_inner5.show()

dossier_local_destination = "resultats/Livres_Auteurs"
os.makedirs(dossier_local_destination, exist_ok=True)

df_result_inner5.write.mode("overwrite").parquet(dossier_local_destination)

#4 Listez le dossier de fichiers Parquet
try:
    df_lu = spark.read.parquet(dossier_local_destination)
    print("DataFrame lu avec succès à partir des fichiers Parquet :")
    df_lu.show()
    df_lu.printSchema()

except Exception as e:
    print(f"Erreur lors de la lecture des fichier Parquet : {e}")

spark.stop()