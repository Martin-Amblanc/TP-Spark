import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random
from datetime import datetime

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("LotoSparkStreaming") \
    .master("local[*]") \
    .getOrCreate()

# Pour éviter que la console soit polluée par les messages INFO de Spark
spark.sparkContext.setLogLevel("ERROR")

# ==========================================
# B1) Développer une carte de Loto à 16 chiffres
# ==========================================
# Génération de 16 chiffres aléatoires uniques entre 1 et 99
grille = random.sample(range(1, 100), 16)
numeros_restants = set(grille) # Un set (ensemble) pour faciliter la suppression des numéros trouvés

print("\n========== MA GRILLE DE LOTO ==========")
print(f"{grille[0:4]}")
print(f"{grille[4:8]}")
print(f"{grille[8:12]}")
print(f"{grille[12:16]}")
print("=======================================\n")
print("En attente du tirage...\n")

# ==========================================
# B2) Développer 1 flux stream
# ==========================================
# 1. Définir le schéma attendu dans les fichiers CSV
schema = StructType([
    StructField("numero", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# 2. Lire le dossier "tirage" en mode Streaming
df_stream = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load("tirage")

# Variable globale pour savoir si on a gagné et arrêter les prints
a_gagne = False

# 3. Fonction pour traiter chaque nouveau numéro (micro-batch)
def valider_grille(batch_df, batch_id):
    global numeros_restants, a_gagne
    
    if a_gagne:
        return # Si on a déjà gagné, on ignore la suite
        
    # On récupère les numéros de ce micro-batch sous forme de liste Python
    tirages = batch_df.select("numero").rdd.flatMap(lambda x: x).collect()
    
    for num in tirages:
        if num is not None and num in numeros_restants:
            numeros_restants.remove(num)
            print(f"🟢 BIEN JOUÉ ! Le {num} est dans ta grille ! (Reste {len(numeros_restants)} numéro(s) à trouver)")
            
            # Vérification de la victoire
            if len(numeros_restants) == 0 and not a_gagne:
                a_gagne = True
                heure = datetime.now().strftime("%H:%M:%S")
                print("\n" + "*"*40)
                print(f"🏆 GAGNE à {heure} 🏆")
                print("*"*40 + "\n")
                break

# 4. Démarrer l'écriture/traitement du flux
query = df_stream.writeStream \
    .foreachBatch(valider_grille) \
    .start()

query.awaitTermination()