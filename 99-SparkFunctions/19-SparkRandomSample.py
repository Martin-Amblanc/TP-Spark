from pyspark.sql import SparkSession

# Création d'une session Spark
spark = SparkSession.builder.appName("19-SparkRandomSamples.py").getOrCreate()

# Création d'un DataFrame de 1000 lignes
data = [(i,) for i in range(1000)]
df = spark.createDataFrame(data, ["id"])

# Afficher le nombre total de lignes pour référence
print(f"Nombre total de lignes dans le DataFrame initial : {df.count()}\n")

# --- Échantillon 1 : Sans remplacement ---
# withReplacement=False (valeur par défaut)
# fraction=0.10 (environ 10% des lignes)
# seed=42 (pour garantir la reproductibilité)
sample_sans_remplacement = df.sample(withReplacement=False, fraction=0.10, seed=42)
print("Échantillon sans remplacement (environ 10% des lignes) :")
print(f"Nombre de lignes dans l'échantillon : {sample_sans_remplacement.count()}")
sample_sans_remplacement.show(10)

# --- Échantillon 2 : Avec remplacement ---
# withReplacement=True
# fraction=0.10 (environ 10% des lignes, mais avec possibilité de doublons)
# seed=42 (même graine pour la reproductibilité)
sample_avec_remplacement = df.sample(withReplacement=True, fraction=0.10, seed=42)
print("\nÉchantillon avec remplacement (peut contenir des doublons) :")
print(f"Nombre de lignes dans l'échantillon : {sample_avec_remplacement.count()}")
sample_avec_remplacement.show(10)

# Arrêt de la session Spark
spark.stop()