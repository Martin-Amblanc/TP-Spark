from pyspark.sql import SparkSession

# Création d'une session Spark
spark = SparkSession.builder.appName("20-SparkRandomSplit.py").getOrCreate()

# Création d'un DataFrame simple de 1000 lignes
data = [(i,) for i in range(1000)]
df = spark.createDataFrame(data, ["id"])

# Division du DataFrame en 3 sous-ensembles aléato#ires
# Les poids sont 0.8, 0.1, 0.1. Le total est 1.0, mais ce n'est pas obligatoire.
# une partie pour l'entraînement (80%), la validation (10%) et le test (10%).
train_df, validation_df, test_df = df.randomSplit([0.8, 0.1, 0.1], seed=42)

# Affichage du nombre de lignes dans chaque sous-ensemble
print(f"Total de lignes : {df.count()}")
print(f"Lignes pour l'entraînement : {train_df.count()}")
print(f"Lignes pour la validation : {validation_df.count()}")
print(f"Lignes pour le test : {test_df.count()}")

# Affichage des premières lignes de l'ensemble d'entraînement sans troncature
print("\nPremières lignes de l'ensemble d'entraînement :")
train_df.show(5, False)

# Arrêt de la session Spark
spark.stop()