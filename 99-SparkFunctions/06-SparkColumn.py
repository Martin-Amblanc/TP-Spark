from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

# Création d'une session Spark
spark = SparkSession.builder.appName("6-SparkColumn.py").getOrCreate() 

# Création d'un DataFrame de test 
print(f"Création d'un DataFrame de test")
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)] 
columns = ["name", "age"] 
df = spark.createDataFrame(data, columns)
df.show() 

# Utilisation de col() pour filtrer une colonne 
# On filtre les lignes où l'âge est supérieur à 30
print(f"On filtre les lignes où l'âge est supérieur à 30")
df_filtered = df.filter(col("age") > 30) 
df_filtered.show() 

# Utilisation de col() dans une opération de sélection
# On sélectionne la colonne 'name' et on ajoute 1 à la colonne 'age'
print(f"On sélectionne la colonne 'name' et on ajoute 1 à la colonne 'age'")
df_new = df.select(col("name"), col("age") + 1)
df_new.show()

# Arrêt de la session Spark
spark.stop()
