import os
import random
import time
from datetime import datetime

# A) Création du dossier « tirage » s'il n'existe pas
output_dir = "tirage"
os.makedirs(output_dir, exist_ok=True)

# Préparation des chiffres de 1 à 99
numeros = list(range(1, 100)) # Va de 1 à 99
random.shuffle(numeros) # Mélange pour faire un tirage aléatoire SANS REMISE

print("Début du tirage du Loto (1 numéro toutes les 5 secondes)...")

# Boucle pour tirer un numéro toutes les 5 secondes
for i, numero in enumerate(numeros):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # On crée un fichier unique pour chaque tirage pour que Spark le lise
    filename = os.path.join(output_dir, f"tirage_{i}.csv")
    
    with open(filename, "w") as f:
        f.write("numero,timestamp\n") # En-tête
        f.write(f"{numero},{timestamp}\n")
    
    print(f"Boule tirée : {numero}")
    time.sleep(5) # Attente de 5 secondes

print("Le tirage complet est terminé.")