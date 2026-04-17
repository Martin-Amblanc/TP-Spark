import os
from mega import Mega

#----- Téléverser un fichier Parquet -----

def get_or_create_mega_folder_id(mega_instance, path):
    """
    Recherche ou crée un dossier sur Mega et retourne son Node ID.
    """
    try:
        # Tente de trouver le dossier
        dossier_node_list = mega_instance.find(path)
        if dossier_node_list:
            print(f"Dossier '{path}' trouvé sur Mega sous l'Id '{dossier_node_list[0]}'")
            return dossier_node_list[0]
        else:
            # Si le dossier n'est pas trouvé, le créer
            print(f"Dossier '{path}' non trouvé. Création en cours...")
            dossier_node_id = mega_instance.create_folder(path)
            print(f"Dossier '{path}' créé sous l'Id '{dossier_node_id}'")
            dossier_node_list = mega_instance.find(path)
            return dossier_node_list[0]
    except Exception as e:
        print(f"Erreur lors de la recherche/création du dossier Mega : {e}")
        return None

def get_mega_node_id(m, path):
    """
    Récupère le Node ID d'un dossier sur Mega en gérant les différents formats de retour.
    """
    resultat = m.find(path)
    
    if not resultat:
        return None
    
    if isinstance(resultat, list):
        resultat = resultat[0]
        
    if isinstance(resultat, tuple):
        return resultat[0]
    else:
        return resultat

# La fonction de téléchargement avec réessai reste la même
def upload_avec_reessai(m, chemin_complet, destination, tentatives=3, delai=5):
    for i in range(tentatives):
        try:

            m.upload(chemin_complet, dest=destination)
            return True
        except Exception as e:
            print(f"Échec du upload (tentative {i+1}/{tentatives}): {e}")
            if i < tentatives - 1:
                print(f"Attente de {delai} secondes avant de réessayer...")
                time.sleep(delai)
            else:
                return False
    return False

#-------------------------------------
#----- Upload un fichier Parquet -----
#------------------------------------

def upload_spark_output_to_mega(directory_path, mega_email, mega_password, mega_destination_path):
    """
    Téléverse tous les fichiers d'un dossier donné sur Mega.
    """
    if not os.path.isdir(directory_path):
        print(f"Erreur : Le chemin '{directory_path}' n'est pas un dossier ou n'existe pas.")
        return

    try:
        mega = Mega()
        m = mega.login(mega_email, mega_password)
        print("Connexion à Mega réussie.")

        # Utilisation de la nouvelle fonction pour obtenir l'ID du dossier de destination
        dossier_mega_id = get_or_create_mega_folder_id(m, mega_destination_path)
        if not dossier_mega_id:
            print("Impossible de trouver ou de créer le dossier de destination sur Mega. Annulation.")
            return

        print(f"Début du téléversement des fichiers vers le dossier Mega Node ID: {dossier_mega_id}")
        
        for nom_fichier in os.listdir(directory_path):
            chemin_complet_du_fichier = os.path.join(directory_path, nom_fichier)

            if nom_fichier.startswith('.') or nom_fichier.startswith('_'):
                print(f"Ignoré : '{nom_fichier}' (fichier de métadonnées ou temporaire).")
                continue

            if os.path.isfile(chemin_complet_du_fichier):
                print(f"Téléversement de '{nom_fichier}'...")
                # Passage de l'ID du dossier (dossier_mega_id) à l'argument 'dest'
                if upload_avec_reessai(mega, chemin_complet_du_fichier, dossier_mega_id):
                    print(f"'{nom_fichier}' a été téléversé avec succès.")
            else:
                print(f"'{nom_fichier}' est un dossier, ignoré.")
                
        print("\nTous les fichiers du dossier ont été téléversés.")

    except Exception as e:
        print(f"\nUne erreur est survenue lors du téléversement : {e}")

#-----------------------------------------
#----- Downloader un fichier Parquet -----
#-----------------------------------------

import os
import time
from mega import Mega

def telecharger_avec_reessai(m, FileNode, chemin_local_final, tentatives=3, delai=5):
    """
    Tente de télécharger un fichier avec la fonction m.download() avec plusieurs essais.
    """
    for i in range(tentatives):
        try:
            # Téléchargement du fichier.
            # print(f"fileName = {fileName}")
            m.download(FileNode, chemin_local_final)
            return True # Succès
        except Exception as e:
            print(f"Échec du téléchargement (tentative {i+1}/{tentatives}): {e}")
            if i < tentatives - 1:
                print(f"Attente de {delai} secondes avant de réessayer...")
                time.sleep(delai)
            else:
                return False # Échec final
    return False

def telecharger_fichiers_de_mega(dossier_mega, dossier_local, mega_email, mega_password):
    """
    Télécharge tous les fichiers d'un dossier Mega en utilisant la méthode de téléchargement standard.
    """
    try:
        mega = Mega()
        m = mega.login(mega_email, mega_password)
        print("Connexion à Mega réussie.")

        os.makedirs(dossier_local, exist_ok=True)
        print(f"Dossier de destination local '{dossier_local}' créé ou existant.")

        dossier_mega_node = get_mega_node_id(m, dossier_mega)
        print 
        
        if not dossier_mega_node:
            print(f"Erreur: Le dossier '{dossier_mega}' n'a pas été trouvé sur Mega. Le téléchargement ne peut pas continuer.")
            return
        
        # On utilise m.find() pour obtenir les fichiers du dossier
        fichiers_mega = m.get_files_in_node(dossier_mega_node)
        print(f"Téléchargement des fichiers depuis le dossier Mega '{dossier_mega}'...")


        for fichier_id, fichier_info in fichiers_mega.items():
            if fichier_info['t'] == 0:
                nom_fichier_original = fichier_info['a']['n']
                chemin_local_final = os.path.join(dossier_local, nom_fichier_original)
                
                print(f"Début du téléchargement de '{nom_fichier_original}' vers '{chemin_local_final}'..")
                
                tuple_fichier = (fichier_id, fichier_info)
                # print(f"tuple_fichier = '{tuple_fichier}'")
                            
                # On utilise la fonction m.download() qui attend l'objet fichier_info
                if telecharger_avec_reessai(m, tuple_fichier, dossier_local):
                    print(f"'{nom_fichier_original}' a été téléchargé avec succès.")
                else:
                    print(f"Échec final du téléchargement de '{nom_fichier_original}'. Ignoré.")
            
        print("\nTous les fichiers du dossier ont été traités.")

    except Exception as e:
        print(f"\nUne erreur est survenue lors de l'opération : {e}")
