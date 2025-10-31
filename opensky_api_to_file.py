import json
import pandas as pd
import time
import random
import os
import requests
import subprocess
from datetime import datetime

# === PARAMÈTRES GLOBAUX ===
BASE_PATH = r"C:\Users\JC\Documents\Sup_de_vinci\M2\Donnees_distribuees\kafka-installation"
INPUT_FILE = os.path.join(BASE_PATH, "fichier_all", "all.json")
PREV_FILE = os.path.join(BASE_PATH, "fichier_all", "n-1.json")

CONTAINER_NAME = "nifi"
DOCKER_DEST_PATH = "/tmp/all.json"

UPDATE_INTERVAL = 30        # toutes les 30 secondes
API_REFRESH_INTERVAL = 480  # toutes les 8 minutes

API_URL = "https://opensky-network.org/api/states/all"

# === FONCTIONS ===
def log(msg, level="INFO"):
    """Affiche un log avec timestamp."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {level}: {msg}")

def copy_to_docker():
    """Copie le fichier mis à jour dans le conteneur NiFi."""
    try:
        subprocess.run(
            ["docker", "cp", INPUT_FILE, f"{CONTAINER_NAME}:{DOCKER_DEST_PATH}"],
            check=True,
            capture_output=True
        )
        log(f"Copie effectuée dans {CONTAINER_NAME}:{DOCKER_DEST_PATH}")
    except subprocess.CalledProcessError as e:
        log(f"Impossible de copier dans le conteneur : {e}", "ERROR")

def get_real_data():
    """Tente de récupérer les données réelles depuis OpenSky (sans authentification)."""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "states" in data:
            log(f"Données OpenSky récupérées ({len(data['states'])} entrées).")
            return data
        else:
            log("Réponse OpenSky sans champ 'states'.", "WARNING")
            return None
    except Exception as e:
        log(f"Erreur lors de la requête OpenSky : {e}", "ERROR")
        return None

def save_json(data, path):
    """Sauvegarde un dictionnaire JSON dans un fichier."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    log(f"Fichier sauvegardé : {path}")

def load_json(path):
    """Charge un fichier JSON."""
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def generate_from_previous(prev_data, current_data):
    """Génère de nouvelles données simulées basées sur la différence entre n-1.json et all.json."""
    if not prev_data or not current_data:
        log("Impossible de générer : fichiers précédents manquants.", "ERROR")
        return current_data, pd.DataFrame(current_data.get("states", []))

    prev_states = prev_data.get("states", [])
    curr_states = current_data.get("states", [])

    new_states = []
    current_time = int(time.time())

    for i, state in enumerate(curr_states):
        if i >= len(prev_states):
            s_new = state.copy()
        else:
            s_prev = prev_states[i]
            s_curr = state.copy()
            s_new = s_curr.copy()

            for j, val in enumerate(s_curr):
                try:
                    if isinstance(val, (int, float)) and isinstance(s_prev[j], (int, float)):
                        s_new[j] = (s_curr[j] - s_prev[j]) + s_curr[j]
                    elif isinstance(val, bool):
                        s_new[j] = val
                    elif isinstance(val, str):
                        s_new[j] = val
                except Exception:
                    continue

        # 🔄 Correction des booléens
        # on_ground = index 8, spi = index 17
        if len(s_new) > 8:
            s_new[8] = bool(s_new[8])
        if len(s_new) > 17:
            s_new[17] = bool(s_new[17])

        # 🕒 Mise à jour des timestamps
        if len(s_new) > 3:
            s_new[3] = current_time
        if len(s_new) > 4:
            s_new[4] = current_time

        new_states.append(s_new)

    new_data = {"time": current_time, "states": new_states}
    df = pd.DataFrame(new_states)
    return new_data, df


# === BOUCLE PRINCIPALE ===
if __name__ == "__main__":
    log("=== Démarrage de la génération automatique ===")
    last_api_time = 0

    while True:
        now = time.time()
        elapsed = now - last_api_time

        # === 1️⃣ Vérifie si on doit appeler OpenSky ===
        if elapsed >= API_REFRESH_INTERVAL:
            log("Tentative de récupération depuis OpenSky...")
            data = get_real_data()
            if data:
                # Sauvegarde ancienne version comme n-1.json
                if os.path.exists(INPUT_FILE):
                    os.replace(INPUT_FILE, PREV_FILE)
                    log(f"Ancien all.json renommé en n-1.json")

                # Sauvegarde nouvelle version
                save_json(data, INPUT_FILE)
                copy_to_docker()
                last_api_time = now
                log("Mise à jour réussie depuis OpenSky.\n")
                time.sleep(UPDATE_INTERVAL)
                continue  # saute la génération locale
            else:
                log("Échec de la mise à jour depuis OpenSky, on continue en local.", "WARNING")

        # === 2️⃣ Génération locale basée sur n-1.json et all.json ===
        prev_data = load_json(PREV_FILE)
        current_data = load_json(INPUT_FILE)

        new_data, df = generate_from_previous(prev_data, current_data)
        save_json(new_data, INPUT_FILE)
        copy_to_docker()

        log(f"{len(df)} lignes générées (simulation locale).")
        log("Attente de 30 secondes...\n")
        time.sleep(UPDATE_INTERVAL)
