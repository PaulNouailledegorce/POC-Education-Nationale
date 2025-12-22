#!/usr/bin/env python
import json
from pathlib import Path

# Nom du fichier de sortie
OUTPUT_FILE = Path("test_output.json")


def safe_write_json(path: Path, data) -> None:
    """
    Écrit le JSON de manière atomique :
    - écriture dans un fichier temporaire
    - remplacement du fichier cible une fois l'écriture terminée
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")  # pour un fichier qui finit proprement par un retour à la ligne

    # Remplacement atomique (sur le même système de fichiers)
    tmp_path.replace(path)


def load_json_list(path: Path):
    """
    Charge le fichier JSON comme une liste d'objets.
    Garantit qu'on travaille toujours avec une liste.
    Gère les cas :
    - fichier absent
    - fichier vide
    - JSON invalide
    - contenu non liste
    """
    if not path.exists() or path.stat().st_size == 0:
        print("[INFO] Fichier inexistant ou vide, initialisation avec une liste vide []")
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[ERREUR] JSON invalide dans {path} : {e}")
        while True:
            choice = input("Souhaitez-vous réinitialiser le fichier ? [O]ui / [N]on : ").strip().lower()
            if choice in ("o", "oui"):
                print("[INFO] Le fichier sera réinitialisé (liste vide). Un backup sera créé.")
                backup_path = path.with_suffix(path.suffix + ".bak")
                path.replace(backup_path)
                print(f"[INFO] Backup enregistré sous : {backup_path}")
                return []
            elif choice in ("n", "non"):
                print("[INFO] Abandon sans modification.")
                raise SystemExit(1)
            else:
                print("[ERREUR] Réponse invalide. Répondez par O ou N.")
    except OSError as e:
        print(f"[ERREUR] Impossible de lire le fichier : {e}")
        raise SystemExit(1)

    if data is None:
        print("[INFO] Contenu 'null' détecté, conversion en liste vide [].")
        return []

    if isinstance(data, list):
        return data

    print("[ERREUR] Le JSON existant n'est pas une liste (tableau) mais :", type(data).__name__)
    while True:
        choice = input("Convertir en liste avec cet objet comme premier élément ? [O]ui / [N]on : ").strip().lower()
        if choice in ("o", "oui"):
            return [data]
        elif choice in ("n", "non"):
            print("[INFO] Abandon sans modification.")
            raise SystemExit(1)
        else:
            print("[ERREUR] Réponse invalide. Répondez par O ou N.")


def prompt_for_object():
    """
    Demande à l'utilisateur de saisir un objet JSON (un dict),
    vérifie que c'est bien un objet { }.
    """
    print("Entrez un objet JSON (ex : {\"nom\": \"Paul\", \"age\": 30})")
    while True:
        raw = input("Objet JSON : ").strip()
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"[ERREUR] Objet JSON invalide : {e}")
            continue

        if not isinstance(obj, dict):
            print("[ERREUR] L'objet doit être un objet JSON (délimité par { } ).")
            continue

        return obj


def main():
    output_path = OUTPUT_FILE.resolve()
    print("[DEBUG] Fichier cible :", output_path)

    while True:
        print("\n=== Menu ===")
        print("1) Ajouter un objet à la suite")
        print("2) Réécrire tous les objets (repartir d'un fichier propre)")
        print("3) Afficher le contenu actuel")
        print("4) Quitter")
        choix = input("Votre choix : ").strip()

        if choix == "1":
            # Ajouter un objet à la suite (en partant de rien si le fichier est vide ou n'existe pas)
            data = load_json_list(output_path)
            obj = prompt_for_object()
            data.append(obj)
            safe_write_json(output_path, data)
            print("[INFO] Objet ajouté et fichier réécrit proprement.")

        elif choix == "2":
            # Overwrite complet : on repart d'une nouvelle liste d'objets
            data = []
            while True:
                obj = prompt_for_object()
                data.append(obj)
                encore = input("Ajouter un autre objet ? [O]ui / [N]on : ").strip().lower()
                if encore not in ("o", "oui"):
                    break
            safe_write_json(output_path, data)
            print("[INFO] Fichier réinitialisé et réécrit avec les nouveaux objets.")

        elif choix == "3":
            # Affichage du contenu actuel
            if not output_path.exists() or output_path.stat().st_size == 0:
                print("[INFO] Fichier inexistant ou vide.")
            else:
                try:
                    with output_path.open("r", encoding="utf-8") as f:
                        print("\n===== Contenu actuel =====")
                        print(f.read())
                        print("===== Fin du contenu =====\n")
                except OSError as e:
                    print("[ERREUR] Impossible de lire le fichier :", e)

        elif choix == "4":
            print("Au revoir.")
            break

        else:
            print("[ERREUR] Choix invalide. Tapez 1, 2, 3 ou 4.")


if __name__ == "__main__":
    main()
