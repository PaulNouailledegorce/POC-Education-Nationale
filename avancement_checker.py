#!/usr/bin/env python
"""
Outil de vérification de l'avancement de l'enrichissement des plaintes.
Permet de vérifier l'état actuel du traitement et de reprendre proprement
au bon endroit si l'API est down.
"""

import json
from pathlib import Path
from typing import List, Set, Dict, Optional

# ---------- CHEMINS BASÉS SUR LE SCRIPT ----------
BASE_DIR = Path(__file__).resolve().parent
INPUT_JSON = BASE_DIR / "output_tri.json"
OUTPUT_JSON = BASE_DIR / "output_tri_structure2.json"


def load_json_list(path: Path) -> List[dict]:
    """
    Charge le fichier JSON comme une liste d'objets.
    Gère les cas : fichier absent, vide, JSON invalide, contenu non liste.
    """
    if not path.exists() or path.stat().st_size == 0:
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[ERREUR] JSON invalide dans {path} : {e}")
        return []
    except OSError as e:
        print(f"[ERREUR] Impossible de lire le fichier : {e}")
        return []

    if data is None:
        return []

    if isinstance(data, list):
        return data

    print(f"[AVERTISSEMENT] Le JSON dans {path} n'est pas une liste mais : {type(data).__name__}")
    return []


def get_statistics(input_data: List[dict], output_data: List[dict]) -> Dict:
    """
    Calcule les statistiques de l'avancement.
    """
    total_input = len(input_data)
    total_output = len(output_data)
    
    # IDs dans les fichiers
    input_ids = {obj.get("id") for obj in input_data if isinstance(obj, dict) and "id" in obj}
    output_ids = {obj.get("id") for obj in output_data if isinstance(obj, dict) and "id" in obj}
    
    # IDs traités et non traités
    treated_ids = output_ids
    pending_ids = input_ids - output_ids
    
    # Calcul du pourcentage
    percentage = (len(treated_ids) / len(input_ids) * 100) if len(input_ids) > 0 else 0
    
    # Dernier ID traité
    last_id = max(treated_ids) if treated_ids else None
    
    # Premier ID non traité
    first_pending_id = min(pending_ids) if pending_ids else None
    
    return {
        "total_input": total_input,
        "total_output": total_output,
        "treated_count": len(treated_ids),
        "pending_count": len(pending_ids),
        "percentage": percentage,
        "last_id": last_id,
        "first_pending_id": first_pending_id,
        "treated_ids": treated_ids,
        "pending_ids": pending_ids,
    }


def display_statistics(stats: Dict):
    """
    Affiche les statistiques de manière lisible.
    """
    print("\n" + "="*60)
    print("📊 STATISTIQUES D'AVANCEMENT")
    print("="*60)
    print(f"Total de plaintes dans le fichier d'entrée : {stats['total_input']}")
    print(f"Plaintes enrichies (fichier de sortie)     : {stats['total_output']}")
    print(f"Plaintes traitées                         : {stats['treated_count']}")
    print(f"Plaintes restantes à traiter              : {stats['pending_count']}")
    print(f"Pourcentage d'avancement                   : {stats['percentage']:.2f}%")
    
    if stats['last_id'] is not None:
        print(f"\nDernier ID traité                      : {stats['last_id']}")
    
    if stats['first_pending_id'] is not None:
        print(f"Premier ID non traité                  : {stats['first_pending_id']}")
    
    print("="*60 + "\n")


def display_sample_pending(pending_ids: Set, input_data: List[dict], limit: int = 5):
    """
    Affiche un échantillon des plaintes non traitées.
    """
    if not pending_ids:
        return
    
    print(f"\n📋 Échantillon des {min(limit, len(pending_ids))} premières plaintes non traitées :")
    print("-" * 60)
    
    count = 0
    for obj in input_data:
        obj_id = obj.get("id")
        if obj_id in pending_ids:
            print(f"\nID: {obj_id}")
            if "Catégorie" in obj:
                print(f"  Catégorie: {obj.get('Catégorie', 'N/A')}")
            if "Pôle en charge" in obj:
                print(f"  Pôle: {obj.get('Pôle en charge', 'N/A')}")
            if "Analyse" in obj and obj.get("Analyse"):
                analyse = str(obj.get("Analyse", ""))[:100]
                print(f"  Analyse: {analyse}...")
            count += 1
            if count >= limit:
                break
    
    if len(pending_ids) > limit:
        print(f"\n... et {len(pending_ids) - limit} autres plaintes non traitées.")
    print("-" * 60 + "\n")


def check_file_integrity(output_data: List[dict]) -> bool:
    """
    Vérifie l'intégrité du fichier de sortie.
    """
    issues = []
    
    for i, obj in enumerate(output_data):
        if not isinstance(obj, dict):
            issues.append(f"Ligne {i}: n'est pas un dictionnaire")
            continue
        
        if "id" not in obj:
            issues.append(f"Ligne {i}: manque le champ 'id'")
        
        # Vérifier si les champs d'enrichissement sont présents
        required_enrichment_fields = [
            "categorie_probleme", "sous_probleme", "gravite", "urgence"
        ]
        missing_fields = [field for field in required_enrichment_fields if field not in obj]
        if missing_fields:
            issues.append(f"Ligne {i} (ID: {obj.get('id', 'N/A')}): champs manquants: {', '.join(missing_fields)}")
    
    if issues:
        print("\n⚠️  PROBLÈMES D'INTÉGRITÉ DÉTECTÉS :")
        print("-" * 60)
        for issue in issues[:10]:  # Limiter à 10 pour ne pas surcharger
            print(f"  - {issue}")
        if len(issues) > 10:
            print(f"  ... et {len(issues) - 10} autres problèmes.")
        print("-" * 60 + "\n")
        return False
    
    print("✅ Aucun problème d'intégrité détecté.\n")
    return True


def main():
    """
    Menu principal de vérification de l'avancement.
    """
    print("\n" + "="*60)
    print("🔍 VÉRIFICATEUR D'AVANCEMENT - ENRICHISSEMENT DES PLAINTES")
    print("="*60)
    
    # Charger les données
    print("\n[INFO] Chargement des fichiers...")
    input_data = load_json_list(INPUT_JSON)
    output_data = load_json_list(OUTPUT_JSON)
    
    if not input_data:
        print(f"[ERREUR] Le fichier d'entrée {INPUT_JSON} est vide ou introuvable.")
        return
    
    print(f"[INFO] Fichier d'entrée : {len(input_data)} plaintes")
    print(f"[INFO] Fichier de sortie : {len(output_data)} plaintes enrichies")
    
    # Calculer les statistiques
    stats = get_statistics(input_data, output_data)
    
    while True:
        print("\n" + "="*60)
        print("MENU")
        print("="*60)
        print("1) Afficher les statistiques d'avancement")
        print("2) Afficher un échantillon des plaintes non traitées")
        print("3) Vérifier l'intégrité du fichier de sortie")
        print("4) Afficher les IDs traités et non traités")
        print("5) Quitter")
        choix = input("\nVotre choix : ").strip()

        if choix == "1":
            display_statistics(stats)

        elif choix == "2":
            display_sample_pending(stats['pending_ids'], input_data, limit=5)

        elif choix == "3":
            if output_data:
                check_file_integrity(output_data)
            else:
                print("[INFO] Aucune donnée dans le fichier de sortie à vérifier.")

        elif choix == "4":
            print("\n📋 IDs traités :")
            sorted_treated = sorted(stats['treated_ids'])
            if sorted_treated:
                print(f"  Total: {len(sorted_treated)}")
                print(f"  Premier: {sorted_treated[0]}, Dernier: {sorted_treated[-1]}")
                if len(sorted_treated) <= 20:
                    print(f"  Liste: {sorted_treated}")
            else:
                print("  Aucun ID traité.")
            
            print("\n📋 IDs non traités :")
            sorted_pending = sorted(stats['pending_ids'])
            if sorted_pending:
                print(f"  Total: {len(sorted_pending)}")
                print(f"  Premier: {sorted_pending[0]}, Dernier: {sorted_pending[-1]}")
                if len(sorted_pending) <= 20:
                    print(f"  Liste: {sorted_pending}")
            else:
                print("  Aucun ID non traité (traitement terminé !)")
            print()

        elif choix == "5":
            print("\nAu revoir.\n")
            break

        else:
            print("[ERREUR] Choix invalide. Tapez 1, 2, 3, 4 ou 5.")


if __name__ == "__main__":
    main()

