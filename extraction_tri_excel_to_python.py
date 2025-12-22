#!/usr/bin/env python
import json
from pathlib import Path
import pandas as pd

INPUT_PATH = Path("Excel et data\concatenation.xlsx")

# 🔥 Nom du fichier de sortie JSON
OUTPUT_JSON = Path("output_tri.json")


def load_table(input_path: Path) -> pd.DataFrame:
    """
    Charge un fichier CSV ou Excel en DataFrame.
    """
    suffix = input_path.suffix.lower()

    if suffix in [".xls", ".xlsx", ".xlsm", ".xlsb", ".ods"]:
        df = pd.read_excel(input_path)
    elif suffix == ".csv":
        # CSV avec ; comme séparateur
        df = pd.read_csv(
            input_path,
            sep=";",          # séparateur
            engine="python",  # plus tolérant
        )
    else:
        raise ValueError(f"Extension non supportée : {suffix}")

    return df


def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pré-nettoyage :
    - 'Numéro' -> 'id' (entier)
    - on garde seulement les colonnes utiles
    """

    # 1) Créer la colonne 'id' à partir de 'Numéro'
    if "Numéro" in df.columns:
        # Convertir en entier (en gérant les NaN)
        df["id"] = df["Numéro"].astype("Int64")
    else:
        # Si jamais la colonne n'existe pas, on lève une erreur claire
        raise KeyError("La colonne 'Numéro' est introuvable dans le fichier.")

    # 2) Définir les colonnes à garder
    keep_cols = [
        "Numéro",
        "Date arrivée",
        "Date clôture fiche",
        "Pôle en charge",
        "Catégorie",
        "Sous-catégorie",
        "Domaine",
        "Sous-domaine",
        "Aspect contextuel",
        "Nature de la saisine",
        "Réclamation : position du médiateur",
        "Impact de l'appui du médiateur",
        "Analyse",
    ]

    # 3) Remplacer les tags géographiques par les noms de villes complets
    tag_to_ville = {
        "AMI": "Amiens",
        "AXM": "Aix-Marseille",
        "BES": "Besançon",
        "BOR": "Bordeaux",
        "CLE": "Clermont-Ferrand",
        "CND": "Caen",
        "COM": "Corse (Ajaccio / Bastia)",
        "CRE": "Creteil",
        "DIJ": "Dijon",
        "GUA": "Guadeloupe",
        "GRE": "Grenoble",
        "GUY": "Guyane",
        "LIL": "Lille",
        "LIM": "Limoges",
        "LYO": "Lyon",
        "MAR": "Martinique",
        "MON": "Montpellier",
        "NAN": "Nantes",
        "NAT": "Nationale (services ou dispositifs nationaux?? )",
        "NCY": "Nancy-Metz",
        "NIC": "Nice",
        "NOR": "Normandie",
        "ORL": "Orleans-Tours",
        "PAR": "Paris",
        "POI": "Poitiers",
        "REI": "Reims",
        "REN": "Rennes",
        "REU": "La Reunion",
        "STR": "Strasbourg",
        "TOU": "Toulouse",
        "VER": "Versailles",
    }
    
    # Remplacer les tags dans la colonne "Pôle en charge" si elle existe
    if "Pôle en charge" in df.columns:
        df["Pôle en charge"] = df["Pôle en charge"].apply(
            lambda x: tag_to_ville.get(x, x) if pd.notna(x) else x
        )

    # 4) Vérifier qu'elles existent bien (sinon tu verras lesquelles manquent)
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans le fichier : {missing}")

    # 3) Ne garder que ces colonnes
    df = df[keep_cols]

    return df


def drop_rows_without_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les lignes dépourvues d'identifiant pour éviter les objets JSON vides.
    """
    before = len(df)
    df = df[df["id"].notna()].copy()
    removed = before - len(df)
    if removed:
        print(f"[INFO] Lignes supprimées car id null : {removed}")
    return df


def dataframe_to_json(df: pd.DataFrame):
    """
    Convertit chaque ligne du DataFrame en objet JSON.
    """
    # Convertit NaN / <NA> → None pour un JSON propre
    df = df.where(pd.notnull(df), None)

    # Convertit en liste de dictionnaires
    return df.to_dict(orient="records")


def save_json(data, output_path: Path):
    """
    Sauvegarde la liste d'objets JSON dans un seul fichier.
    """
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    print(f"[INFO] Chargement du fichier : {INPUT_PATH}")

    df = load_table(INPUT_PATH)
    print(f"[INFO] Lignes brutes trouvées : {len(df)}")
    print(f"[INFO] Colonnes brutes : {list(df.columns)}")

    # 🔥 Pré-nettoyage selon tes règles
    df = preprocess_df(df)
    df = drop_rows_without_id(df)
    print(f"[INFO] Colonnes après pré-nettoyage : {list(df.columns)}")

    data = dataframe_to_json(df)

    print(f"[INFO] Sauvegarde JSON dans : {OUTPUT_JSON}")
    save_json(data, OUTPUT_JSON)

    print("[OK] Conversion + pré-nettoyage terminés ✔️")


if __name__ == "__main__":
    main()
