#!/usr/bin/env python
import json
from pathlib import Path
import pandas as pd


# ---------- CHEMINS BASÉS SUR LE SCRIPT ----------
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "data" / "input" / "Excel et data" / "MEDIA2_Extraction_janv2022_ECE_12102025 ananymisé.xlsx"

# 🔥 Nom du fichier de sortie JSON
OUTPUT_JSON = BASE_DIR / "data" / "output" / "output.json"


def load_table(input_path: Path) -> pd.DataFrame:
    """
    Charge un fichier CSV ou Excel en DataFrame.
    """
    suffix = input_path.suffix.lower()

    if suffix in [".xls", ".xlsx", ".xlsm", ".xlsb", ".ods"]:
        df = pd.read_excel(input_path)
    elif suffix == ".csv":
        # Le fichier CSV utilise des points-virgules comme séparateur
        # et contient des guillemets dans certaines cellules.
        df = pd.read_csv(
            input_path,
            sep=";",           # séparateur correct
            engine="python",   # moteur plus tolérant pour les cas complexes
        )
    else:
        raise ValueError(f"Extension non supportée : {suffix}")

    return df


def dataframe_to_json(df: pd.DataFrame):
    """
    Convertit chaque ligne du DataFrame en objet JSON.
    """
    # Convertit NaN → None pour un JSON propre
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
    print(f"[INFO] Lignes trouvées : {len(df)}")
    print(f"[INFO] Colonnes : {list(df.columns)}")

    data = dataframe_to_json(df)

    print(f"[INFO] Sauvegarde JSON dans : {OUTPUT_JSON}")
    save_json(data, OUTPUT_JSON)

    print("[OK] Conversion terminée ✔️")


if __name__ == "__main__":
    main()
