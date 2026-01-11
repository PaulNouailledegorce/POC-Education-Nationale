#!/usr/bin/env python
"""
Convertit un fichier JSON contenant une LISTE d'objets
en un fichier NDJSON (1 document JSON par ligne).
"""

import json
from pathlib import Path

# ---------- CHEMINS BASÉS SUR LE SCRIPT ----------
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT = BASE_DIR / "data" / "output" / "output_tri_structure.json"
OUTPUT = BASE_DIR / "data" / "output" / "output_tri_structure.ndjson"

def main():
    if not INPUT.exists():
        print(f"[ERREUR] Fichier introuvable : {INPUT.resolve()}")
        return

    print(f"[INFO] Chargement du fichier JSON : {INPUT.resolve()}")
    data = json.loads(INPUT.read_text(encoding="utf-8"))

    if not isinstance(data, list):
        raise ValueError(
            "ERREUR : Le fichier JSON doit contenir une liste d'objets, "
            "pas un objet unique."
        )

    print(f"[INFO] Nombre de documents trouvés : {len(data)}")

    with OUTPUT.open("w", encoding="utf-8") as f:
        for doc in data:
            line = json.dumps(doc, ensure_ascii=False)
            f.write(line + "\n")

    print("=" * 60)
    print("[OK] Conversion terminée.")
    print("[OK] Fichier NDJSON :", OUTPUT.resolve())
    print("[OK] Nombre de lignes écrites :", len(data))
    print("=" * 60)


if __name__ == "__main__":
    main()
