#!/usr/bin/env python
"""
Importe un fichier NDJSON dans Elasticsearch via l'API _bulk.

- Elasticsearch est supposé tourner sur http://localhost:9200
- Le NDJSON est supposé contenir 1 document JSON par ligne
- Le champ "id" du document est utilisé comme _id dans ES (si présent)
"""

import json
from pathlib import Path
import requests

# ----------------- CONFIG -----------------

ES_URL = "http://localhost:9200"
INDEX_NAME = "plaintes_mediation_v1"

# ---------- CHEMINS BASÉS SUR LE SCRIPT ----------
BASE_DIR = Path(__file__).resolve().parent.parent
NDJSON_FILE = BASE_DIR / "back" / "data" / "output" / "output_tri_structure.ndjson"

BATCH_SIZE = 500  # nombre de documents envoyés par requête _bulk


# ----------------- FONCTIONS -----------------


def check_es_up():
    """Vérifie que Elasticsearch répond bien."""
    try:
        r = requests.get(ES_URL)
        r.raise_for_status()
        print(f"[OK] Elasticsearch est joignable sur {ES_URL}")
    except Exception as e:
        raise SystemExit(f"[ERREUR] Impossible de joindre Elasticsearch sur {ES_URL} : {e}")


def ensure_index():
    """
    Vérifie si l'index existe.
    Si non, le crée avec un mapping très simple (ES fera le reste dynamiquement).
    """
    r = requests.head(f"{ES_URL}/{INDEX_NAME}")
    if r.status_code == 200:
        print(f"[INFO] Index '{INDEX_NAME}' existe déjà.")
        return

    print(f"[INFO] Index '{INDEX_NAME}' inexistant, création...")
    # Ici on laisse ES créer le mapping dynamique.
    r = requests.put(f"{ES_URL}/{INDEX_NAME}")
    try:
        r.raise_for_status()
        print(f"[OK] Index '{INDEX_NAME}' créé.")
    except Exception as e:
        print(f"[ERREUR] Création de l'index '{INDEX_NAME}' : {r.text}")
        raise SystemExit(e)


def iter_ndjson_lines(path: Path):
    """Itère sur chaque ligne non vide du fichier NDJSON."""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield line


def bulk_send(batch_docs):
    """
    Envoie un batch de documents au format _bulk.
    batch_docs = liste de dict JSON (déjà parsés)
    """
    if not batch_docs:
        return 0

    bulk_lines = []

    for doc in batch_docs:
        # On utilise le champ "id" comme _id si présent
        doc_id = doc.get("id")
        if doc_id is not None:
            action = {"index": {"_index": INDEX_NAME, "_id": doc_id}}
        else:
            action = {"index": {"_index": INDEX_NAME}}

        bulk_lines.append(json.dumps(action, ensure_ascii=False))
        bulk_lines.append(json.dumps(doc, ensure_ascii=False))

    payload = "\n".join(bulk_lines) + "\n"

    resp = requests.post(
        f"{ES_URL}/_bulk",
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
    )

    try:
        resp.raise_for_status()
    except Exception as e:
        print("[ERREUR] Requête _bulk échouée :")
        print(resp.text[:2000])
        raise e

    data = resp.json()
    if data.get("errors"):
        # On affiche quelques erreurs pour debug
        print("[AVERTISSEMENT] _bulk a des erreurs pour certains documents.")
        for item in data.get("items", [])[:5]:
            if "error" in item["index"]:
                print("  ->", item["index"]["error"])
        # On continue quand même, les autres docs sont indexés
    else:
        print(f"[OK] _bulk : {len(batch_docs)} documents indexés sans erreur.")

    return len(batch_docs)


def main():
    if not NDJSON_FILE.exists():
        raise SystemExit(f"[ERREUR] Fichier NDJSON introuvable : {NDJSON_FILE.resolve()}")

    print(f"[INFO] Fichier NDJSON : {NDJSON_FILE.resolve()}")

    check_es_up()
    ensure_index()

    total = 0
    batch = []

    for line in iter_ndjson_lines(NDJSON_FILE):
        try:
            doc = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"[AVERTISSEMENT] Ligne invalide ignorée : {e}")
            continue

        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            print(f"[INFO] Envoi d'un batch de {len(batch)} documents...")
            n = bulk_send(batch)
            total += n
            batch = []

    # Dernier batch restant
    if batch:
        print(f"[INFO] Envoi du dernier batch de {len(batch)} documents...")
        n = bulk_send(batch)
        total += n

    print(f"[OK] Import terminé. Total de documents envoyés : {total}")


if __name__ == "__main__":
    main()
