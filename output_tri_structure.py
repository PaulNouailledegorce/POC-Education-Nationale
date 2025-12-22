#!/usr/bin/env python
"""
Traitement complet : on prend toutes les plaintes de output_tri.json,
on les passe à Gemini 2.5 Flash Lite PAR BATCHS (plusieurs plaintes par requête),
et on écrit une LISTE d'objets enrichis dans output_tri_structure.json.

Gestion "propre" du JSON :
- écriture atomique (fichier temporaire puis replace),
- reprise possible sans casser le fichier,
- reset possible sur demande.
"""

import json
import time
from pathlib import Path
from typing import List, Optional, Literal, Tuple, Set
from nature_probleme import NATURE_PROBLEME
import json


from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# ---------- CONFIG ----------
BATCH_SIZE = 5          # nombre de plaintes traitées par requête API
MAX_RETRIES = 3         # nb de tentatives par batch en cas de 503
RETRY_BASE_DELAY = 10   # secondes (backoff exponentiel)

# ---------- CHEMINS BASÉS SUR LE SCRIPT ----------
BASE_DIR = Path(__file__).resolve().parent
API_KEY_FILE = BASE_DIR / "api_key.txt"
INPUT_JSON = BASE_DIR / "output_tri.json"
OUTPUT_JSON = BASE_DIR / "output_tri_structure.json"


# ---------- SCHÉMA DE SORTIE (Pydantic) ----------
class PlainteEnrichie(BaseModel):
    # 📌 Contexte extrait automatiquement
    lieu: Optional[str] = Field(
        description="Lieu principal concerné par la plainte (si identifiable)."
    )
    etablissement: Optional[str] = Field(
        description="Nom ou type d'établissement (collège, lycée, etc.) si mentionné."
    )
    commune: Optional[str] = Field(
        description="Commune / ville si identifiable."
    )
    academie: Optional[str] = Field(
        description="Académie si identifiable à partir du contexte."
    )
    personnes_impliquees: List[str] = Field(
        default_factory=list,
        description="Liste des personnes ou rôles impliqués (ex: élève, parent, enseignant, chef d'établissement).",
    )
    acteurs_vises: List[str] = Field(
        default_factory=list,
        description="Liste des acteurs ou institutions principalement visés par la plainte.",
    )

    # 📌 Problématique
    categorie_probleme: Optional[str]
    type_probleme: Optional[str]
    sous_probleme: Optional[str]
    thematique: Optional[str]

    # 📌 Gravité & Urgence
    gravite: Optional[int] = Field(
        description="Gravité de 1 (faible) à 3 (critique)."
    )
    urgence: Optional[int] = Field(
        description="Urgence de 1 (faible) à 3 (critique)."
    )
    enjeux_sensibles: List[str] = Field(
        default_factory=list,
        description="Liste d'enjeux sensibles (ex: handicap, harcèlement, discrimination), vide si aucun.",
    )

    # 📌 Sentiment & Ton
    sentiment: Optional[Literal["positif", "neutre", "negatif", "tres_negatif"]] = None
    emotion: Optional[str] = Field(
        description="Emotion dominante (ex: colère, anxiété, injustice, détresse...)."
    )
    tonalite: Optional[str] = Field(
        description="Tonalité du texte (ex: formel, agressif, urgent, résigné...)."
    )

    # 📌 Action recherchée
    action_souhaitee: Optional[str] = Field(
        description="Ce que la personne semble demander (information, intervention, révision de décision...)."
    )
    objectif_demandeur: Optional[str] = Field(
        description="Objectif final implicite ou explicite du demandeur."
    )
    type_resolution_attendue: Optional[str] = Field(
        description="Type de résolution attendue (médiation, réparation, explication, changement de décision...)."
    )

    # 📌 Données textuelles dérivées
    analyse_brute: str = Field(
        description="Texte original d'analyse / plainte fourni en entrée (copie du champ 'Analyse')."
    )
    resume_synthetique: Optional[str] = Field(
        description="Résumé factuel et synthétique en quelques phrases de la situation."
    )
    mots_cles: List[str] = Field(
        default_factory=list,
        description="Liste de mots-clés synthétiques résumant la situation.",
    )


# ---------- UTILITAIRES GÉNÉRIQUES JSON ----------

def safe_write_json(path: Path, data) -> None:
    """
    Écrit le JSON de manière atomique :
    - écriture dans un fichier temporaire
    - remplacement du fichier cible une fois l'écriture terminée

    Objectif : ne jamais laisser un fichier partiellement écrit ou corrompu.
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    # Écriture du JSON dans un fichier temporaire
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")  # fichier qui se termine proprement par un retour à la ligne
    # Remplacement atomique
    tmp_path.replace(path)


# ---------- UTILITAIRES EXISTANTS ----------

def load_api_key() -> str:
    if not API_KEY_FILE.exists():
        raise FileNotFoundError(
            f"Le fichier {API_KEY_FILE} n'existe pas. "
            "Crée ce fichier et mets-y UNIQUEMENT la clé API."
        )

    api_key = API_KEY_FILE.read_text(encoding="utf-8").strip()
    if not api_key:
        raise ValueError("La clé API est vide dans le fichier.")
    return api_key


def load_all_plaintes() -> List[dict]:
    """Charge toutes les plaintes depuis le JSON d'entrée."""
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Fichier d'entrée introuvable : {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list) or len(data) == 0:
        raise ValueError("Le fichier JSON doit contenir une liste non vide d'objets.")

    return data


def load_existing_results() -> Tuple[List[dict], Set]:
    """
    Charge le fichier de sortie s'il existe déjà, de manière robuste.

    Retourne :
      - la liste d'objets enrichis déjà présents
      - l'ensemble des IDs déjà traités

    Gestion "propre" :
    - si le fichier n'existe pas ou est vide -> [], set()
    - si le JSON est invalide -> on propose de créer un backup et de repartir de zéro
    - si le contenu n'est pas une liste -> avertissement + option de repartir de zéro
    """
    if not OUTPUT_JSON.exists() or OUTPUT_JSON.stat().st_size == 0:
        return [], set()

    try:
        raw = OUTPUT_JSON.read_text(encoding="utf-8").strip()
        if not raw:
            # Fichier rempli uniquement d'espaces / retours à la ligne
            print("[INFO] Fichier de sortie vide (espaces/retours à la ligne). Initialisation avec une liste vide.")
            return [], set()

        data = json.loads(raw)

        if not isinstance(data, list):
            print("[AVERTISSEMENT] Le fichier de sortie existe mais ne contient pas une liste JSON.")
            while True:
                choice = input(
                    "Souhaites-tu sauvegarder ce fichier sous forme de backup et repartir de zéro ? "
                    "[O]ui / [N]on : "
                ).strip().lower()
                if choice in ("o", "oui"):
                    backup = OUTPUT_JSON.with_suffix(OUTPUT_JSON.suffix + ".bak")
                    OUTPUT_JSON.replace(backup)
                    print(f"[INFO] Backup créé : {backup}")
                    return [], set()
                elif choice in ("n", "non"):
                    print("[INFO] Abandon du traitement.")
                    raise SystemExit(1)
                else:
                    print("[ERREUR] Réponse invalide. Réponds par O ou N.")
    except json.JSONDecodeError as e:
        print(f"[ERREUR] JSON invalide dans {OUTPUT_JSON} : {e}")
        while True:
            choice = input(
                "Le fichier de sortie est corrompu. Sauvegarder en .bak et repartir d'un fichier propre ? "
                "[O]ui / [N]on : "
            ).strip().lower()
            if choice in ("o", "oui"):
                backup = OUTPUT_JSON.with_suffix(OUTPUT_JSON.suffix + ".bak")
                OUTPUT_JSON.replace(backup)
                print(f"[INFO] Fichier corrompu sauvegardé sous : {backup}")
                return [], set()
            elif choice in ("n", "non"):
                print("[INFO] Abandon du traitement.")
                raise SystemExit(1)
            else:
                print("[ERREUR] Réponse invalide. Réponds par O ou N.")
    except OSError as e:
        print(f"[ERREUR] Impossible de lire {OUTPUT_JSON} : {e}")
        raise SystemExit(1)

    done_ids = {obj.get("id") for obj in data if isinstance(obj, dict) and "id" in obj}
    print(f"[INFO] Fichier de sortie existant trouvé : {len(data)} plaintes déjà enrichies.")
    if done_ids:
        try:
            last_id = max(done_ids)
            print(f"[INFO] Dernier id traité dans le fichier de sortie : {last_id}")
        except Exception:
            pass
    return data, done_ids


def build_batch_prompt(batch: List[dict]) -> str:
    """
    Construit le prompt pour UN BATCH de plaintes.
    On donne la liste en JSON, + la taxonomie NATURE_PROBLEME,
    et on demande un tableau JSON d'objets d'enrichissement dans le même ordre.
    """
    plaintes_json = json.dumps(batch, ensure_ascii=False, indent=2)
    taxonomie_json = json.dumps(NATURE_PROBLEME, ensure_ascii=False, indent=2)

    prompt = f"""
Tu es un expert de médiation scolaire et d'analyse de saisines.

On te fournit :
1) Une LISTE de plaintes sous forme JSON, avec des champs bruts issus d'un système
   (identifiant, dates, pôle, catégorie, analyse textuelle, etc.).
2) Une TAXONOMIE de la nature des problèmes, appelée NATURE_PROBLEME, structurée ainsi :
   - chaque entrée a une clé "label" (catégorie principale, ex. "harcelement", "examens", "bourses_aides", etc.)
   - chaque "label" contient un dictionnaire "sous_labels" avec des clés (codes) de sous-problèmes
     (ex. "harcelement_islamophobe", "contestation_note", "refus_bourse", etc.).

TAXONOMIE NATURE_PROBLEME (codes autorisés) :
{taxonomie_json}

RÔLE ATTENDU
------------ 
Pour CHAQUE plainte, tu dois :
- analyser le contenu de manière objective et factuelle ;
- remplir un objet d'enrichissement conforme au schéma (response_schema) avec, en particulier :
  - categorie_probleme : le CODE du label choisi dans NATURE_PROBLEME
    (ex. "harcelement", "examens", "bourses_aides", "autre"...)
  - sous_probleme : le CODE du sous_label choisi dans NATURE_PROBLEME[categorie_probleme]["sous_labels"]
    (ex. "harcelement_islamophobe", "contestation_note", "refus_bourse", "autre"...)

CONTRAINTES SUR LA TAXONOMIE
----------------------------
- Tu DOIS toujours choisir EXACTEMENT :
  - 1 valeur de "label" parmi les clés de NATURE_PROBLEME,
  - 1 valeur de "sous_label" parmi les clés de "sous_labels" du label choisi.
- Tu NE DOIS PAS inventer de nouveaux codes.
- Si aucun label ne semble parfaitement adapté, tu prends "autre" comme categorie_probleme.
- Si aucun sous_label spécifique ne convient à l'intérieur d'un label, tu prends "autre" dans ses sous_labels.
- Tu écris dans les champs :
  - categorie_probleme : le code du label (par ex. "harcelement", "examens", "bourses_aides", "autre")
  - sous_probleme : le code du sous_label (par ex. "harcelement_islamophobe", "refus_bourse", "autre")

AUTRES CHAMPS DU SCHÉMA
-----------------------
En plus de ces deux champs, tu renseignes normalement les autres champs de l'objet d'enrichissement :
- gravite : entier de 1 (faible) à 3 (critique).
- urgence : entier de 1 (faible) à 3 (critique).
- enjeux_sensibles : liste de codes (ex. ["handicap", "harcelement", "discrimination"]) si applicable.
- personnes_impliquees, acteurs_vises, action_souhaitee, type_resolution_attendue, etc.
- analyse_brute : doit reprendre INTÉGRALEMENT le texte du champ "Analyse" de la plainte correspondante.
- resume_synthetique : résumé concis, factuel, sans jugement de valeur.
- mots_cles : liste de quelques termes courts (1 à 3 mots chacun).

RAPPEL IMPORTANT
----------------
- Tu n'as PAS à renvoyer les champs bruts (dates, id, pôle, etc.) : le système les recopie lui-même.
- Si une information n'est pas clairement déductible, mets null, une valeur neutre adaptée ou une liste vide.
- Tu dois répondre par un TABLEAU JSON d'objets d'enrichissement, dans le même ordre que la liste d'entrée :
  - 1er élément de la liste d'entrée -> 1er objet d'enrichissement
  - 2e élément -> 2e objet, etc.

Voici la LISTE DES PLAINTES À ANALYSER (JSON) :

{plaintes_json}

NE RENVOIE AUCUNE EXPLICATION EN TEXTE LIBRE.
Renvoie UNIQUEMENT un TABLEAU JSON d'objets d'enrichissement, dans le même ordre que la liste d'entrée.
"""
    return prompt


def enrich_batch(client: genai.Client, batch: List[dict]) -> List[dict]:
    """
    Appelle Gemini pour un batch de plaintes, avec gestion des retries en cas de 503.
    Retourne la liste des objets finaux (plainte d'origine + enrichissement).
    """
    prompt = build_batch_prompt(batch)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=list[PlainteEnrichie],  # liste d'objets d'enrichissement
                ),
            )
            parsed_list: List[PlainteEnrichie] = response.parsed

            if len(parsed_list) != len(batch):
                raise ValueError(
                    f"Nombre d'objets retournés ({len(parsed_list)}) "
                    f"différent du nombre de plaintes en entrée ({len(batch)})."
                )

            final_batch = []
            for plainte_brute, enrichie in zip(batch, parsed_list):
                enriched_dict = enrichie.model_dump()
                final_obj = {**plainte_brute, **enriched_dict}
                final_batch.append(final_obj)

            return final_batch

        except Exception as e:
            msg = str(e)
            if "503" in msg or "UNAVAILABLE" in msg:
                if attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    print(
                        f"[AVERTISSEMENT] 503 UNAVAILABLE (tentative {attempt}/{MAX_RETRIES}). "
                        f"Nouvel essai dans {delay} secondes..."
                    )
                    time.sleep(delay)
                    continue
                else:
                    print("[ERREUR] 503 UNAVAILABLE après plusieurs tentatives. Arrêt du traitement.")
                    # On remonte l'erreur jusqu'au main, qui ne touchera pas au fichier de sortie
                    raise
            else:
                print(f"[ERREUR] Échec enrichissement batch : {e}")
                raise


# ---------- LOGIQUE PRINCIPALE ----------

def main():
    try:
        api_key = load_api_key()
        client = genai.Client(api_key=api_key)

        plaintes = load_all_plaintes()
        total = len(plaintes)
        print(f"[INFO] Nombre total de plaintes dans le fichier d'entrée : {total}")
        print(f"[INFO] Fichier de sortie : {OUTPUT_JSON.resolve()}")

        # Charger résultats existants (gestion robuste)
        existing_results, done_ids = load_existing_results()

        if existing_results:
            choice = input(
                "Un fichier de sortie existe déjà.\n"
                f"- {len(existing_results)} plaintes déjà enrichies.\n"
                "Que veux-tu faire ? [R]eprendre là où ça s'est arrêté / "
                "[E]craser et recommencer depuis le début : "
            ).strip().lower()

            if choice == "e":
                print("[INFO] Écrasement du fichier de sortie et reprise à zéro.")
                existing_results = []
                done_ids = set()
                OUTPUT_JSON.unlink(missing_ok=True)
            else:
                print("[INFO] Reprise : les plaintes dont l'id est déjà présent seront ignorées.")

        results: List[dict] = list(existing_results)

        # Filtrer les plaintes non encore traitées
        pending = [p for p in plaintes if p.get("id") not in done_ids]
        print(f"[INFO] Plaintes restantes à traiter : {len(pending)}")

        for start in range(0, len(pending), BATCH_SIZE):
            batch = pending[start: start + BATCH_SIZE]
            ids_batch = [p.get("id") for p in batch]
            print(f"\n[INFO] Traitement batch {start} -> {start + len(batch) - 1} (ids={ids_batch})")

            final_batch = enrich_batch(client, batch)

            # Mise à jour des résultats + set d'IDs
            results.extend(final_batch)
            for obj in final_batch:
                pid = obj.get("id")
                if pid is not None:
                    done_ids.add(pid)

            # Sauvegarde immédiate, en mode "propre" (atomique)
            safe_write_json(OUTPUT_JSON, results)
            print(
                f"[OK] Batch de {len(final_batch)} plaintes enrichies et sauvegardées "
                f"(total={len(results)})."
            )

        print(f"\n[OK] Traitement terminé. {len(results)} plaintes enrichies au total.")
        print(f"[OK] Résultat final dans : {OUTPUT_JSON.resolve()}")

    except Exception as e:
        print(f"[ERREUR] Une erreur s'est produite : {e}")
        print("[INFO] Tout ce qui a été enrichi avant l'erreur est déjà sauvegardé (écriture atomique).")


if __name__ == "__main__":
    main()
