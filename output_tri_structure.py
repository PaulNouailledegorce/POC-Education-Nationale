#!/usr/bin/env python
"""
Traitement complet (enrichissement MINIMAL) :
- On lit output_tri.json (liste d'objets "plaintes" bruts)
- On appelle Gemini 2.5 Flash Lite par BATCH
- On écrit une LISTE d'objets enrichis dans output_tri_structure2.json

CONTRAT DE SORTIE (par objet) :
- On conserve TOUS les champs initiaux tels quels (y compris "Analyse")
- On ajoute UNIQUEMENT :
  - "label" (code taxonomie)
  - "sous_label" (code taxonomie)
  - "lieu" (lieu concret si identifiable, sinon null)
  - "key_word" (liste de mots-clés, max ~5)

Aucun autre champ ne doit apparaître dans la sortie.

Gestion "propre" :
- écriture atomique (fichier temporaire puis replace),
- reprise possible sans casser le fichier,
- reset possible sur demande.
"""

import json
import time
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple, Set

from pydantic import BaseModel, Field
from google import genai
from google.genai import types
from acronymes import ACRONYMES, ACRONYMES_BRUIT



from nature_probleme import NATURE_PROBLEME

# ---------- CONFIG ----------
BATCH_SIZE = 10          # nombre de plaintes traitées par requête API
MAX_RETRIES = 3          # nb de tentatives par batch en cas de 503
RETRY_BASE_DELAY = 10    # secondes (backoff exponentiel)

# ---------- CHEMINS BASÉS SUR LE SCRIPT ----------
BASE_DIR = Path(__file__).resolve().parent
API_KEY_FILE = BASE_DIR / "api_key.txt"
INPUT_JSON = BASE_DIR / "output_tri.json"
OUTPUT_JSON = BASE_DIR / "output_tri_structure2.json"

# ---------- SCHÉMA DE SORTIE (Pydantic) : MINIMAL ----------
class EnrichissementMinimal(BaseModel):
    # 🔹 Classification principale (OBLIGATOIRE)
    label: str = Field(
        description=(
            "Code du label principal choisi parmi les clés de NATURE_PROBLEME "
            "(ex: 'harcelement', 'examens_evaluations', 'bourses_aides', 'autre')."
        )
    )
    sous_label: str = Field(
        description=(
            "Code du sous_label choisi parmi les clés de "
            "NATURE_PROBLEME[label]['sous_labels'] "
            "(ex: 'harcelement_islamophobe', 'contestation_note', 'autre')."
        )
    )

    # 🔹 Contexte léger
    lieu: Optional[str] = Field(
        default=None,
        description=(
            "Lieu concret si identifiable et pertinent "
            "(ex: 'salle de classe', 'cantine', 'cour', 'internat', "
            "'examen', 'en ligne'), sinon null."
        )
    )
    key_word: List[str] = Field(
        default_factory=list,
        description=(
            "Liste courte de mots-clés factuels (2 à 5 maximum), utiles à la statistique. "
            "Ne pas dupliquer label ou sous_label. "
            "Ex: ['conflit', 'COP', 'comportement']."
        )
    )

    # 🔹 Propositions (OPTIONNELLES – uniquement en cas d'incertitude)
    label_proposition: Optional[str] = Field(
        default=None,
        description=(
            "Label alternatif proposé UNIQUEMENT si la classification principale "
            "est incertaine ou trop restrictive. "
            "Doit être plus générique et appartenir aux clés de NATURE_PROBLEME."
        )
    )
    sous_label_proposition: Optional[str] = Field(
        default=None,
        description=(
            "Sous-label alternatif proposé UNIQUEMENT si nécessaire. "
            "Doit être plus générique ou transverse que le sous_label principal "
            "et appartenir à NATURE_PROBLEME[label_proposition]['sous_labels']."
        )
    )

# ---------- UTILITAIRES JSON ----------
def safe_write_json(path: Path, data) -> None:
    """Écrit le JSON de manière atomique."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
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
    """Charge le fichier de sortie s'il existe déjà, de manière robuste."""
    if not OUTPUT_JSON.exists() or OUTPUT_JSON.stat().st_size == 0:
        return [], set()

    try:
        raw = OUTPUT_JSON.read_text(encoding="utf-8").strip()
        if not raw:
            print("[INFO] Fichier de sortie vide (espaces/retours). Initialisation avec une liste vide.")
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

# ---------- PROMPT MINIMAL (verrouillé) ----------
def build_batch_prompt(batch: List[dict]) -> str:
    """
    Construit le prompt pour UN BATCH :
    - On fournit la taxonomie
    - On fournit les plaintes
    - On exige une sortie STRICTEMENT MINIMALE (label/sous_label/lieu/key_word) uniquement.
    """
    plaintes_json = json.dumps(batch, ensure_ascii=False, indent=2)
    taxonomie_json = json.dumps(NATURE_PROBLEME, ensure_ascii=False, indent=2)
    acronymes_json = json.dumps(ACRONYMES, ensure_ascii=False, indent=2)
    bruit_json = json.dumps(sorted(list(ACRONYMES_BRUIT)), ensure_ascii=False, indent=2)

    return f"""
Tu es un expert de médiation scolaire. Tu dois classifier des saisines afin de produire
des statistiques fiables, stables et exploitables.

================================================
IMPORTANT — CONTRAT DE SORTIE STRICT
================================================

Tu dois répondre UNIQUEMENT par un TABLEAU JSON.

Chaque élément du tableau doit contenir :
- OBLIGATOIREMENT :
  1) "label"
  2) "sous_label"
  3) "lieu"
  4) "key_word"

- OPTIONNELLEMENT (UNIQUEMENT dans certains cas) :
  5) "label_proposition"
  6) "sous_label_proposition"

⚠️ INTERDICTIONS ABSOLUES :
- Ne renvoie AUCUN autre champ.
- Ne renvoie PAS de résumé, analyse, émotion, gravité, urgence, etc.
- Ne recopie JAMAIS le texte de la plainte.
- Ne commente PAS ta réponse.

================================================
TAXONOMIE — CODES AUTORISÉS
================================================

La classification DOIT s’appuyer sur la taxonomie suivante.

- "label" doit être une des clés principales de NATURE_PROBLEME
- "sous_label" doit être une des clés de NATURE_PROBLEME[label]["sous_labels"]
- Tu ne dois JAMAIS inventer de nouveaux codes pour la classification principale.

NATURE_PROBLEME :
{taxonomie_json}

================================================
RÈGLE FONDAMENTALE DE CLASSIFICATION
================================================

Tu dois TOUJOURS fournir une classification principale ("label", "sous_label").
Cette classification est celle utilisée pour les statistiques.

------------------------------------------------
1) UTILISATION DE "autre"
------------------------------------------------
Utilise "label = autre" UNIQUEMENT si :
- aucun label existant de la taxonomie ne correspond au problème,
- le sujet est clairement hors du champ couvert par NATURE_PROBLEME.

Si tu utilises "label = autre" :
- tu DOIS proposer un "label_proposition",
- et un "sous_label_proposition" si possible.

------------------------------------------------
2) UTILISATION DES PROPOSITIONS
------------------------------------------------
Les champs "label_proposition" et "sous_label_proposition" servent
EXCLUSIVEMENT à suggérer une ÉVOLUTION de la taxonomie.

Tu peux les utiliser si :
- le cas est ambigu entre plusieurs sous_labels existants,
- le sous_label existant est trop spécifique,
- le problème est transversal ou récurrent,
- plusieurs sous_labels proches pourraient s’appliquer.

RÈGLES POUR LES PROPOSITIONS :
- Elles doivent être PLUS GÉNÉRIQUES que la classification principale.
- Elles doivent pouvoir regrouper PLUSIEURS cas similaires.
- Elles doivent appartenir à la taxonomie existante
  OU être formulées de manière suffisamment générales pour y être intégrées.

------------------------------------------------
3) CAS CLAIR
------------------------------------------------
Si la classification principale est claire et suffisante :
- N’utilise PAS "label_proposition"
- N’utilise PAS "sous_label_proposition"

IMPORTANT :
- "autre" n’est PAS une proposition.
- Les propositions ne remplacent JAMAIS la classification principale.

================================================
RÈGLES SUR "lieu"
================================================

- "lieu" = lieu CONCRET si identifiable
  (ex: "salle de classe", "cantine", "cour", "internat",
       "examen", "plateforme en ligne").
- Si non identifiable ou non pertinent : null.
- Ne pas confondre avec pôle, académie ou rectorat (ex: Lille).

================================================
RÈGLES SUR "key_word"
================================================

- "key_word" = liste de 2 à 5 mots-clés factuels.
- Ne pas dupliquer "label" ou "sous_label".
- Mots courts, sans phrases, sans ponctuation superflue.
- Objectif : aide à la statistique et au filtrage.

================================================
AIDE À L’INTERPRÉTATION — ACRONYMES
================================================

Les données peuvent contenir des acronymes.
Ils servent d’INDICES DE CONTEXTE, mais ne sont PAS fiables à 100%.

RÈGLES :
1) Utilise un acronyme UNIQUEMENT s’il est cohérent avec le texte "Analyse".
2) Ignore un acronyme si :
   - il n’a pas de définition,
   - il figure dans la liste "BRUIT",
   - il semble hors-sujet (initiales, faute de frappe).
3) Ne base JAMAIS une classification uniquement sur un acronyme.
4) Ne recopie JAMAIS les définitions dans la sortie.

ACRONYMES DÉFINIS :
{acronymes_json}

CODES BRUIT / FAUTES PROBABLES / INITIALES MÉDIATEURS :
{bruit_json}

================================================
PRIORITÉ DES SOURCES POUR CLASSIFIER
================================================

1) Texte "Analyse" (priorité maximale)
2) Champs métier structurés (Catégorie, Domaine, Sous-domaine, Nature de la saisine)
3) Acronymes (indices secondaires uniquement)

================================================
ENTRÉE — LISTE DES PLAINTES (JSON)
================================================

Tu dois produire EXACTEMENT un objet de sortie par plainte,
dans le MÊME ORDRE que la liste d’entrée.

PLAINTES :
{plaintes_json}

================================================
SORTIE ATTENDUE
================================================

RÉPONSE : UNIQUEMENT un tableau JSON,
sans texte libre, sans commentaire, sans métadonnées.
""".strip()


# ---------- APPEL GEMINI ----------
def enrich_batch(client: genai.Client, batch: List[dict]) -> List[dict]:
    """
    Appelle Gemini pour un batch.
    Retourne une liste d'objets finaux :
    - champs initiaux inchangés
    - + 4 champs enrichis minimaux
    """
    prompt = build_batch_prompt(batch)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash-lite",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=list[EnrichissementMinimal],
                ),
            )

            parsed_list = response.parsed

            # 🔒 GARDE-FOU CRITIQUE (cause de ton erreur NoneType)
            if parsed_list is None:
                raise ValueError(
                    "Réponse Gemini invalide : response.parsed est None "
                    "(JSON vide, tronqué ou non conforme)"
                )

            if not isinstance(parsed_list, list):
                raise ValueError(
                    f"Réponse Gemini invalide : type inattendu {type(parsed_list)}"
                )

            if len(parsed_list) != len(batch):
                raise ValueError(
                    f"Nombre d'objets retournés ({len(parsed_list)}) "
                    f"différent du nombre de plaintes en entrée ({len(batch)})."
                )

            final_batch: List[dict] = []
            for plainte_brute, enrichie in zip(batch, parsed_list):
                enriched_dict = enrichie.model_dump()
                final_obj = {**plainte_brute, **enriched_dict}
                final_batch.append(final_obj)

            return final_batch

        except Exception as e:
            msg = str(e)

            # ---- 429 : quota / rate limit ----
            if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                delay = 60
                try:
                    m1 = re.search(r"Please retry in\s+(\d+)(?:\.\d+)?s", msg)
                    m2 = re.search(r"retryDelay'\s*:\s*'(\d+)s'", msg)
                    m3 = re.search(r'"retryDelay"\s*:\s*"(\d+)s"', msg)

                    if m1:
                        delay = int(m1.group(1)) + 2
                    elif m2:
                        delay = int(m2.group(1)) + 2
                    elif m3:
                        delay = int(m3.group(1)) + 2
                except Exception:
                    pass

                print(f"[AVERTISSEMENT] 429 RESOURCE_EXHAUSTED. Pause {delay}s puis retry...")
                time.sleep(delay)
                continue

            # ---- 503 : service unavailable ----
            if "503" in msg or "UNAVAILABLE" in msg:
                if attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    print(
                        f"[AVERTISSEMENT] 503 UNAVAILABLE (tentative {attempt}/{MAX_RETRIES}). "
                        f"Nouvel essai dans {delay} secondes..."
                    )
                    time.sleep(delay)
                    continue

                print("[ERREUR] 503 UNAVAILABLE après plusieurs tentatives. Arrêt du traitement.")
                raise

            # ---- Autres erreurs : stop ----
            print(f"[ERREUR] Échec enrichissement batch : {e}")
            raise



# ---------- VÉRIFICATION AVANCEMENT (optionnel) ----------
def check_avancement():
    """Lance le vérificateur d'avancement dans un sous-processus."""
    checker_path = BASE_DIR / "avancement_checker.py"
    if checker_path.exists():
        print("\n[INFO] Lancement du vérificateur d'avancement...")
        try:
            subprocess.run([sys.executable, str(checker_path)], check=False)
        except Exception as e:
            print(f"[AVERTISSEMENT] Impossible de lancer le vérificateur : {e}")
    else:
        print(f"[AVERTISSEMENT] Fichier {checker_path} introuvable.")

# ---------- LOGIQUE PRINCIPALE ----------
def main():
    try:
        print("\n" + "=" * 60)
        print("🚀 ENRICHISSEMENT MINIMAL DES PLAINTES (GEMINI)")
        print("=" * 60)

        check_choice = input(
            "\nSouhaites-tu vérifier l'avancement actuel avant de commencer ? [O]ui / [N]on : "
        ).strip().lower()
        if check_choice in ("o", "oui"):
            check_avancement()
            input("\nAppuie sur Entrée pour continuer avec le traitement...")

        api_key = load_api_key()
        client = genai.Client(api_key=api_key)

        plaintes = load_all_plaintes()
        print(f"\n[INFO] Nombre total de plaintes dans le fichier d'entrée : {len(plaintes)}")
        print(f"[INFO] Fichier de sortie : {OUTPUT_JSON.resolve()}")

        existing_results, done_ids = load_existing_results()

        if existing_results:
            choice = input(
                "Un fichier de sortie existe déjà.\n"
                f"- {len(existing_results)} plaintes déjà enrichies.\n"
                "Que veux-tu faire ? [R]eprendre là où ça s'est arrêté / [E]craser et recommencer : "
            ).strip().lower()

            if choice == "e":
                print("[INFO] Écrasement du fichier de sortie et reprise à zéro.")
                existing_results = []
                done_ids = set()
                OUTPUT_JSON.unlink(missing_ok=True)
            else:
                print("[INFO] Reprise : les plaintes dont l'id est déjà présent seront ignorées.")

        results: List[dict] = list(existing_results)

        pending = [p for p in plaintes if p.get("id") not in done_ids]
        print(f"[INFO] Plaintes restantes à traiter : {len(pending)}")

        for start in range(0, len(pending), BATCH_SIZE):
            batch = pending[start : start + BATCH_SIZE]
            ids_batch = [p.get("id") for p in batch]
            print(f"\n[INFO] Traitement batch {start} -> {start + len(batch) - 1} (ids={ids_batch})")

            final_batch = enrich_batch(client, batch)

            results.extend(final_batch)
            for obj in final_batch:
                pid = obj.get("id")
                if pid is not None:
                    done_ids.add(pid)

            safe_write_json(OUTPUT_JSON, results)
            print(f"[OK] Batch de {len(final_batch)} plaintes enrichies et sauvegardées (total={len(results)}).")

        print(f"\n[OK] Traitement terminé. {len(results)} plaintes enrichies au total.")
        print(f"[OK] Résultat final dans : {OUTPUT_JSON.resolve()}")

    except Exception as e:
        print(f"[ERREUR] Une erreur s'est produite : {e}")
        print("[INFO] Tout ce qui a été enrichi avant l'erreur est déjà sauvegardé (écriture atomique).")

if __name__ == "__main__":
    main()
