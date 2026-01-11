"""
Définition du schéma et fonctions de normalisation des enregistrements
bruts (JSON/JSONL) vers un format tabulaire stable.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import unicodedata
from typing import Dict, Iterable, List, Tuple

logger = logging.getLogger(__name__)

# Mapping explicite des noms de champs source vers des noms normalisés.
FIELD_MAPPING: Dict[str, str] = {
    "id": "id",
    "Date arrivée": "date_arrivee",
    "Date d'arrivée": "date_arrivee",
    "Date clôture fiche": "date_cloture",
    "Date cloture fiche": "date_cloture",
    "Pôle en charge": "pole_en_charge",
    "Pole en charge": "pole_en_charge",
    "Catégorie": "categorie",
    "Sous-catégorie": "sous_categorie",
    "Domaine": "domaine",
    "Sous-domaine": "sous_domaine",
    "Aspect contextuel": "aspect_contextuel",
    "Nature de la saisine": "nature_saisine",
    "Réclamation : position du médiateur": "reclamation_position_mediateur",
    "Impact de l'appui du médiateur": "impact_appui_mediateur",
    "Analyse": "analyse",
    "label": "label",
    "sous_label": "sous_label",
    "lieu": "lieu",
    "key_word": "key_word",
    "keywords": "key_word",
    "key_words": "key_word",
    "label_proposition": "label_proposition",
    "sous_label_proposition": "sous_label_proposition",
}

DATE_FIELDS = {"date_arrivee", "date_cloture"}
TEXT_FIELDS = {"analyse", "key_word_str"}
LIST_FIELDS = {"key_word"}


def slugify(name: str) -> str:
    """Convertit un libellé libre en snake_case ASCII stable."""
    normalized = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_{2,}", "_", normalized).strip("_")
    return normalized


def map_field(name: str) -> str:
    """Retourne le nom de champ normalisé (mapping explicite ou slug)."""
    if name in FIELD_MAPPING:
        return FIELD_MAPPING[name]
    return slugify(name)


def parse_date(value) -> dt.date | None:
    """Essaie de parser une date au format ISO (YYYY-MM-DD) ou proche."""
    if value is None or value == "":
        return None
    if isinstance(value, dt.date):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, (int, float)):
        # Interprétation timestamp (secondes)
        try:
            return dt.datetime.utcfromtimestamp(value).date()
        except Exception:  # pragma: no cover - cas rare
            return None
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return dt.datetime.strptime(value[:10], fmt).date()
            except ValueError:
                continue
        try:
            return dt.date.fromisoformat(value[:10])
        except Exception:
            logger.warning("Date invalide ignorée: %s", value)
            return None
    return None


def normalize_keywords(value) -> List[str]:
    """Transforme le champ key_word sous forme liste de chaînes normalisées."""
    if value is None:
        return []
    if isinstance(value, list):
        raw_values = value
    elif isinstance(value, str):
        raw_values = re.split(r"[;,]", value)
    else:
        raw_values = [str(value)]
    keywords = [v.strip().lower() for v in raw_values if v and str(v).strip()]
    # Filtrer doublons tout en conservant l'ordre
    seen = set()
    uniq: List[str] = []
    for kw in keywords:
        if kw not in seen:
            uniq.append(kw)
            seen.add(kw)
    return uniq


def normalize_record(record: Dict) -> Tuple[Dict, List[Dict[str, str]]]:
    """
    Normalise un enregistrement brut.

    Retourne (main_row, keywords_rows). main_row inclut key_word_str.
    """
    norm: Dict = {}
    keyword_list: List[str] = []

    for raw_key, raw_val in record.items():
        norm_key = map_field(raw_key)
        if norm_key == "key_word":
            keyword_list = normalize_keywords(raw_val)
            continue

        if norm_key in DATE_FIELDS:
            norm[norm_key] = parse_date(raw_val)
        else:
            norm[norm_key] = raw_val

    if "id" not in norm:
        raise ValueError("Champ 'id' manquant dans l'enregistrement.")

    # key_word_str pour recherche simple
    norm["key_word_str"] = " ".join(keyword_list) if keyword_list else None

    # Table enfant keywords
    keyword_rows = [{"id": norm["id"], "keyword": kw} for kw in keyword_list]

    return norm, keyword_rows


__all__ = [
    "FIELD_MAPPING",
    "DATE_FIELDS",
    "TEXT_FIELDS",
    "LIST_FIELDS",
    "slugify",
    "map_field",
    "parse_date",
    "normalize_keywords",
    "normalize_record",
]

