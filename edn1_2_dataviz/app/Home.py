import streamlit as st

from edn1_2_dataviz.app.utils.semantic import load_semantic

st.set_page_config(page_title="EDN1.2 - Dataviz", layout="wide")

semantic = load_semantic()
title = semantic.get("meta", {}).get("title", "EDN1.2 - Dataviz")
desc = semantic.get("meta", {}).get("description", "")

st.title(title)
if desc:
    st.write(desc)

st.markdown(
    """
### Navigation
- **Exploration** : table filtrable et recherche plein texte.
- **Pivot** : mini builder (axe, split, métrique, Top N).
- **Presets** : vues prêtes définies dans le YAML.
- **Qualité & données** : statistiques, complétude, top valeurs.

Placez vos fichiers JSON/JSONL dans `data/input/`, lancez l'ETL puis la base
DuckDB sera disponible pour l'app.
"""
)

