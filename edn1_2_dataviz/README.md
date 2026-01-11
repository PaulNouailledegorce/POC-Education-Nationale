# EDN1.2-Dataviz (DuckDB + Parquet + Streamlit)

Mini-produit de datavisualisation pour les saisines (sans Elastic).

## Prérequis
- Python 3.11+
- `pip install -r requirements.txt`

## Arborescence
```
edn1_2_dataviz/
  app/                 # App Streamlit (Home + pages)
  config/semantic_layer.yaml
  data/
    input/             # Déposez vos JSON/JSONL ici
    parquet/           # Parquet générés
    edn1.duckdb        # Base locale (générée)
  etl/
    ingest_json_to_parquet.py
    build_duckdb.py
    schema.py
  tests/
```

## Pipeline ETL
1. Déposer vos fichiers `*.json` ou `*.jsonl` dans `data/input/`.
2. Ingestion -> Parquet (avec déduplication sur `id`) :
   ```
   python -m edn1_2_dataviz.etl.ingest_json_to_parquet
   ```
3. Construction/rafraîchissement de la base DuckDB + vues :
   ```
   python -m edn1_2_dataviz.etl.build_duckdb
   ```

## Lancer l'app Streamlit
```
streamlit run edn1_2_dataviz/app/Home.py
```

## Configuration sémantique
- Modifier `etl/schema.py` pour ajouter/mapper des champs source -> snake_case.
- Modifier `config/semantic_layer.yaml` pour exposer les dimensions, labels,
  presets, limites d'affichage.

## Tests
```
pytest
```

## Notes
- Recherches plein texte sur `analyse` et `key_word_str`.
- Top N / regroupement "Autre" gérés côté DuckDB (pas de pandas massif).
- Parquet et DuckDB restent locaux; aucune dépendance Elastic/Kibana.

