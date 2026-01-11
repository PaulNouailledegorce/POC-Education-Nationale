# Schéma de l'arborescence du projet

```
EDN1/
├── back/                          # Backend - Traitement des données
│   ├── tests/                     # Tests et audits
│   │   ├── audit_gemini.py        # Audit de l'API Gemini
│   │   └── avancement_checker.py  # Vérification de l'avancement du traitement
│   │
│   ├── data/                      # Données du projet
│   │   ├── input/                 # Données d'entrée
│   │   │   └── Excel et data/     # Fichiers Excel et CSV sources
│   │   │       ├── concatenation.xlsx
│   │   │       ├── MEDIA2_Extraction_janv2022_ECE_12102025 ananymisé.xlsx
│   │   │       └── ... (autres fichiers Excel)
│   │   │
│   │   └── output/                # Données de sortie
│   │       ├── output.json
│   │       ├── output_tri.json
│   │       ├── output_tri_structure.json
│   │       ├── output_tri_structure2.json
│   │       ├── output_tri_structure.ndjson
│   │       └── ... (autres fichiers JSON/NDJSON)
│   │
│   └── projet/                    # Code du projet principal
│       ├── acronymes.py          # Dictionnaire des acronymes
│       ├── api_key.txt            # Clé API (à ne pas commiter)
│       ├── docker-compose.yml     # Configuration Docker pour Elasticsearch/Kibana
│       ├── extraction_excel_to_json.py      # Extraction Excel → JSON
│       ├── extraction_tri_excel_to_python.py # Extraction et tri Excel → JSON
│       ├── json_to_ndjson.py     # Conversion JSON → NDJSON
│       ├── nature_probleme.py     # Taxonomie des problèmes
│       ├── output_tri_structure.py # Enrichissement avec Gemini
│       └── requirements.txt       # Dépendances Python
│
├── front/                         # Frontend - Visualisation et Elastic
│   └── push_to_elastic.py        # Import des données dans Elasticsearch
│
├── LICENSE                        # Licence du projet
└── OuSuisJe.txt                   # Fichier de documentation
```

## Description des dossiers

### `back/tests/`
Contient tous les scripts de tests et d'audits :
- **audit_gemini.py** : Tests de performance et de fiabilité de l'API Gemini
- **avancement_checker.py** : Vérification de l'état d'avancement du traitement des plaintes

### `back/data/input/`
Contient les données sources du projet :
- Fichiers Excel et CSV bruts
- Données non traitées

### `back/data/output/`
Contient les données traitées :
- Fichiers JSON structurés
- Fichiers NDJSON pour l'import dans Elasticsearch
- Fichiers de sauvegarde

### `back/projet/`
Contient le code principal du projet :
- Scripts d'extraction et de transformation des données
- Configuration et dépendances
- Modules de traitement (acronymes, nature_probleme)

### `front/`
Contient le code lié à la visualisation et à Elasticsearch :
- Scripts d'import dans Elasticsearch
- (À venir : code de visualisation)

## Chemins relatifs mis à jour

Tous les chemins dans les scripts Python ont été mis à jour pour refléter la nouvelle structure :
- Les scripts de tests pointent vers `../projet/` et `../data/output/`
- Les scripts du projet pointent vers `../data/input/` et `../data/output/`
- Le script front pointe vers `../back/data/output/`

