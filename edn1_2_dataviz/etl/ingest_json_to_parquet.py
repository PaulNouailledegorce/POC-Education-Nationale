"""
Ingestion des fichiers JSON/JSONL vers Parquet + DuckDB.

Usage:
    python -m edn1_2_dataviz.etl.ingest_json_to_parquet
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import duckdb
import pyarrow as pa

from .schema import normalize_record

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def iter_input_files(input_dir: Path) -> Iterable[Path]:
    """Liste les fichiers .json et .jsonl du dossier input."""
    for path in sorted(input_dir.glob("*.json*")):
        if path.is_file():
            yield path


def read_json_records(path: Path) -> Iterable[Dict]:
    """Yield les enregistrements d'un fichier JSON ou JSONL."""
    if path.suffix.lower() == ".jsonl" or path.name.lower().endswith(".ndjson"):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.error("Ligne invalide dans %s: %s", path.name, exc)
    else:
        with path.open("r", encoding="utf-8") as f:
            try:
                payload = json.load(f)
            except json.JSONDecodeError as exc:
                logger.error("JSON invalide dans %s: %s", path.name, exc)
                return
        if isinstance(payload, list):
            for row in payload:
                yield row
        elif isinstance(payload, dict):
            yield payload
        else:
            logger.error("Format inattendu dans %s (ni liste ni objet)", path.name)


def collect_records(input_dir: Path) -> Tuple[List[Dict], List[Dict]]:
    """Charge et normalise tous les enregistrements du dossier input/."""
    mains: List[Dict] = []
    keywords: List[Dict] = []
    files = list(iter_input_files(input_dir))
    if not files:
        logger.warning("Aucun fichier .json/.jsonl trouvé dans %s", input_dir)
    for path in files:
        logger.info("Lecture de %s", path.name)
        for raw in read_json_records(path):
            try:
                main_row, keyword_rows = normalize_record(raw)
                mains.append(main_row)
                keywords.extend(keyword_rows)
            except Exception as exc:
                logger.exception("Enregistrement ignoré (fichier %s): %s", path.name, exc)
    return mains, keywords


def _table_from_pylist(rows: List[Dict]) -> pa.Table:
    """Construit une table Arrow à partir d'une liste de dicts."""
    return pa.Table.from_pylist(rows) if rows else pa.table({})


def write_parquet_with_dedup(
    mains: List[Dict], keywords: List[Dict], parquet_dir: Path
) -> None:
    """Fusionne avec l'existant en supprimant les doublons sur id."""
    parquet_dir.mkdir(parents=True, exist_ok=True)
    main_path = parquet_dir / "saisines.parquet"
    kw_path = parquet_dir / "keywords.parquet"

    con = duckdb.connect(":memory:")

    # Table principale
    new_main = _table_from_pylist(mains)
    con.register("new_main", new_main)
    if main_path.exists():
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW existing_main AS "
            f"SELECT * FROM read_parquet('{main_path.as_posix()}')"
        )
    else:
        con.execute("CREATE OR REPLACE TEMP VIEW existing_main AS SELECT * FROM new_main WHERE 1=0")

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW combined_main AS
        SELECT * FROM (
            SELECT * FROM existing_main
            UNION ALL
            SELECT * FROM new_main
        )
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY coalesce(date_arrivee, date_cloture) DESC NULLS LAST, id
        ) = 1
        """
    )
    con.execute(
        f"COPY (SELECT * FROM combined_main ORDER BY date_arrivee NULLS LAST, id) "
        f"TO '{main_path.as_posix()}' (FORMAT 'parquet');"
    )
    logger.info("Parquet principal écrit: %s", main_path)

    # Table enfant keywords (optionnelle)
    new_kw = _table_from_pylist(keywords)
    con.register("new_kw", new_kw)
    if kw_path.exists():
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW existing_kw AS SELECT * FROM read_parquet('{kw_path.as_posix()}')"
        )
    else:
        con.execute("CREATE OR REPLACE TEMP VIEW existing_kw AS SELECT * FROM new_kw WHERE 1=0")

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW combined_kw AS
        SELECT * FROM (
            SELECT * FROM existing_kw
            UNION ALL
            SELECT * FROM new_kw
        )
        QUALIFY row_number() OVER (
            PARTITION BY id, keyword
            ORDER BY id
        ) = 1
        """
    )
    con.execute(
        f"COPY (SELECT * FROM combined_kw ORDER BY id) TO '{kw_path.as_posix()}' (FORMAT 'parquet');"
    )
    logger.info("Parquet keywords écrit: %s", kw_path)


def run(input_dir: Path | None = None, parquet_dir: Path | None = None) -> None:
    base = Path(__file__).resolve().parents[1]
    input_dir = input_dir or base / "data" / "input"
    parquet_dir = parquet_dir or base / "data" / "parquet"

    mains, keywords = collect_records(input_dir)
    if not mains:
        logger.warning("Aucune donnée ingérée.")
        return
    write_parquet_with_dedup(mains, keywords, parquet_dir)


def main():
    run()


if __name__ == "__main__":
    main()

