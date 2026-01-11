"""
Construit/actualise la base DuckDB locale à partir des Parquet générés.

Usage:
    python -m edn1_2_dataviz.etl.build_duckdb
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_views(con: duckdb.DuckDBPyConnection, parquet_dir: Path) -> None:
    main_path = parquet_dir / "saisines.parquet"
    kw_path = parquet_dir / "keywords.parquet"

    if not main_path.exists():
        raise FileNotFoundError(f"Parquet principal manquant: {main_path}")

    con.execute(
        f"CREATE OR REPLACE VIEW v_saisines AS SELECT * FROM read_parquet('{main_path.as_posix()}')"
    )

    if kw_path.exists():
        con.execute(
            f"CREATE OR REPLACE VIEW v_keywords AS SELECT * FROM read_parquet('{kw_path.as_posix()}')"
        )
    else:
        con.execute(
            "CREATE OR REPLACE VIEW v_keywords AS "
            "SELECT NULL::BIGINT AS id, NULL::VARCHAR AS keyword WHERE FALSE"
        )
    logger.info("Vues DuckDB créées/rafraîchies.")


def run(db_path: Path | None = None, parquet_dir: Path | None = None) -> None:
    base = Path(__file__).resolve().parents[1]
    db_path = db_path or base / "data" / "edn1.duckdb"
    parquet_dir = parquet_dir or base / "data" / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4")
    create_views(con, parquet_dir)
    con.close()
    logger.info("Base DuckDB prête: %s", db_path)


def main():
    run()


if __name__ == "__main__":
    main()

