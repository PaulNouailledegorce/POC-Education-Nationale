from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import duckdb
import streamlit as st

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "edn1.duckdb"


@lru_cache
def _open_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4")
    return con


@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    """Retourne une connexion DuckDB partagée (cache Streamlit)."""
    db_env = os.getenv("EDN1_DUCKDB_PATH")
    path = Path(db_env) if db_env else DEFAULT_DB_PATH
    return _open_connection(path)

