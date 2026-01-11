from __future__ import annotations

from typing import Dict, List, Tuple

import duckdb


def _filter_clause(filters: Dict[str, List], exclude: str | None = None) -> Tuple[str, List]:
    clauses: List[str] = []
    params: List = []
    for col, values in filters.items():
        if exclude and col == exclude:
            continue
        if not values:
            continue
        placeholders = ", ".join(["?"] * len(values))
        clauses.append(f"{col} IN ({placeholders})")
        params.extend(values)
    if clauses:
        return " AND ".join(clauses), params
    return "", []


def _date_clause(date_range) -> Tuple[str, List]:
    if not date_range:
        return "", []
    start, end = date_range
    if start and end:
        return "date_arrivee BETWEEN ? AND ?", [start, end]
    if start:
        return "date_arrivee >= ?", [start]
    if end:
        return "date_arrivee <= ?", [end]
    return "", []


def distinct_values(
    con: duckdb.DuckDBPyConnection,
    column: str,
    filters: Dict[str, List] | None = None,
    date_range=None,
) -> List[str]:
    """
    Valeurs distinctes d'une colonne, restreintes par les filtres déjà posés
    (excluant la colonne courante) et la plage de dates.
    """
    filters = filters or {}
    clauses: List[str] = []
    params: List = []

    f_clause, f_params = _filter_clause(filters, exclude=column)
    if f_clause:
        clauses.append(f_clause)
        params.extend(f_params)

    d_clause, d_params = _date_clause(date_range)
    if d_clause:
        clauses.append(d_clause)
        params.extend(d_params)

    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    sql = f"""
    SELECT DISTINCT {column}
    FROM v_saisines
    {where}
    AND {column} IS NOT NULL
    ORDER BY 1
    """
    rows = con.execute(sql, params).fetchall()
    return [r[0] for r in rows if r and r[0] is not None]


def date_bounds(con: duckdb.DuckDBPyConnection) -> Tuple:
    """Bornes min/max sur date_arrivee."""
    res = con.execute(
        "SELECT min(date_arrivee), max(date_arrivee) FROM v_saisines"
    ).fetchone()
    return res if res else (None, None)

