from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


def _build_search_clause(
    search_text: str, match_all: bool, fields: List[str]
) -> Tuple[str, List[str]]:
    words = [w for w in search_text.strip().split() if w]
    if not words:
        return "", []

    clauses = []
    params: List[str] = []
    for word in words:
        sub = " OR ".join([f"{field} ILIKE ?" for field in fields])
        clauses.append(f"({sub})")
        like_value = f"%{word}%"
        params.extend([like_value] * len(fields))

    joiner = " AND " if match_all else " OR "
    return f"({joiner.join(clauses)})", params


def _build_filter_clause(filters: Dict[str, List]) -> Tuple[str, List]:
    clauses: List[str] = []
    params: List = []
    for col, values in filters.items():
        if not values:
            continue
        placeholders = ", ".join(["?"] * len(values))
        clauses.append(f"{col} IN ({placeholders})")
        params.extend(values)
    if clauses:
        return " AND ".join(clauses), params
    return "", []


def _build_date_clause(date_range) -> Tuple[str, List]:
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


@dataclass
class Query:
    sql: str
    params: List


class QueryBuilder:
    """Construit des requêtes SQL DuckDB paramétrées."""

    def __init__(self, text_fields: List[str]) -> None:
        self.text_fields = text_fields

    def exploration_query(
        self,
        filters: Dict[str, List],
        search_text: str,
        match_all: bool,
        date_range=None,
        limit: int = 2000,
    ) -> Query:
        clauses: List[str] = []
        params: List = []

        filter_clause, filter_params = _build_filter_clause(filters)
        if filter_clause:
            clauses.append(filter_clause)
            params.extend(filter_params)

        date_clause, date_params = _build_date_clause(date_range)
        if date_clause:
            clauses.append(date_clause)
            params.extend(date_params)

        search_clause, search_params = _build_search_clause(
            search_text, match_all, self.text_fields
        )
        if search_clause:
            clauses.append(search_clause)
            params.extend(search_params)

        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        sql = f"""
        SELECT *
        FROM v_saisines
        {where}
        ORDER BY coalesce(date_arrivee, date_cloture) DESC NULLS LAST, id
        LIMIT {limit}
        """
        return Query(sql=sql, params=params)

    def pivot_query(
        self,
        group_by: str,
        split_by: str | None,
        metric: str,
        filters: Dict[str, List],
        search_text: str,
        match_all: bool,
        date_range=None,
        top_n: int | None = None,
        include_other: bool = True,
        time_grain: str | None = None,
    ) -> Query:
        clauses: List[str] = []
        params: List = []

        filter_clause, filter_params = _build_filter_clause(filters)
        if filter_clause:
            clauses.append(filter_clause)
            params.extend(filter_params)

        date_clause, date_params = _build_date_clause(date_range)
        if date_clause:
            clauses.append(date_clause)
            params.extend(date_params)

        search_clause, search_params = _build_search_clause(
            search_text, match_all, self.text_fields
        )
        if search_clause:
            clauses.append(search_clause)
            params.extend(search_params)

        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        metric_expr = "count(*)" if metric == "count" else "count(distinct id)"
        x_dim = group_by
        if time_grain and group_by.startswith("date_"):
            # DuckDB permet date_trunc sur les dates
            x_dim = f"date_trunc('{time_grain}', {group_by})"

        split_select = f", {split_by} AS split_by" if split_by else ""
        split_group = ", split_by" if split_by else ""

        base_sql = f"""
        SELECT {x_dim} AS x{split_select}, {metric_expr} AS value
        FROM v_saisines
        {where}
        GROUP BY 1{split_group}
        """

        if top_n:
            rank_partition = "PARTITION BY split_by" if split_by else ""
            ranked = f"""
            WITH ranked AS (
                SELECT *, row_number() OVER ({rank_partition} ORDER BY value DESC) AS rnk
                FROM ({base_sql})
            )
            SELECT
                CASE WHEN rnk <= {top_n} THEN x ELSE 'Autre' END AS x,
                {('split_by' if split_by else 'NULL')} AS split_by,
                sum(value) AS value
            FROM ranked
            {"" if include_other else "WHERE rnk <= " + str(top_n)}
            GROUP BY 1, 2
            ORDER BY value DESC
            """
            sql = ranked
        else:
            sql = (
                base_sql
                + " ORDER BY value DESC"
            )

        return Query(sql=sql, params=params)

