import pandas as pd
import streamlit as st

from edn1_2_dataviz.app.utils.duckdb_conn import get_connection
from edn1_2_dataviz.app.utils.filters import date_bounds, distinct_values
from edn1_2_dataviz.app.utils.query_builder import QueryBuilder
from edn1_2_dataviz.app.utils.semantic import (
    dimension_labels,
    load_semantic,
    text_fields,
)

st.set_page_config(page_title="Exploration", layout="wide")

semantic = load_semantic()
labels = dimension_labels(semantic)
text_fields_list = text_fields(semantic)
display_conf = semantic.get("display", {})
max_rows = display_conf.get("max_rows", 2000)

con = get_connection()
qb = QueryBuilder(text_fields_list)

st.title("Exploration des saisines")
st.caption("Filtres, recherche plein texte, export CSV.")

with st.sidebar:
    st.header("Filtres")
    min_date, max_date = date_bounds(con)
    date_range = st.date_input(
        "Plage de dates (arrivée)",
        value=(min_date, max_date) if min_date and max_date else None,
    )
    filters = {}
    for dim, meta in semantic.get("dimensions", {}).items():
        if dim.startswith("date_"):
            continue
        options = distinct_values(
            con,
            dim,
            filters=filters,
            date_range=date_range if isinstance(date_range, tuple) else None,
        )
        filters[dim] = st.multiselect(labels.get(dim, dim), options)

    st.divider()
    search_text = st.text_input(
        "Recherche analyse / mots-clés",
        placeholder="mot1 mot2",
    )
    match_all = st.checkbox("Faire correspondre tous les mots", value=False)

query = qb.exploration_query(
    filters=filters,
    search_text=search_text,
    match_all=match_all,
    date_range=date_range if isinstance(date_range, tuple) else None,
    limit=max_rows,
)

df = con.execute(query.sql, query.params).df()

if df.empty:
    st.info("0 résultat avec les filtres actuels.")
else:
    st.success(f"{len(df)} lignes affichées (max {max_rows}).")
    st.dataframe(df, width="stretch", hide_index=True)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Exporter CSV (résultat filtré)",
        data=csv_bytes,
        file_name="edn1_exploration.csv",
        mime="text/csv",
    )

