import streamlit as st
import altair as alt

from edn1_2_dataviz.app.utils.duckdb_conn import get_connection
from edn1_2_dataviz.app.utils.filters import date_bounds, distinct_values
from edn1_2_dataviz.app.utils.query_builder import QueryBuilder
from edn1_2_dataviz.app.utils.semantic import dimension_labels, load_semantic, text_fields

st.set_page_config(page_title="Pivot", layout="wide")

semantic = load_semantic()
labels = dimension_labels(semantic)
display_conf = semantic.get("display", {})
text_fields_list = text_fields(semantic)

con = get_connection()
qb = QueryBuilder(text_fields_list)

st.title("Pivot / Mini builder")

dimensions = list(semantic.get("dimensions", {}).keys())
string_dims = [d for d in dimensions if not d.startswith("date_")]

with st.sidebar:
    st.header("Configuration")
    group_by = st.selectbox("Axe X", options=dimensions, format_func=lambda x: labels.get(x, x))
    split_by = st.selectbox(
        "Découper par (optionnel)",
        options=[""] + string_dims,
        format_func=lambda x: labels.get(x, "Aucun") if x else "Aucun",
    )
    metric = st.selectbox("Métrique", options=["count", "count_distinct"])
    top_n = st.number_input(
        "Top N (0 = tous)",
        min_value=0,
        max_value=500,
        value=int(display_conf.get("default_top_n", 20)),
        step=1,
    )
    include_other = st.checkbox("Regrouper le reste en 'Autre'", value=True)
    time_grain = st.selectbox(
        "Granularité date (si axe date)",
        options=["", "day", "week", "month", "quarter", "year"],
        index=3,
    )

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
    search_text = st.text_input("Recherche analyse / mots-clés", placeholder="mot1 mot2")
    match_all = st.checkbox("Faire correspondre tous les mots", value=False)

query = qb.pivot_query(
    group_by=group_by,
    split_by=split_by or None,
    metric=metric,
    filters=filters,
    search_text=search_text,
    match_all=match_all,
    date_range=date_range if isinstance(date_range, tuple) else None,
    top_n=top_n or None,
    include_other=include_other,
    time_grain=time_grain or None,
)

df = con.execute(query.sql, query.params).df()

if df.empty:
    st.info("0 résultat pour ce pivot.")
else:
    st.dataframe(df, width="stretch", hide_index=True)
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("x:O", title=labels.get(group_by, group_by)),
            y=alt.Y("value:Q", title="Valeur"),
            color="split_by:N" if "split_by" in df.columns and df["split_by"].notna().any() else alt.value("#4c78a8"),
            tooltip=list(df.columns),
        )
        .properties(height=400)
    )
    st.altair_chart(chart, width="stretch")

