import streamlit as st

from edn1_2_dataviz.app.utils.duckdb_conn import get_connection
from edn1_2_dataviz.app.utils.query_builder import QueryBuilder
from edn1_2_dataviz.app.utils.semantic import (
    dimension_labels,
    load_semantic,
    presets as load_presets,
    text_fields,
)

st.set_page_config(page_title="Presets", layout="wide")

semantic = load_semantic()
labels = dimension_labels(semantic)
text_fields_list = text_fields(semantic)
presets_conf = load_presets(semantic)

con = get_connection()
qb = QueryBuilder(text_fields_list)

st.title("Vues prêtes (presets)")

if not presets_conf:
    st.info("Aucun preset défini dans config/semantic_layer.yaml.")
    st.stop()

names = [p["name"] for p in presets_conf]
choice = st.selectbox("Sélection", options=names)
preset = next(p for p in presets_conf if p["name"] == choice)

filters = {k: v for k, v in (preset.get("filters") or {}).items()}
group_by = preset.get("group_by")
split_by = preset.get("split_by")
metric = preset.get("metric", "count")
top_n = preset.get("top_n")
include_other = preset.get("include_other", True)
time_grain = preset.get("time_grain")

st.caption(preset.get("description", ""))

query = qb.pivot_query(
    group_by=group_by,
    split_by=split_by,
    metric=metric,
    filters=filters,
    search_text="",
    match_all=False,
    date_range=None,
    top_n=top_n,
    include_other=include_other,
    time_grain=time_grain,
)

df = con.execute(query.sql, query.params).df()

if df.empty:
    st.info("0 résultat pour ce preset.")
else:
    st.dataframe(df, width="stretch", hide_index=True)

