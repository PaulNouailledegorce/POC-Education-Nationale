import streamlit as st

from edn1_2_dataviz.app.utils.duckdb_conn import get_connection
from edn1_2_dataviz.app.utils.semantic import dimension_labels, load_semantic

st.set_page_config(page_title="Qualité & données", layout="wide")

semantic = load_semantic()
labels = dimension_labels(semantic)
con = get_connection()

st.title("Qualité & données")

# Volumétrie globale
counts = con.execute(
    "SELECT count(*) AS nb_lignes, count(DISTINCT id) AS nb_ids FROM v_saisines"
).fetchone()
st.metric("Lignes", counts[0])
st.metric("IDs uniques", counts[1])

st.subheader("Taux de null par champ")
null_rows = []
for col in semantic.get("dimensions", {}).keys():
    null_ratio = con.execute(
        f"SELECT (sum(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)::DOUBLE / nullif(count(*),0)) AS ratio FROM v_saisines"
    ).fetchone()[0]
    null_rows.append({"champ": labels.get(col, col), "ratio_null": null_ratio})
st.dataframe(null_rows, hide_index=True, width="stretch")

st.subheader("Top valeurs (10)")
for col in list(semantic.get("dimensions", {}).keys())[:6]:
    top = con.execute(
        f"SELECT {col} AS valeur, count(*) AS freq FROM v_saisines "
        f"WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY freq DESC LIMIT 10"
    ).df()
    st.markdown(f"**{labels.get(col, col)}**")
    st.dataframe(top, hide_index=True, width="stretch")

st.subheader("Top keywords")
top_kw = con.execute(
    "SELECT keyword, count(*) AS freq FROM v_keywords "
    "WHERE keyword IS NOT NULL GROUP BY 1 ORDER BY freq DESC LIMIT 30"
).df()
if top_kw.empty:
    st.info("Pas de keywords disponibles.")
else:
    st.dataframe(top_kw, hide_index=True, width="stretch")

