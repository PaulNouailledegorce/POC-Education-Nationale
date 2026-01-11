PY=python
STREAMLIT=streamlit

install:
	$(PY) -m pip install -r requirements.txt

etl:
	$(PY) -m edn1_2_dataviz.etl.ingest_json_to_parquet
	$(PY) -m edn1_2_dataviz.etl.build_duckdb

app:
	$(STREAMLIT) run edn1_2_dataviz/app/Home.py

test:
	pytest

