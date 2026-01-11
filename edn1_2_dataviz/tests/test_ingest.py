from pathlib import Path

import duckdb

from edn1_2_dataviz.etl import ingest_json_to_parquet


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(f"{row}\n".replace("'", '"'))


def test_deduplication_on_id(tmp_path: Path):
    input_dir = tmp_path / "input"
    parquet_dir = tmp_path / "parquet"
    rows = [
        {
            "id": 1,
            "Date arrivée": "2022-01-01",
            "Analyse": "premier",
            "key_word": ["a"],
        },
        {
            "id": 1,
            "Date arrivée": "2022-02-01",
            "Analyse": "plus récent",
            "key_word": ["b"],
        },
    ]
    _write_jsonl(input_dir / "data.jsonl", rows)

    ingest_json_to_parquet.run(input_dir=input_dir, parquet_dir=parquet_dir)

    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{parquet_dir / 'saisines.parquet'}')").df()
    assert len(df) == 1
    assert df.loc[0, "analyse"] == "plus récent"

    kw_df = con.execute(f"SELECT * FROM read_parquet('{parquet_dir / 'keywords.parquet'}')").df()
    assert set(kw_df["keyword"]) == {"a", "b"}

