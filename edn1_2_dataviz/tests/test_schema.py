import datetime as dt

import pytest

from edn1_2_dataviz.etl import schema


def test_slugify_accents():
    assert schema.slugify("Pôle en charge") == "pole_en_charge"


def test_parse_date_iso():
    d = schema.parse_date("2022-01-31")
    assert d == dt.date(2022, 1, 31)


def test_normalize_record_keywords():
    main, keywords = schema.normalize_record(
        {
            "id": 1,
            "Date arrivée": "2022-01-31",
            "key_word": ["A", "B", "A"],
        }
    )
    assert main["date_arrivee"] == dt.date(2022, 1, 31)
    assert main["key_word_str"] == "a b"
    assert keywords == [{"id": 1, "keyword": "a"}, {"id": 1, "keyword": "b"}]


def test_missing_id_raises():
    with pytest.raises(ValueError):
        schema.normalize_record({"Analyse": "test"})

