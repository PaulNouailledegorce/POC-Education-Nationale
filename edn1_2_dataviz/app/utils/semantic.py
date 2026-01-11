from __future__ import annotations

import functools
from pathlib import Path
from typing import Any, Dict

import streamlit as st
import yaml

SEMANTIC_PATH = Path(__file__).resolve().parents[2] / "config" / "semantic_layer.yaml"


@functools.lru_cache
def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@st.cache_data(show_spinner=False)
def load_semantic() -> Dict[str, Any]:
    """Charge le YAML de couche sémantique (cache côté Streamlit)."""
    return _load_yaml(SEMANTIC_PATH)


def dimension_labels(semantic: Dict[str, Any]) -> Dict[str, str]:
    return {k: v.get("label", k) for k, v in semantic.get("dimensions", {}).items()}


def text_fields(semantic: Dict[str, Any]) -> list[str]:
    return list(semantic.get("text_fields", []))


def presets(semantic: Dict[str, Any]) -> list[Dict[str, Any]]:
    return list(semantic.get("presets", []))

