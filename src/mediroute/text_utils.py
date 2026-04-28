"""Text normalization, CSV-list parsing, and matching helpers."""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Iterable

from .lexicons import NEGATION_TERMS

NULL_LIKE = {"", "null", "none", "nan", "na", "n/a", "[]", "{}"}


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in NULL_LIKE:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize(value: object) -> str:
    text = clean_text(value).lower()
    text = text.replace("–", "-").replace("—", "-")
    return text


def to_snake_case(value: str) -> str:
    """Normalize messy CSV / Databricks column names.

    Examples:
    - ``mongo DB`` -> ``mongo_db``
    - ``address_stateOrRegion`` -> ``address_state_or_region``
    - ``numberDoctors`` -> ``number_doctors``
    """
    s = str(value).strip()
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = re.sub(r"[^0-9A-Za-z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s


def parse_list_value(value: object) -> list[str]:
    """Parse list-like CSV cells safely.

    The official Virtue Foundation CSV stores columns such as ``specialties``,
    ``procedure``, ``equipment`` and ``capability`` as JSON-looking strings.
    This function accepts real lists, JSON arrays, Python-list strings,
    semicolon/comma-delimited fallbacks, and null-like tokens.
    """
    if isinstance(value, list):
        return compact_list(str(x) for x in value if clean_text(x))
    text = clean_text(value)
    if not text:
        return []

    parsed = None
    if text.startswith("[") and text.endswith("]"):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
                break
            except Exception:
                parsed = None
        if isinstance(parsed, list):
            return compact_list(str(x) for x in parsed if clean_text(x))

    # Last-resort split only for exported semicolon strings. Avoid splitting long
    # descriptive prose on commas unless it clearly looks like a list.
    if ";" in text:
        return compact_list(x.strip() for x in text.split(";") if clean_text(x))
    if "," in text and len(text) < 160:
        return compact_list(x.strip() for x in text.split(",") if clean_text(x))
    return [text]


def is_medical_fact(text: str) -> bool:
    """Filter location/contact facts that are useful as citations but not medical facts."""
    t = normalize(text)
    if not t:
        return False
    non_medical_prefixes = (
        "has a location at",
        "located in",
        "listed as",
        "contact",
        "phone",
        "website",
        "facebook",
        "twitter",
        "linkedin",
        "instagram",
        "address",
    )
    return not t.startswith(non_medical_prefixes)


def contains_any(text: str, phrases: Iterable[str]) -> bool:
    text_norm = normalize(text)
    return any(p.lower() in text_norm for p in phrases)


def is_negated_near(text: str, phrase: str, window: int = 35) -> bool:
    """Detect simple negation near a phrase.

    This is intentionally conservative: a negated phrase is not counted as evidence.
    Example: "no operating theatre" should not support surgical capability.
    """
    text_norm = normalize(text)
    phrase_norm = phrase.lower()
    idx = text_norm.find(phrase_norm)
    if idx == -1:
        return False
    start = max(0, idx - window)
    before = text_norm[start:idx]
    combined = text_norm[start : min(len(text_norm), idx + len(phrase_norm) + window)]
    return any(term in before or term in combined for term in NEGATION_TERMS)


def matched_aliases(text: str, aliases: list[str]) -> list[str]:
    text_norm = normalize(text)
    hits: list[str] = []
    for a in aliases:
        if a.lower() in text_norm and not is_negated_near(text_norm, a):
            hits.append(a)
    return hits


def compact_list(values: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for v in values:
        v = clean_text(v)
        if not v:
            continue
        key = v.lower()
        if key not in seen:
            seen.add(key)
            out.append(v)
    return out


def snippet_for(text: str, phrase: str, radius: int = 90) -> str:
    text_clean = clean_text(text)
    idx = text_clean.lower().find(phrase.lower())
    if idx == -1:
        return text_clean[: radius * 2]
    start = max(0, idx - radius)
    end = min(len(text_clean), idx + len(phrase) + radius)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text_clean) else ""
    return f"{prefix}{text_clean[start:end]}{suffix}"
