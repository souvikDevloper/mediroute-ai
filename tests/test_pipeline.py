from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mediroute.pipeline import process_dataframe


def test_pipeline_official_dataset_smoke():
    df = pd.read_csv(ROOT / "data" / "official" / "virtue_foundation_ghana_v0_3.csv", dtype=str, keep_default_na=False).head(25)
    outputs = process_dataframe(df)
    assert len(outputs["gold"]) > 0
    assert len(outputs["verification"]) > 0
    assert {"risk_score", "risk_level", "recommendation"}.issubset(outputs["gold"].columns)
    assert outputs["extracted"]["evidence"].map(len).sum() > 0
