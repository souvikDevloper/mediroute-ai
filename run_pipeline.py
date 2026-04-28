from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from mediroute.pipeline import run_pipeline


def main() -> None:
    p = argparse.ArgumentParser(description="Run MediRoute AI pipeline locally.")
    p.add_argument("--input", default="data/official/virtue_foundation_ghana_v0_3.csv", help="Input facility CSV; defaults to official VF Ghana v0.3 data")
    p.add_argument("--output", default="data/processed", help="Output directory")
    p.add_argument("--use-llm", action="store_true", help="Use optional Databricks model serving endpoint")
    args = p.parse_args()
    outputs = run_pipeline(args.input, args.output, use_llm=args.use_llm)
    print("Pipeline completed.")
    for k, v in outputs.items():
        print(f"- {k}: {len(v)} rows")
    print(f"Outputs written to {args.output}")


if __name__ == "__main__":
    main()
