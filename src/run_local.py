"""
Skrip CLI Pengujian Lokal (Tanpa Airflow) — Pipeline ETL Modular
Jalankan dari root proyek:
    python3 -m src.run_local --input data/chatgpt1.csv --output-dir output
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extract import audit_raw_profile, read_and_validate_schema  # noqa: E402
from src.load import export_partitioned_languages, generate_daily_summary, load_to_sqlite_and_audit  # noqa: E402
from src.transform import (  # noqa: E402
    clean_and_deduplicate,
    engineer_dual_text,
    engineer_temporal_and_engagement,
    merge_transformed_features,
)
from src.utils import setup_logger  # noqa: E402


def run_local_pipeline(input_csv: str, output_dir: str, log_file: str | None = None) -> dict:
    out_path = Path(output_dir)
    staging_dir = out_path / "_staging"
    logger = setup_logger(log_file=log_file or (out_path.parent / "logs" / "etl_audit.log"))
    run_id = f"cli__{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')}"

    # tg_extract
    m11 = read_and_validate_schema(input_csv, str(staging_dir), run_id=run_id, logger=logger)
    m12 = audit_raw_profile(m11, str(staging_dir), run_id=run_id, logger=logger)

    # tg_transform
    m21 = clean_and_deduplicate(m12, str(staging_dir), run_id=run_id, logger=logger)
    m22 = engineer_dual_text(m21, str(staging_dir), run_id=run_id, logger=logger)
    m23 = engineer_temporal_and_engagement(m21, str(staging_dir), run_id=run_id, logger=logger)
    m24 = merge_transformed_features(m21, m22, m23, str(staging_dir), output_dir=str(out_path), run_id=run_id, logger=logger)

    # tg_load
    m31 = export_partitioned_languages(m24, str(out_path), run_id=run_id, logger=logger)
    m32 = generate_daily_summary(m24, str(out_path), run_id=run_id, logger=logger)
    m33 = load_to_sqlite_and_audit(m24, str(out_path), run_id=run_id, logger=logger)

    return {
        "run_id": run_id,
        "run_folder": m24["run_folder"],
        "clean_rows": m24["clean_row_count"],
        "language_files": m31["language_file_count"],
        "global_summary_rows": m32["global_rows"],
        "sqlite_db": m33["sqlite_db_path"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Uji coba lokal Pipeline ETL Modular (Kelompok 2)")
    parser.add_argument("--input", required=True, help="Path menuju file chatgpt1.csv")
    parser.add_argument("--output-dir", default="output", help="Direktori root output")
    parser.add_argument("--log-file", default="logs/etl_audit.log", help="Path file log audit")
    args = parser.parse_args()

    res = run_local_pipeline(args.input, args.output_dir, args.log_file)
    print("\n=== RINGKASAN UJI LOKAL MODULAR ===")
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
