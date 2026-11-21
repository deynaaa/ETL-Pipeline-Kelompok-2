"""
Modul TaskGroup 1 (tg_extract): Ekstraksi & Validasi Bahan Baku
PIC: Anggota 1 & 2 — Kelompok 2
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.utils import MANDATORY_COLUMNS, make_run_folder_name, setup_logger


def read_and_validate_schema(
    input_csv_path: str,
    staging_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_extract.read_and_validate_schema",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 1.1: Membaca CSV multiline dan memvalidasi 6 kolom wajib."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(input_csv_path)
        if not in_path.exists():
            raise FileNotFoundError(f"File dataset CSV tidak ditemukan di: {in_path}")

        df = pd.read_csv(in_path, low_memory=False)
        rows_in, cols_in = len(df), len(df.columns)
        log.info("[INPUT] Membaca data dari: %s | Total baris: %d, kolom: %d", in_path, rows_in, cols_in)

        missing_cols = [c for c in MANDATORY_COLUMNS if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Validasi skema gagal! Kolom wajib tidak ditemukan: {missing_cols}")

        multiline_count = int(df["Text"].astype(str).str.contains("\n", regex=False).sum())
        log.info(
            "[AUDIT] Validasi 6 kolom wajib LOLOS | Tweet multiline (\\n): %d | Baris keluar: %d",
            multiline_count,
            rows_in,
        )

        out_path = Path(staging_dir) / "stage_00_validated.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8")
        log.info("[OUTPUT] Artefak tersimpan di: %s", out_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            "run_id": run_id,
            "run_folder": make_run_folder_name(run_id),
            "raw_csv_path": str(in_path),
            "stage_00_path": str(out_path),
            "raw_row_count": int(rows_in),
            "column_count": int(cols_in),
            "multiline_tweet_count": multiline_count,
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise


def audit_raw_profile(
    validate_meta: Dict[str, Any],
    staging_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_extract.audit_raw_profile",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 1.2: Audit profil mentah (null & 61 bahasa) dan simpan stage_01_extracted.csv."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(validate_meta["stage_00_path"])
        df = pd.read_csv(in_path, low_memory=False)
        rows_in = len(df)
        log.info("[INPUT] Dataset tervalidasi terbaca: %d baris, %d kolom.", rows_in, len(df.columns))

        null_mandatory = {c: int(df[c].isna().sum()) for c in MANDATORY_COLUMNS}
        lang_counts = df["Language"].astype(str).str.lower().str.strip().value_counts()
        unique_langs = int(len(lang_counts))
        top5_langs = {str(k): int(v) for k, v in lang_counts.head(5).items()}
        log.info(
            "[AUDIT] Ditemukan %d bahasa unik. Top 5: %s | Null kolom wajib: %s",
            unique_langs,
            json.dumps(top5_langs),
            json.dumps(null_mandatory),
        )

        out_path = Path(staging_dir) / "stage_01_extracted.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8")
        log.info("[OUTPUT] Staging tersimpan di: %s", out_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            **validate_meta,
            "stage_01_path": str(out_path),
            "unique_languages": unique_langs,
            "top5_languages": top5_langs,
            "null_mandatory_counts": null_mandatory,
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise
