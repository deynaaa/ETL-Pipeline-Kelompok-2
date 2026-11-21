"""
Modul TaskGroup 3 (tg_load): Partisi Bahasa Ramah-Baca, 2 Ringkasan Harian UTC, & SQLite per Running
PIC: Anggota 3 & 4 — Kelompok 2
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.utils import get_language_file_slug, make_run_folder_name, setup_logger


def _resolve_run_dir(transform_meta: Dict[str, Any], output_dir: str, run_id: str) -> Path:
    if transform_meta.get("run_output_dir"):
        p = Path(transform_meta["run_output_dir"])
    else:
        p = Path(output_dir) / "runs" / make_run_folder_name(run_id)
    p.mkdir(parents=True, exist_ok=True)
    return p


def export_partitioned_languages(
    transform_meta: Dict[str, Any],
    output_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_load.export_partitioned_languages",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 3.1: Partisi seluruh bahasa dengan nama file ramah baca (clean_twitter_<nama_bahasa>_<kode>.csv)."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(transform_meta["stage_02_path"])
        df = pd.read_csv(in_path, dtype={"tweet_id": str}, low_memory=False)
        rows_in = len(df)
        log.info("[INPUT] Membaca stage_02_transformed.csv: %d baris bersih.", rows_in)

        run_dir = _resolve_run_dir(transform_meta, output_dir, run_id)
        lang_dir = run_dir / "by_language"
        lang_dir.mkdir(parents=True, exist_ok=True)

        file_counts: Dict[str, int] = {}
        for lang_code, group_df in df.groupby("Language", sort=True):
            slug = get_language_file_slug(str(lang_code))
            filename = f"clean_twitter_{slug}.csv"
            out_file = lang_dir / filename
            group_df.to_csv(out_file, index=False, encoding="utf-8")
            file_counts[filename] = int(len(group_df))

        sorted_counts = sorted(file_counts.items(), key=lambda kv: kv[1], reverse=True)
        sample_str = ", ".join(f"{fname} ({cnt:,} baris)" for fname, cnt in sorted_counts[:5])
        log.info("[AUDIT] Berhasil mempartisi data ke %d file bahasa di %s", len(file_counts), lang_dir)
        log.info("[OUTPUT] Sample file: %s", sample_str)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            "run_folder": run_dir.name,
            "by_language_dir": str(lang_dir),
            "language_file_count": len(file_counts),
            "top_language_files": dict(sorted_counts[:10]),
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise


def generate_daily_summary(
    transform_meta: Dict[str, Any],
    output_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_load.generate_daily_summary",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 3.2: Menghasilkan 2 file ringkasan terpisah (summary_harian_global.csv & summary_harian_per_bahasa.csv)."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(transform_meta["stage_02_path"])
        df = pd.read_csv(in_path, dtype={"tweet_id": str}, low_memory=False)
        rows_in = len(df)
        log.info("[INPUT] Membaca data dari: %s | Total baris: %d", in_path, rows_in)

        run_dir = _resolve_run_dir(transform_meta, output_dir, run_id)

        global_df = df.groupby("date_utc", as_index=False).agg(
            total_tweets=("tweet_id", "count"),
            unique_languages=("Language", "nunique"),
            total_engagement=("total_engagement", "sum"),
            avg_engagement=("total_engagement", "mean"),
            max_engagement=("total_engagement", "max"),
            avg_virality_ratio=("virality_ratio", "mean"),
            total_emojis=("emoji_count", "sum"),
            avg_word_count=("word_count", "mean"),
        )
        global_df["avg_engagement"] = global_df["avg_engagement"].round(2)
        global_df["avg_virality_ratio"] = global_df["avg_virality_ratio"].round(4)
        global_df["avg_word_count"] = global_df["avg_word_count"].round(2)

        global_path = run_dir / "summary_harian_global.csv"
        global_df.to_csv(global_path, index=False, encoding="utf-8")

        per_lang_df = df.groupby(["date_utc", "Language", "language_name"], as_index=False).agg(
            total_tweets=("tweet_id", "count"),
            total_engagement=("total_engagement", "sum"),
            avg_engagement=("total_engagement", "mean"),
            max_engagement=("total_engagement", "max"),
            avg_virality_ratio=("virality_ratio", "mean"),
            total_emojis=("emoji_count", "sum"),
        )
        per_lang_df["avg_engagement"] = per_lang_df["avg_engagement"].round(2)
        per_lang_df["avg_virality_ratio"] = per_lang_df["avg_virality_ratio"].round(4)
        per_lang_df = per_lang_df.sort_values(["date_utc", "total_tweets"], ascending=[True, False])

        per_lang_path = run_dir / "summary_harian_per_bahasa.csv"
        per_lang_df.to_csv(per_lang_path, index=False, encoding="utf-8")

        log.info(
            "[AUDIT] Baris masuk: %d | Global: %d hari UTC | Per bahasa: %d kombinasi (date_utc, language_name)",
            rows_in,
            len(global_df),
            len(per_lang_df),
        )
        log.info("[OUTPUT] Artefak tersimpan di: %s & %s", global_path, per_lang_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            "run_folder": run_dir.name,
            "summary_global_path": str(global_path),
            "summary_per_bahasa_path": str(per_lang_path),
            "global_rows": int(len(global_df)),
            "per_language_rows": int(len(per_lang_df)),
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise


def load_to_sqlite_and_audit(
    transform_meta: Dict[str, Any],
    output_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_load.load_to_sqlite_and_audit",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 3.3: Memuat seluruh dataset bersih ke SQLite (chatgpt_etl.db), indeks, & run_metadata.json."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(transform_meta["stage_02_path"])
        df = pd.read_csv(in_path, dtype={"tweet_id": str}, low_memory=False)
        rows_in = len(df)
        log.info("[INPUT] Membaca data dari: %s | Total baris: %d", in_path, rows_in)

        run_dir = _resolve_run_dir(transform_meta, output_dir, run_id)
        db_path = run_dir / "chatgpt_etl.db"
        run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")

        daily_lang_df = df.groupby(["date_utc", "Language", "language_name"], as_index=False).agg(
            total_tweets=("tweet_id", "count"),
            total_engagement=("total_engagement", "sum"),
            avg_engagement=("total_engagement", "mean"),
            max_engagement=("total_engagement", "max"),
            avg_virality_ratio=("virality_ratio", "mean"),
        )
        daily_lang_df["avg_engagement"] = daily_lang_df["avg_engagement"].round(2)
        daily_lang_df["avg_virality_ratio"] = daily_lang_df["avg_virality_ratio"].round(4)

        daily_global_df = df.groupby("date_utc", as_index=False).agg(
            total_tweets=("tweet_id", "count"),
            unique_languages=("Language", "nunique"),
            total_engagement=("total_engagement", "sum"),
            avg_engagement=("total_engagement", "mean"),
            max_engagement=("total_engagement", "max"),
            avg_virality_ratio=("virality_ratio", "mean"),
        )
        daily_global_df["avg_engagement"] = daily_global_df["avg_engagement"].round(2)
        daily_global_df["avg_virality_ratio"] = daily_global_df["avg_virality_ratio"].round(4)

        audit_record = {
            "run_id": run_id,
            "run_folder": run_dir.name,
            "run_timestamp_utc": run_timestamp,
            "raw_row_count": int(transform_meta["raw_row_count"]),
            "clean_row_count": int(transform_meta["clean_row_count"]),
            "dropped_null": int(transform_meta["dropped_null"]),
            "dropped_dup_id": int(transform_meta["dropped_dup_id"]),
            "dropped_dup_text": int(transform_meta["dropped_dup_text"]),
            "dropped_empty_after_clean": int(transform_meta["dropped_empty_after_clean"]),
            "unique_languages_raw": int(transform_meta.get("unique_languages", 0)),
            "unique_languages_final": int(transform_meta["unique_languages_final"]),
            "total_emojis_detected": int(transform_meta["total_emojis_detected"]),
            "total_engagement_final": int(transform_meta.get("total_engagement_final", df["total_engagement"].sum())),
            "emoji_backend": str(transform_meta["emoji_backend"]),
        }

        with sqlite3.connect(db_path) as conn:
            df.to_sql("tweets_clean", conn, if_exists="replace", index=False)
            daily_lang_df.to_sql("daily_engagement_summary", conn, if_exists="replace", index=False)
            daily_global_df.to_sql("daily_summary_global", conn, if_exists="replace", index=False)
            pd.DataFrame([audit_record]).to_sql("etl_run_audit", conn, if_exists="append", index=False)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_lang_date ON tweets_clean(Language, date_utc)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lang_name ON tweets_clean(language_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tweets_engagement ON tweets_clean(total_engagement DESC)")
            conn.commit()

        meta_path = run_dir / "run_metadata.json"
        meta_payload = {
            **audit_record,
            "top5_languages": transform_meta.get("top5_languages", {}),
            "date_utc_min": str(df["date_utc"].min()),
            "date_utc_max": str(df["date_utc"].max()),
        }
        meta_path.write_text(json.dumps(meta_payload, indent=2, ensure_ascii=False), encoding="utf-8")

        latest_ptr = Path(output_dir) / "latest_run.json"
        latest_ptr.write_text(json.dumps({"latest_run_folder": run_dir.name, "run_id": run_id}, indent=2), encoding="utf-8")

        log.info(
            "[AUDIT] Baris dimuat ke %s: tweets_clean=%d | daily_engagement_summary=%d | daily_summary_global=%d",
            db_path.name,
            rows_in,
            len(daily_lang_df),
            len(daily_global_df),
        )
        log.info("[OUTPUT] Database & metadata run tersimpan di: %s", run_dir)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            "run_folder": run_dir.name,
            "sqlite_db_path": str(db_path),
            "metadata_path": str(meta_path),
            "tweets_clean_rows": int(rows_in),
            "daily_summary_rows": int(len(daily_lang_df)),
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise
