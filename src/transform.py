"""
Modul TaskGroup 2 (tg_transform): Pembersihan Lintas Bahasa & Feature Engineering Paralel
PIC: Anggota 3 & 4 — Kelompok 2
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.utils import (
    ENGAGEMENT_COLUMNS,
    HAS_EMOJI,
    LANG_SAFE_PATTERN,
    MENTION_PATTERN,
    RT_SIGNAL_PATTERN,
    URL_PATTERN,
    build_base_clean_text,
    build_text_context,
    build_text_normalized,
    count_emojis,
    get_language_name,
    make_run_folder_name,
    setup_logger,
)


def clean_and_deduplicate(
    extract_meta: Dict[str, Any],
    staging_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_transform.clean_and_deduplicate",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 2.1: Filter null wajib, standarisasi kode & nama 61 bahasa, deduplikasi, & cast numerik."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(extract_meta["stage_01_path"])
        df = pd.read_csv(in_path, low_memory=False)
        rows_in = len(df)
        log.info("[INPUT] Membaca data dari: %s | Total baris: %d", in_path, rows_in)

        before_null = len(df)
        df = df.dropna(subset=["Text", "Datetime", "Language"]).copy()
        df["Text"] = df["Text"].astype(str)
        df = df[df["Text"].str.strip() != ""].copy()
        dropped_null = int(before_null - len(df))

        df["Language"] = (
            df["Language"]
            .astype(str)
            .str.lower()
            .str.strip()
            .map(lambda x: LANG_SAFE_PATTERN.sub("", x) or "unknown")
        )
        df["language_name"] = df["Language"].map(get_language_name)

        before_dup_id = len(df)
        df = df.drop_duplicates(subset=["Tweet Id"], keep="first").copy()
        dropped_dup_id = int(before_dup_id - len(df))

        before_dup_text = len(df)
        df = df.drop_duplicates(subset=["Text"], keep="first").copy()
        dropped_dup_text = int(before_dup_text - len(df))

        for col in ENGAGEMENT_COLUMNS + ["hastag_counts"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            else:
                df[col] = 0

        df["tweet_id"] = df["Tweet Id"].astype(str)
        rows_out = len(df)
        rows_dropped = rows_in - rows_out
        unique_langs_after = int(df["Language"].nunique())
        log.info(
            "[AUDIT] Baris masuk: %d | Dibuang (null=%d, dup_id=%d, dup_text=%d; total=%d) | Baris keluar: %d (%d bahasa)",
            rows_in,
            dropped_null,
            dropped_dup_id,
            dropped_dup_text,
            rows_dropped,
            rows_out,
            unique_langs_after,
        )

        out_path = Path(staging_dir) / "stage_02a_cleaned.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8")
        log.info("[OUTPUT] Artefak tersimpan di: %s", out_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            **extract_meta,
            "stage_02a_path": str(out_path),
            "dropped_null": dropped_null,
            "dropped_dup_id": dropped_dup_id,
            "dropped_dup_text": dropped_dup_text,
            "rows_after_dedup": int(rows_out),
            "unique_languages_after_dedup": unique_langs_after,
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise


def engineer_dual_text(
    clean_meta: Dict[str, Any],
    staging_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_transform.engineer_dual_text",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 2.2 (Paralel): Ekstraksi sinyal teks, emoji_count, text_context (demojize), & text_normalized."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(clean_meta["stage_02a_path"])
        df = pd.read_csv(in_path, usecols=["tweet_id", "Text"], dtype={"tweet_id": str}, low_memory=False)
        rows_in = len(df)
        emoji_mode = "emoji (native demojize)" if HAS_EMOJI else "unicode-regex (offline fallback)"
        log.info("[INPUT] Membaca data dari: %s | Total baris: %d | Emoji backend: %s", in_path, rows_in, emoji_mode)

        raw_series = df["Text"].astype(str)
        url_count = raw_series.map(lambda s: len(URL_PATTERN.findall(s))).astype(int)
        mention_count = raw_series.map(lambda s: len(MENTION_PATTERN.findall(s))).astype(int)
        is_retweet = raw_series.map(lambda s: 1 if RT_SIGNAL_PATTERN.search(s) else 0).astype(int)
        emoji_count = raw_series.map(count_emojis).astype(int)

        base_clean = raw_series.map(build_base_clean_text)
        text_context = base_clean.map(build_text_context)
        text_normalized = base_clean.map(build_text_normalized)

        char_count = text_context.str.len().astype(int)
        word_count = text_normalized.map(lambda s: len(s.split()) if s else 0).astype(int)

        text_df = pd.DataFrame(
            {
                "tweet_id": df["tweet_id"],
                "url_count": url_count,
                "mention_count": mention_count,
                "is_retweet": is_retweet,
                "emoji_count": emoji_count,
                "text_context": text_context,
                "text_normalized": text_normalized,
                "char_count": char_count,
                "word_count": word_count,
            }
        )

        empty_text_rows = int(((text_df["text_context"] == "") | (text_df["text_normalized"] == "")).sum())
        total_emojis_detected = int(emoji_count.sum())
        log.info(
            "[AUDIT] Baris masuk: %d | Total emoji terdeteksi: %d | Baris teks kosong pasca-clean: %d | Baris keluar: %d",
            rows_in,
            total_emojis_detected,
            empty_text_rows,
            len(text_df),
        )

        out_path = Path(staging_dir) / "stage_02b_text.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        text_df.to_csv(out_path, index=False, encoding="utf-8")
        log.info("[OUTPUT] Artefak tersimpan di: %s", out_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            "stage_02b_path": str(out_path),
            "emoji_backend": emoji_mode,
            "total_emojis_detected": total_emojis_detected,
            "empty_text_rows": empty_text_rows,
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise


def engineer_temporal_and_engagement(
    clean_meta: Dict[str, Any],
    staging_dir: str,
    run_id: str = "cli_local",
    task_id: str = "tg_transform.engineer_temporal_and_engagement",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 2.3 (Paralel): Standarisasi waktu UTC (date_utc, hour_utc, day_of_week) & metrik interaksi."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        in_path = Path(clean_meta["stage_02a_path"])
        use_cols = ["tweet_id", "Datetime"] + ENGAGEMENT_COLUMNS
        df = pd.read_csv(in_path, usecols=use_cols, dtype={"tweet_id": str}, low_memory=False)
        rows_in = len(df)
        log.info("[INPUT] Membaca data dari: %s | Total baris: %d", in_path, rows_in)

        dt_utc = pd.to_datetime(df["Datetime"], errors="coerce", utc=True)
        invalid_dt_count = int(dt_utc.isna().sum())

        total_engagement = (
            df["LikeCount"] + df["RetweetCount"] + df["ReplyCount"] + df["QuoteCount"]
        ).astype(int)
        virality_ratio = (df["RetweetCount"] / (total_engagement + 1)).round(4)

        temp_df = pd.DataFrame(
            {
                "tweet_id": df["tweet_id"],
                "datetime_utc": dt_utc.dt.strftime("%Y-%m-%d %H:%M:%S+00:00"),
                "date_utc": dt_utc.dt.strftime("%Y-%m-%d"),
                "hour_utc": dt_utc.dt.hour.fillna(-1).astype(int),
                "day_of_week": dt_utc.dt.dayofweek.fillna(-1).astype(int),
                "total_engagement": total_engagement,
                "virality_ratio": virality_ratio,
            }
        )

        log.info(
            "[AUDIT] Baris masuk: %d | Datetime invalid: %d | Total engagement: %d | Max engagement: %d | Baris keluar: %d",
            rows_in,
            invalid_dt_count,
            int(total_engagement.sum()),
            int(total_engagement.max()),
            len(temp_df),
        )

        out_path = Path(staging_dir) / "stage_02c_temporal.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        temp_df.to_csv(out_path, index=False, encoding="utf-8")
        log.info("[OUTPUT] Artefak tersimpan di: %s", out_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            "stage_02c_path": str(out_path),
            "invalid_dt_count": invalid_dt_count,
            "total_engagement_sum": int(total_engagement.sum()),
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise


def merge_transformed_features(
    clean_meta: Dict[str, Any],
    text_meta: Dict[str, Any],
    temporal_meta: Dict[str, Any],
    staging_dir: str,
    output_dir: str = "output",
    run_id: str = "cli_local",
    task_id: str = "tg_transform.merge_transformed_features",
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Sub-task 2.4 (Join): Menggabungkan hasil paralel 2.2 & 2.3, buang teks kosong, simpan stage_02_transformed.csv."""
    log = logger or setup_logger()
    start_time = time.time()
    log.info("[START] Task='%s' | RunID='%s'", task_id, run_id)

    try:
        base_df = pd.read_csv(
            clean_meta["stage_02a_path"],
            usecols=["tweet_id", "Username", "Language", "language_name", "LikeCount", "RetweetCount", "ReplyCount", "QuoteCount", "hastag_counts"],
            dtype={"tweet_id": str},
            low_memory=False,
        )
        text_df = pd.read_csv(text_meta["stage_02b_path"], dtype={"tweet_id": str}, low_memory=False)
        temp_df = pd.read_csv(temporal_meta["stage_02c_path"], dtype={"tweet_id": str}, low_memory=False)
        rows_in = len(base_df)
        log.info("[INPUT] Menggabungkan 3 sumber staging (base=%d, text=%d, temporal=%d)", len(base_df), len(text_df), len(temp_df))

        merged = base_df.merge(temp_df, on="tweet_id", how="inner").merge(text_df, on="tweet_id", how="inner")

        merged["text_context"] = merged["text_context"].fillna("").astype(str)
        merged["text_normalized"] = merged["text_normalized"].fillna("").astype(str)
        valid_mask = (
            (merged["text_context"].str.strip() != "")
            & (merged["text_normalized"].str.strip() != "")
            & merged["datetime_utc"].notna()
        )
        merged = merged[valid_mask].copy()
        rows_out = len(merged)
        dropped_empty_or_dt = int(rows_in - rows_out)

        final_df = merged.rename(
            columns={
                "Username": "username",
                "LikeCount": "like_count",
                "RetweetCount": "retweet_count",
                "ReplyCount": "reply_count",
                "QuoteCount": "quote_count",
            }
        )[
            [
                "tweet_id",
                "datetime_utc",
                "date_utc",
                "hour_utc",
                "day_of_week",
                "username",
                "Language",
                "language_name",
                "text_context",
                "text_normalized",
                "url_count",
                "mention_count",
                "is_retweet",
                "emoji_count",
                "char_count",
                "word_count",
                "like_count",
                "retweet_count",
                "reply_count",
                "quote_count",
                "total_engagement",
                "virality_ratio",
                "hastag_counts",
            ]
        ]

        unique_langs_final = int(final_df["Language"].nunique())
        total_engagement_final = int(final_df["total_engagement"].sum())
        run_folder = clean_meta.get("run_folder") or make_run_folder_name(run_id)
        run_output_dir = Path(output_dir) / "runs" / run_folder
        run_output_dir.mkdir(parents=True, exist_ok=True)

        log.info(
            "[AUDIT] Baris masuk: %d | Dibuang (teks kosong/datetime invalid): %d | Baris keluar: %d (%d bahasa unik) | Folder Run: %s",
            rows_in,
            dropped_empty_or_dt,
            rows_out,
            unique_langs_final,
            run_folder,
        )

        out_path = Path(staging_dir) / "stage_02_transformed.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(out_path, index=False, encoding="utf-8")
        log.info("[OUTPUT] Artefak tersimpan di: %s", out_path)

        elapsed = round(time.time() - start_time, 2)
        log.info("[SUCCESS] Task='%s' selesai dalam %.2f detik.", task_id, elapsed)
        return {
            **clean_meta,
            "run_folder": run_folder,
            "run_output_dir": str(run_output_dir),
            "emoji_backend": text_meta["emoji_backend"],
            "total_emojis_detected": text_meta["total_emojis_detected"],
            "total_engagement_final": total_engagement_final,
            "dropped_empty_after_clean": dropped_empty_or_dt,
            "stage_02_path": str(out_path),
            "clean_row_count": int(rows_out),
            "unique_languages_final": unique_langs_final,
        }
    except Exception as e:
        log.error("[FAILED] Task='%s' gagal pada proses eksekusi! Detail error: %s", task_id, str(e), exc_info=True)
        raise
