"""
DAG Apache Airflow: dag_etl_chatgpt_twitter (Orkestrasi Modular + TaskGroup + Audit Log DETAILS)
Kelompok 2 — Proyek UAS Pemrograman 3

Mengimpor logika bisnis dari modul terpisah:
  - src/extract.py   -> tg_extract   (Anggota 1 & 2)
  - src/transform.py -> tg_transform (Anggota 3 & 4)
  - src/load.py      -> tg_load      (Anggota 3 & 4)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.models import Log
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extract import audit_raw_profile, read_and_validate_schema  # noqa: E402
from src.load import (  # noqa: E402
    export_partitioned_languages,
    generate_daily_summary,
    load_to_sqlite_and_audit,
)
from src.transform import (  # noqa: E402
    clean_and_deduplicate,
    engineer_dual_text,
    engineer_temporal_and_engagement,
    merge_transformed_features,
)

INPUT_CSV_PATH = os.getenv("CHATGPT_CSV_PATH", str(PROJECT_ROOT / "data" / "chatgpt1.csv"))
STAGING_DIR = os.getenv("CHATGPT_STAGING_DIR", str(PROJECT_ROOT / "output" / "_staging"))
OUTPUT_DIR = os.getenv("CHATGPT_OUTPUT_DIR", str(PROJECT_ROOT / "output"))


def _write_audit_log(context: dict, event: str, details: Dict[str, Any]) -> None:
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return
    try:
        owner = getattr(ti.task, "owner", "kelompok_2") if getattr(ti, "task", None) else "kelompok_2"
        with create_session() as session:
            entry = Log(
                event=event,
                task_instance=ti,
                owner=owner,
                extra=json.dumps(details, ensure_ascii=False),
            )
            session.add(entry)
            session.commit()
    except Exception as exc:
        logging.getLogger("airflow.task").warning("Gagal menulis rincian ke Audit Log Airflow: %s", exc)


def _on_task_failure(context: dict) -> None:
    exc = context.get("exception")
    task_obj = context.get("task")
    task_id = task_obj.task_id if task_obj is not None else "unknown_task"
    err_type = type(exc).__name__ if exc is not None else "TaskError"
    err_msg = str(exc)[:300] if exc is not None else "Task gagal tanpa pesan exception."
    _write_audit_log(
        context,
        event="etl_failed",
        details={
            "status": "FAILED",
            "ringkasan": f"Gagal pada {task_id} ({err_type})",
            "error": err_msg,
        },
    )


def _t11_validate(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    meta = read_and_validate_schema(
        input_csv_path=INPUT_CSV_PATH,
        staging_dir=STAGING_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    context["ti"].xcom_push(key="validate_meta", value=meta)
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "run_folder": meta["run_folder"],
            "ringkasan": (
                f"Validasi lolos: {meta['raw_row_count']:,} baris, {meta['column_count']} kolom "
                f"(6 kolom wajib valid, {meta['multiline_tweet_count']:,} multiline)"
            ),
        },
    )
    return meta


def _t12_audit(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    validate_meta = ti.xcom_pull(task_ids="tg_extract.read_and_validate_schema", key="validate_meta")
    meta = audit_raw_profile(
        validate_meta=validate_meta,
        staging_dir=STAGING_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    ti.xcom_push(key="extract_meta", value=meta)
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "ringkasan": f"Audit mentah selesai: {meta['raw_row_count']:,} baris, {meta['unique_languages']} bahasa unik",
            "top5_bahasa": meta["top5_languages"],
        },
    )
    return meta


def _t21_clean(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    extract_meta = ti.xcom_pull(task_ids="tg_extract.audit_raw_profile", key="extract_meta")
    meta = clean_and_deduplicate(
        extract_meta=extract_meta,
        staging_dir=STAGING_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    ti.xcom_push(key="clean_meta", value=meta)
    total_drop = meta["dropped_null"] + meta["dropped_dup_id"] + meta["dropped_dup_text"]
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "ringkasan": (
                f"Cleaning & dedup selesai: {meta['raw_row_count']:,} -> {meta['rows_after_dedup']:,} baris "
                f"({meta['unique_languages_after_dedup']} bahasa), dibuang {total_drop} baris"
            ),
        },
    )
    return meta


def _t22_dual_text(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    clean_meta = ti.xcom_pull(task_ids="tg_transform.clean_and_deduplicate", key="clean_meta")
    meta = engineer_dual_text(
        clean_meta=clean_meta,
        staging_dir=STAGING_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    ti.xcom_push(key="text_meta", value=meta)
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "ringkasan": (
                f"Dual-text selesai: {meta['total_emojis_detected']:,} emoji dikonversi, "
                f"{meta['empty_text_rows']} baris kosong"
            ),
        },
    )
    return meta


def _t23_temporal(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    clean_meta = ti.xcom_pull(task_ids="tg_transform.clean_and_deduplicate", key="clean_meta")
    meta = engineer_temporal_and_engagement(
        clean_meta=clean_meta,
        staging_dir=STAGING_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    ti.xcom_push(key="temporal_meta", value=meta)
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "ringkasan": f"Standarisasi UTC & interaksi selesai: total engagement {meta['total_engagement_sum']:,}",
        },
    )
    return meta


def _t24_merge(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    clean_meta = ti.xcom_pull(task_ids="tg_transform.clean_and_deduplicate", key="clean_meta")
    text_meta = ti.xcom_pull(task_ids="tg_transform.engineer_dual_text", key="text_meta")
    temporal_meta = ti.xcom_pull(task_ids="tg_transform.engineer_temporal_and_engagement", key="temporal_meta")
    meta = merge_transformed_features(
        clean_meta=clean_meta,
        text_meta=text_meta,
        temporal_meta=temporal_meta,
        staging_dir=STAGING_DIR,
        output_dir=OUTPUT_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    ti.xcom_push(key="transform_meta", value=meta)
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "run_folder": meta["run_folder"],
            "ringkasan": (
                f"Merge selesai: {meta['clean_row_count']:,} baris bersih "
                f"({meta['unique_languages_final']} bahasa), target output/runs/{meta['run_folder']}/"
            ),
        },
    )
    return meta


def _t31_export_languages(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    transform_meta = ti.xcom_pull(task_ids="tg_transform.merge_transformed_features", key="transform_meta")
    result = export_partitioned_languages(
        transform_meta=transform_meta,
        output_dir=OUTPUT_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "run_folder": result["run_folder"],
            "ringkasan": f"Partisi selesai: {result['language_file_count']} file CSV di output/runs/{result['run_folder']}/by_language/",
        },
    )
    return result


def _t32_daily_summary(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    transform_meta = ti.xcom_pull(task_ids="tg_transform.merge_transformed_features", key="transform_meta")
    result = generate_daily_summary(
        transform_meta=transform_meta,
        output_dir=OUTPUT_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "run_folder": result["run_folder"],
            "ringkasan": (
                f"2 file ringkasan UTC selesai di output/runs/{result['run_folder']}/ "
                f"(global {result['global_rows']} hari, per bahasa {result['per_language_rows']} baris)"
            ),
        },
    )
    return result


def _t33_load_sqlite(**context) -> dict:
    logger = logging.getLogger("airflow.task")
    ti = context["ti"]
    transform_meta = ti.xcom_pull(task_ids="tg_transform.merge_transformed_features", key="transform_meta")
    result = load_to_sqlite_and_audit(
        transform_meta=transform_meta,
        output_dir=OUTPUT_DIR,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
        logger=logger,
    )
    _write_audit_log(
        context,
        event="etl_success",
        details={
            "status": "SUCCESS",
            "run_folder": result["run_folder"],
            "ringkasan": (
                f"Load SQLite & run_metadata.json selesai di output/runs/{result['run_folder']}/ "
                f"({result['tweets_clean_rows']:,} tweet)"
            ),
        },
    )
    return result


default_args = {
    "owner": "kelompok_2",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _on_task_failure,
}

with DAG(
    dag_id="dag_etl_chatgpt_twitter",
    description="Pipeline ETL Multibahasa Modular + TaskGroup + Versioned Data Warehouse — Kelompok 2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["uas", "kelompok2", "etl", "multilingual", "modular", "warehouse"],
) as dag:

    with TaskGroup(group_id="tg_extract", tooltip="Ekstraksi & Validasi Bahan Baku") as tg_extract:
        t11 = PythonOperator(task_id="read_and_validate_schema", python_callable=_t11_validate)
        t12 = PythonOperator(task_id="audit_raw_profile", python_callable=_t12_audit)
        t11 >> t12

    with TaskGroup(group_id="tg_transform", tooltip="Pembersihan Lintas Bahasa & Feature Engineering Paralel") as tg_transform:
        t21 = PythonOperator(task_id="clean_and_deduplicate", python_callable=_t21_clean)
        t22 = PythonOperator(task_id="engineer_dual_text", python_callable=_t22_dual_text)
        t23 = PythonOperator(task_id="engineer_temporal_and_engagement", python_callable=_t23_temporal)
        t24 = PythonOperator(task_id="merge_transformed_features", python_callable=_t24_merge)
        t21 >> [t22, t23] >> t24

    with TaskGroup(group_id="tg_load", tooltip="Partisi Bahasa, 2 Ringkasan Harian, & SQLite per Run") as tg_load:
        t31 = PythonOperator(task_id="export_partitioned_languages", python_callable=_t31_export_languages)
        t32 = PythonOperator(task_id="generate_daily_summary", python_callable=_t32_daily_summary)
        t33 = PythonOperator(task_id="load_to_sqlite_and_audit", python_callable=_t33_load_sqlite)

    tg_extract >> tg_transform >> tg_load
