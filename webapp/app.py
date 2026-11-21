"""
Web Portal & Mini Data Warehouse — Pipeline ETL Multibahasa ChatGPT Twitter
Kelompok 2 — Proyek UAS Pemrograman 3
"""

from __future__ import annotations

import io
import json
import os
import sqlite3
import zipfile
from pathlib import Path
from typing import Dict, List

import pandas as pd
import streamlit as st

OUTPUT_ROOT = Path(os.getenv("WAREHOUSE_OUTPUT_DIR", "/app/output"))
RUNS_DIR = OUTPUT_ROOT / "runs"

st.set_page_config(
    page_title="ETL Data Warehouse — ChatGPT Twitter (Kelompok 2)",
    page_icon="📊",
    layout="wide",
)


def discover_runs() -> List[Dict]:
    if not RUNS_DIR.exists():
        return []
    runs: List[Dict] = []
    for folder in sorted(RUNS_DIR.iterdir(), key=lambda p: p.name, reverse=True):
        if not folder.is_dir():
            continue
        meta_file = folder / "run_metadata.json"
        meta: Dict = {}
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        runs.append({"folder": folder, "name": folder.name, "meta": meta})
    return runs


def build_by_language_zip(lang_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for csv_file in sorted(lang_dir.glob("clean_twitter_*.csv")):
            zf.write(csv_file, arcname=csv_file.name)
    buf.seek(0)
    return buf.getvalue()


def format_csv_label(filename: str) -> str:
    """Mengubah 'clean_twitter_indonesian_in.csv' menjadi 'Indonesian (in) — clean_twitter_indonesian_in.csv'."""
    stem = filename.replace("clean_twitter_", "").replace(".csv", "")
    parts = stem.split("_")
    if len(parts) >= 2:
        code = parts[-1]
        name = " ".join(p.capitalize() for p in parts[:-1])
        return f"{name} ({code}) — {filename}"
    return filename


col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.title("ETL Data Warehouse Portal — ChatGPT Twitter")
    st.caption("Kelompok 2 · Monitoring Output per DAG Run, Summary Report, & Download Center")
with col_refresh:
    st.write("")
    if st.button("Refresh Daftar Run", use_container_width=True):
        st.rerun()

runs = discover_runs()

if not runs:
    st.info(
        "Belum ada hasil eksekusi pipeline di folder `output/runs/`. "
        "Silakan buka Apache Airflow di `http://localhost:8080`, jalankan **Trigger DAG** "
        "pada `dag_etl_chatgpt_twitter`, lalu klik **Refresh Daftar Run**."
    )
    st.stop()

total_runs = len(runs)
run_options = []
for idx, r in enumerate(runs):
    run_num = total_runs - idx
    ts = r["meta"].get("run_timestamp_utc", r["name"])
    clean_cnt = r["meta"].get("clean_row_count")
    suffix = f" · {clean_cnt:,} baris bersih" if isinstance(clean_cnt, int) else ""
    run_options.append(f"Running #{run_num} — {r['name']} ({ts}{suffix})")

selected_label = st.selectbox("Pilih Riwayat Running Pipeline (Dropdown):", options=run_options, index=0)
selected_run = runs[run_options.index(selected_label)]
run_dir: Path = selected_run["folder"]
meta: Dict = selected_run["meta"]

st.markdown(f"**Run ID:** `{meta.get('run_id', selected_run['name'])}`  |  **Folder:** `output/runs/{selected_run['name']}/`")

tab_summary, tab_explorer, tab_download = st.tabs(
    ["Summary Report Dashboard", "Data Warehouse Explorer (SQLite)", "Download Center"]
)

with tab_summary:
    raw_rows = meta.get("raw_row_count", 0)
    clean_rows = meta.get("clean_row_count", 0)
    retention_pct = round((clean_rows / raw_rows) * 100, 2) if raw_rows else 0.0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Input Mentah", f"{raw_rows:,}")
    k2.metric("Output Bersih", f"{clean_rows:,}", f"{retention_pct}% lolos")
    k3.metric("Bahasa Aktif", f"{meta.get('unique_languages_final', 0)}")
    k4.metric("Total Engagement", f"{meta.get('total_engagement_final', 0):,}")
    k5.metric("Total Emoji", f"{meta.get('total_emojis_detected', 0):,}")

    st.subheader("Audit Kualitas Data (Delta Cleaning)")
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Dibuang (Null Wajib)", f"{meta.get('dropped_null', 0):,}")
    a2.metric("Dibuang (Duplikat ID)", f"{meta.get('dropped_dup_id', 0):,}")
    a3.metric("Dibuang (Duplikat Teks)", f"{meta.get('dropped_dup_text', 0):,}")
    a4.metric("Dibuang (Teks Kosong)", f"{meta.get('dropped_empty_after_clean', 0):,}")

    global_csv = run_dir / "summary_harian_global.csv"
    per_lang_csv = run_dir / "summary_harian_per_bahasa.csv"

    c_left, c_right = st.columns(2)
    with c_left:
        st.subheader("Ringkasan Harian Global (UTC)")
        if global_csv.exists():
            df_global = pd.read_csv(global_csv)
            st.bar_chart(df_global.set_index("date_utc")[["total_tweets", "total_emojis"]])
            st.dataframe(df_global, use_container_width=True, hide_index=True)
        else:
            st.warning("File `summary_harian_global.csv` belum tersedia.")

    with c_right:
        st.subheader("Top 10 Bahasa Terbanyak")
        if per_lang_csv.exists():
            df_per_lang = pd.read_csv(per_lang_csv)
            label_col = "language_name" if "language_name" in df_per_lang.columns else "Language"
            top10 = (
                df_per_lang.groupby(label_col, as_index=False)["total_tweets"]
                .sum()
                .sort_values("total_tweets", ascending=False)
                .head(10)
            )
            st.bar_chart(top10.set_index(label_col)["total_tweets"])
            st.dataframe(df_per_lang.head(30), use_container_width=True, hide_index=True)
        else:
            st.warning("File `summary_harian_per_bahasa.csv` belum tersedia.")

with tab_explorer:
    db_path = run_dir / "chatgpt_etl.db"
    if not db_path.exists():
        st.warning("Database `chatgpt_etl.db` belum ditemukan pada folder run ini.")
    else:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(tweets_clean)").fetchall()}
            has_lang_name = "language_name" in cols

            if has_lang_name:
                lang_rows = conn.execute(
                    "SELECT DISTINCT Language, language_name FROM tweets_clean ORDER BY language_name"
                ).fetchall()
                lang_map = {f"{name} ({code})": code for code, name in lang_rows}
            else:
                lang_rows = conn.execute("SELECT DISTINCT Language FROM tweets_clean ORDER BY Language").fetchall()
                lang_map = {r[0]: r[0] for r in lang_rows}

            dates = [r[0] for r in conn.execute("SELECT DISTINCT date_utc FROM tweets_clean ORDER BY date_utc").fetchall()]

            f1, f2, f3, f4 = st.columns([1.4, 1.2, 1.8, 1.3])
            sel_lang_label = f1.selectbox("Filter Bahasa", ["Semua Bahasa"] + list(lang_map.keys()))
            sel_date = f2.selectbox("Filter Tanggal (UTC)", ["Semua Tanggal"] + dates)
            keyword = f3.text_input("Cari Kata Kunci (di text_context)", placeholder="contoh: OpenAI, MBA, Google...")
            sort_col = f4.selectbox("Urutkan Berdasarkan", ["total_engagement", "emoji_count", "virality_ratio", "word_count"])

            clauses, params = [], []
            if sel_lang_label != "Semua Bahasa":
                clauses.append("Language = ?")
                params.append(lang_map[sel_lang_label])
            if sel_date != "Semua Tanggal":
                clauses.append("date_utc = ?")
                params.append(sel_date)
            if keyword.strip():
                clauses.append("text_context LIKE ?")
                params.append(f"%{keyword.strip()}%")

            where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            lang_select = "language_name, Language" if has_lang_name else "Language"
            query = f"""
                SELECT date_utc, hour_utc, {lang_select}, username, text_context, text_normalized,
                       emoji_count, total_engagement, virality_ratio, word_count
                FROM tweets_clean
                {where_sql}
                ORDER BY {sort_col} DESC
                LIMIT 200
            """
            df_preview = pd.read_sql_query(query, conn, params=params)
            st.caption(f"Menampilkan maksimal 200 baris teratas (ditemukan {len(df_preview)} baris sesuai filter).")
            st.dataframe(df_preview, use_container_width=True, hide_index=True)

with tab_download:
    st.subheader(f"Unduh Artefak Output — Running `{selected_run['name']}`")

    d1, d2, d3 = st.columns(3)
    if global_csv.exists():
        d1.download_button(
            label="Download summary_harian_global.csv",
            data=global_csv.read_bytes(),
            file_name=f"{selected_run['name']}__summary_harian_global.csv",
            mime="text/csv",
            use_container_width=True,
        )
    if per_lang_csv.exists():
        d2.download_button(
            label="Download summary_harian_per_bahasa.csv",
            data=per_lang_csv.read_bytes(),
            file_name=f"{selected_run['name']}__summary_harian_per_bahasa.csv",
            mime="text/csv",
            use_container_width=True,
        )
    db_path = run_dir / "chatgpt_etl.db"
    if db_path.exists():
        d3.download_button(
            label="Download chatgpt_etl.db (SQLite)",
            data=db_path.read_bytes(),
            file_name=f"{selected_run['name']}__chatgpt_etl.db",
            mime="application/octet-stream",
            use_container_width=True,
        )

    st.subheader("Unduh Dataset Partisi per Bahasa (`by_language/`)")
    lang_dir = run_dir / "by_language"
    if lang_dir.exists():
        lang_files = sorted(lang_dir.glob("clean_twitter_*.csv"))
        if lang_files:
            file_label_map = {format_csv_label(f.name): f for f in lang_files}
            dl_col1, dl_col2 = st.columns([2, 1])
            chosen_label = dl_col1.selectbox(
                f"Pilih bahasa yang ingin diunduh ({len(lang_files)} bahasa tersedia):",
                options=list(file_label_map.keys()),
            )
            chosen_file = file_label_map[chosen_label]
            dl_col1.download_button(
                label=f"Download {chosen_file.name}",
                data=chosen_file.read_bytes(),
                file_name=f"{selected_run['name']}__{chosen_file.name}",
                mime="text/csv",
            )
            dl_col2.write("")
            dl_col2.write("")
            dl_col2.download_button(
                label=f"Download Semua ({len(lang_files)} CSV) sebagai ZIP",
                data=build_by_language_zip(lang_dir),
                file_name=f"{selected_run['name']}__by_language_all.zip",
                mime="application/zip",
                use_container_width=True,
            )
        else:
            st.info("Belum ada file partisi bahasa di folder run ini.")
    else:
        st.info("Folder `by_language/` belum tersedia pada run ini.")
