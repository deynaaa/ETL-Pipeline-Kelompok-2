# Proyek UAS Kelompok 2 — Pipeline ETL Multibahasa + Web Portal Data Warehouse

Paket proyek lengkap berbasis **Docker Compose (7 Kontainer Aktif)**: 6 kontainer resmi **Apache Airflow (`CeleryExecutor`)** + 1 kontainer **Web Portal Data Warehouse (`etl-warehouse-web`, Streamlit port 8501)** dengan pemisahan output per running (`output/runs/<run_folder>/`).

## Cara Menjalankan (7 Kontainer)
```bash
# 1. Build image Airflow & Web Portal + inisialisasi DB Postgres
docker compose up --build airflow-init

# 2. Jalankan seluruh 7 kontainer aktif
docker compose up --build -d

# 3. Cek status kontainer
docker compose ps
```

* **Airflow Web UI (Orkestrasi & Monitoring):** `http://localhost:8080` (login: `airflow` / `airflow`)
* **Web Portal Data Warehouse (Dropdown per Running, Summary Dashboard, & Download):** `http://localhost:8501`

Setiap kali Anda menekan **Trigger DAG** di Airflow, output otomatis tersimpan di folder terpisah `output/runs/<timestamp>__<trigger>/` (tidak menimpa run sebelumnya) dan langsung muncul di dropdown pada `http://localhost:8501`.
