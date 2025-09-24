# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import os, sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# === caminhos no Composer ===
BASE_DIR = Path(__file__).resolve().parent
SQL_DIR  = BASE_DIR / "sql"
SQL_ETL  = SQL_DIR / "etl"
SQL_DEL  = SQL_DIR / "delivery"

SQL_CAPTACAO_ETL     = SQL_ETL / "tb_captacao.sql"
SQL_LEADS_CAPT_ETL   = SQL_ETL / "tb_leads_captacao.sql"
SQL_GA_EVENTS_ETL    = SQL_ETL / "tb_ga_events.sql"
SQL_GA_PURCHASES_ETL = SQL_ETL / "tb_ga_purchases.sql"
SQL_GA_CONV_CH_ETL   = SQL_ETL / "tb_ga_conv_channel.sql"
SQL_VIEW_CANAL_CONV  = SQL_DEL / "VW_GA_CANAL_CONVERSAO.sql"
SQL_VIEW_CANAIS_TX   = SQL_DEL / "VW_CANAIS_TX_CONVERSAO_POR_LEAD.sql"
SQL_VIEW_TIME_LEAD   = SQL_DEL / "VW_TIME_LEAD_TO_CONVERSION.sql"  


sys.path.insert(0, str(BASE_DIR))


from scr.scr_integration_data_landing import (
    fn_extract_postgres_to_gcs,
    load_parquet_to_bigquery,
)

def _read_sql(path: Path) -> str:
    if not path.exists():
        available = "\n - ".join(sorted(p.name for p in path.parent.glob("*.sql")))
        raise FileNotFoundError(
            f"Arquivo SQL não encontrado: {path}\n"
            f"Arquivos disponíveis em {path.parent}:\n - {available or '(nenhum .sql encontrado)'}"
        )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="case_cruzeiro_daily",
    default_args=DEFAULT_ARGS,
    description="Pipeline diário: Extract Postgres -> GCS, Load RAW, ETLs e Views",
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cruzeiro", "marketing", "lakehouse"],
) as dag:

    # 1) Extract Postgres -> GCS (landing)
    t_extract_pg = PythonOperator(
        task_id="extract_postgres_to_gcs",
        python_callable=fn_extract_postgres_to_gcs,
    )

    t_load_bq_raw = PythonOperator(
        task_id="load_parquet_to_bq_raw",
        python_callable=load_parquet_to_bigquery,
    )


    t_etl_captacao = BigQueryInsertJobOperator(
        task_id="etl_tb_captacao",
        configuration={"query": {"query": _read_sql(SQL_CAPTACAO_ETL), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )

    t_etl_leads_capt = BigQueryInsertJobOperator(
        task_id="etl_tb_leads_captacao",
        configuration={"query": {"query": _read_sql(SQL_LEADS_CAPT_ETL), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )

    t_etl_ga_events = BigQueryInsertJobOperator(
        task_id="etl_tb_ga_events",
        configuration={"query": {"query": _read_sql(SQL_GA_EVENTS_ETL), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )
    t_etl_ga_purchases = BigQueryInsertJobOperator(
        task_id="etl_tb_ga_purchases",
        configuration={"query": {"query": _read_sql(SQL_GA_PURCHASES_ETL), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )
    t_etl_ga_conv_channel = BigQueryInsertJobOperator(
        task_id="etl_tb_ga_conv_channel",
        configuration={"query": {"query": _read_sql(SQL_GA_CONV_CH_ETL), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )

    # 4) Views
    t_view_canal_conv = BigQueryInsertJobOperator(
        task_id="view_ga_canal_conversao",
        configuration={"query": {"query": _read_sql(SQL_VIEW_CANAL_CONV), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )

    t_view_canais_tx = BigQueryInsertJobOperator(
        task_id="view_canais_tx_conversao_por_lead",
        configuration={"query": {"query": _read_sql(SQL_VIEW_CANAIS_TX), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )

    t_view_time_lead = BigQueryInsertJobOperator(
        task_id="view_time_lead_to_conversion",
        configuration={"query": {"query": _read_sql(SQL_VIEW_TIME_LEAD), "useLegacySql": False}},
        location=os.getenv("BQ_LOCATION", "US"),
    )


    t_extract_pg >> t_load_bq_raw >> t_etl_captacao >>  t_etl_leads_capt  >>  t_etl_ga_events  >>  t_etl_ga_purchases  >> t_etl_ga_conv_channel >> [t_view_canal_conv, t_view_canais_tx, t_view_time_lead]
