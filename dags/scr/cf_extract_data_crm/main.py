# -*- coding: utf-8 -*-
import os
import pandas as pd
import psycopg2
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import tempfile
import pyarrow.parquet as pq
import pyarrow as pa
from flask import Request

# encodings
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
os.environ.setdefault("PYTHONUTF8", "1")

# ---------- Função Cloud Function ----------
def run_pipeline(request: Request):
    fn_extract_postgres_to_gcs()
    return Pipeline concluido com sucesso!", 200

# ---------- EXTRACT: Postgres -> GCS ----------
def fn_extract_postgres_to_gcs():
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db   = os.getenv("POSTGRES_DB")
    pg_user = os.getenv("POSTGRES_USER")
    pg_pass = os.getenv("POSTGRES_PASSWORD")

    gcp_bucket   = os.getenv("GCP_BUCKET")
    gcp_dest     = os.getenv("GCP_DEST_PATH", "leads/leads_extract.parquet")
    query        = os.getenv("PG_QUERY", "SELECT * FROM tb_event_ingress_csv2;")
    chunksize    = int(os.getenv("PG_CHUNK_SIZE", "100000"))

    conn = psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass)
    temp_file = os.path.join(tempfile.gettempdir(), "leads_extract.parquet")

    writer = None
    total = 0
    for i, chunk in enumerate(pd.read_sql(query, conn, chunksize=chunksize)):
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(temp_file, table.schema, compression="snappy")
        writer.write_table(table)
        total += len(chunk)
        print(f"   ➕ Chunk {i+1} ({len(chunk)} linhas) — total {total}")

    if writer:
        writer.close()
    conn.close()

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcp_bucket)
    blob = bucket.blob(gcp_dest)
    blob.upload_from_filename(temp_file)
    print(f"✅ Upload: gs://{gcp_bucket}/{gcp_dest}")


