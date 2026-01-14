import os
import sys
from datetime import datetime, timezone
from typing import Iterable, List, Tuple, Optional

from dotenv import load_dotenv
from boto3.session import Session
import psycopg2
from psycopg2.extras import execute_values


# ----------------------------
# Helpers: connections
# ----------------------------

def get_s3_client():
    """
    MinIO via S3 API (boto3).
    """
    session = Session(
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    return session.client(
        service_name="s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
    )


def get_pg_conn():
    """
    PostgreSQL connection.
    """
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


# ----------------------------
# Helpers: postgres
# ----------------------------

def ensure_bronze_constraints(cur, schema: str, table: str):
    """
    Ensure a unique constraint exists for dedupe on (source_path, line_number).
    This is the simplest deterministic dedupe for "one file = many raw lines".
    """
    cur.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_source_line
        ON {schema}.{table} (source_path, line_number);
    """)


def detect_has_ingestion_ts(cur, schema: str, table: str) -> bool:
    """
    Detect if the target table has an 'ingestion_ts' column.
    """
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = 'ingestion_ts'
        LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def insert_rows(
    cur,
    schema: str,
    table: str,
    rows: List[Tuple],
    has_ingestion_ts: bool,
):
    """
    Bulk insert rows. Ignore duplicates thanks to unique index.
    """
    if not rows:
        return 0

    if has_ingestion_ts:
        sql = f"""
            INSERT INTO {schema}.{table} (ingestion_ts, source_path, line_number, brut)
            VALUES %s
            ON CONFLICT (source_path, line_number) DO NOTHING
        """
    else:
        sql = f"""
            INSERT INTO {schema}.{table} (source_path, line_number, brut)
            VALUES %s
            ON CONFLICT (source_path, line_number) DO NOTHING
        """

    execute_values(cur, sql, rows)
    return len(rows)


# ----------------------------
# Helpers: s3/minio
# ----------------------------

def list_keys(s3, bucket: str, prefix: str = "") -> List[str]:
    """
    List all object keys in the bucket (handles pagination).
    """
    keys: List[str] = []
    token: Optional[str] = None

    while True:
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])
        keys.extend([c["Key"] for c in contents])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def read_object_text(s3, bucket: str, key: str) -> str:
    """
    Download object content and decode as text.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return data.decode("utf-8", errors="replace")


def to_raw_lines(text: str) -> List[str]:
    """
    1 log line = 1 raw line. We keep it as-is (bronze philosophy).
    """
    return [ln for ln in text.splitlines() if ln.strip()]


# ----------------------------
# Main
# ----------------------------

def main():
    # 0) Load env
    load_dotenv()

    # 1) Read config
    bucket = os.environ["MINIO_BUCKET"]
    prefix = os.environ.get("MINIO_PREFIX", "")

    schema = os.environ.get("PG_SCHEMA", "bramly")
    table = os.environ.get("PG_TABLE", "bronze1")

    # 2) Connect
    s3 = get_s3_client()
    conn = get_pg_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Ensure dedupe
            ensure_bronze_constraints(cur, schema, table)
            has_ingestion_ts = detect_has_ingestion_ts(cur, schema, table)
            conn.commit()

        # 3) List files on MinIO
        keys = list_keys(s3, bucket=bucket, prefix=prefix)
        if not keys:
            print(f"Aucun fichier trouvé dans MinIO bucket='{bucket}' prefix='{prefix}'")
            return

        print(f"{len(keys)} fichier(s) trouvé(s) dans MinIO.")

        total_inserted = 0

        # 4) For each file: read raw lines and insert
        with conn.cursor() as cur:
            for key in keys:
                print(f"\n--- Ingestion: {key}")
                text = read_object_text(s3, bucket, key)
                lines = to_raw_lines(text)

                # Build rows (batch insert)
                now = datetime.now(timezone.utc)

                batch: List[Tuple] = []
                for i, line in enumerate(lines, start=1):
                    if has_ingestion_ts:
                        batch.append((now, key, i, line))
                    else:
                        batch.append((key, i, line))

                inserted = insert_rows(cur, schema, table, batch, has_ingestion_ts)
                # Note: inserted = number of attempted rows (duplicates ignored by ON CONFLICT)
                total_inserted += inserted

                print(f"  lignes lues: {len(lines)} | tentatives d'insert: {inserted}")

            conn.commit()

        print(f"\n✅ Terminé. Total tentatives d'insert: {total_inserted}")
        print(f"Table cible: {schema}.{table}")

    except Exception as e:
        conn.rollback()
        print("\n❌ Erreur pendant l'ingestion:")
        print(str(e))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
