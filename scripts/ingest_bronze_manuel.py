import boto3
import psycopg2

BUCKET_NAME = "wintershoplogs"

# ---------- MinIO ----------
s3 = boto3.client(
    "s3",
    endpoint_url="http://51.77.215.42:9010",
    aws_access_key_id="studentSDV",
    aws_secret_access_key="coucou44",
)

# ---------- PostgreSQL ----------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="monmotdepasse"
)
cur = conn.cursor()


def ingest_file(object_key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=object_key)
    content = response["Body"].read().decode("utf-8")

    for line in content.splitlines():
        if line.strip():
            cur.execute(
                """
                INSERT INTO "Prod".bronze (brut)
                VALUES (%s)
                """,
                (line,)
            )


def main():
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" not in objects:
        print("Aucun fichier trouvé dans le bucket")
        return

    for obj in objects["Contents"]:
        key = obj["Key"]
        print(f"Ingestion du fichier : {key}")
        ingest_file(key)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
