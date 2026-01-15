import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "bronze"
DATA_DIR = "/opt/spark-data/input"
BRONZE_BASE = "s3a://bronze/raw"

minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

spark = SparkSession.builder \
    .appName("Traffic-Bronze-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .getOrCreate()

def main():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    
    if not files:
        print("No files found to ingest")
        return
    
    ingest_date = datetime.now().strftime("%Y-%m-%d")
    metadata = []
    success_count = 0

    print(f"Starting Bronze ingestion: {len(files)} files")

    for filename in files:
        file_path = os.path.join(DATA_DIR, filename)
        size_bytes = os.path.getsize(file_path)
        
        # Create path on trên MinIO (Ex: raw/2025-05-20/file.json)
        s3_path = f"raw/{ingest_date}/{filename}"
        
        try:
            print(f"Uploading {filename} ({size_bytes / (1024**3):.2f} GB)...")
            minio_client.fput_object(BUCKET_NAME, s3_path, file_path)
            
            status = "success"
            error_msg = None
            success_key = os.path.dirname(s3_path) + "/_SUCCESS"
            minio_client.fput_object(BUCKET_NAME, success_key, "/dev/null")
            
            success_count += 1
            print(f"Uploaded: {s3_path}")
        except Exception as e:
            status = "failed"
            error_msg = str(e)
            print(f"Failed {filename}: {error_msg}")

        metadata.append((
            filename, 
            s3_path, 
            size_bytes, 
            status, 
            ingest_date, 
            datetime.now(), 
            error_msg
        ))

    schema = StructType([
        StructField("file_name", StringType()),
        StructField("s3_path", StringType()),
        StructField("size_bytes", LongType()),
        StructField("status", StringType()),
        StructField("ingest_date", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("error_msg", StringType())
    ])

    df_metadata = spark.createDataFrame(metadata, schema)

    meta_path = f"{BRONZE_BASE}/_metadata/ingest_date={ingest_date}"
    df_metadata.write.mode("append").parquet(meta_path)

    print(f"\nIngestion completed: {success_count}/{len(files)} successful")
    print(f"Metadata: {meta_path}")

    spark.stop()

if __name__ == "__main__":
    main()