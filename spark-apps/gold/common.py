from pyspark.sql import SparkSession

# Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
SILVER_PATH = "s3a://silver/traffic_data"
GOLD_PATH = "s3a://gold/traffic_data"

# Postgres Config
PG_URL = "jdbc:postgresql://postgres:5432/analytics"
PG_USER = "spark_user"
PG_PASSWORD = "spark_password"
PG_DRIVER = "org.postgresql.Driver"

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.read.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.commit.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
        .config("spark.hadoop.fs.s3a.threads.max", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout", "300s") \
        .getOrCreate()
        
def write_to_gold(df, table_name, mode="overwrite"):
    """Write DataFrame to Gold layer in MinIO (Parquet)"""
    path = f"{GOLD_PATH}/{table_name}"
    print(f"Writing {table_name} to Gold MinIO at {path}...")
    df.write \
        .mode(mode) \
        .parquet(path)
    print(f"Finished writing {table_name} to Gold MinIO.")
    
def write_to_postgres(df, table_name, mode="append"):
    print(f"Writing {table_name} to PostgreSQL...")
    df.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", table_name) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", PG_DRIVER) \
        .option("batchsize", "10000") \
        .option("isolationLevel", "NONE") \
        .option("numPartitions", "4") \
        .mode(mode) \
        .save()
    print(f"Finished writing {table_name}.")
