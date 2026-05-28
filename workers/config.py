import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = "document-processing-group"
KAFKA_TOPIC = "document-processing"
KAFKA_POLL_TIMEOUT_S = 1.0

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:new_password@localhost:5432/messaging",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "documents")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

# Safety cap: skip pages beyond this count instead of running forever.
# At ~2 KB of text per page, 500 pages ≈ 1 MB of clean text.
MAX_PAGES = 500
