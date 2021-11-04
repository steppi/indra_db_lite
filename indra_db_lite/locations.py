import os

S3_BUCKET = os.environ.get("INDRA_DB_LITE_S3_BUCKET")
S3_KEY = os.environ.get("INDRA_DB_LITE_S3_KEY")
if S3_KEY is None and S3_BUCKET is not None:
    S3_KEY = "indra_lite.db.xz"
INDRA_DB_LITE_LOCATION = os.environ.get("INDRA_DB_LITE_LOCATION")
