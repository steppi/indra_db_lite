import indra_db_lite.locations as locations
from indra_db_lite.s3 import download_local_db_from_s3
import logging
import sys

logger = logging.getLogger(__file__)


if __name__ == '__main__':
    if locations.S3_BUCKET is None:
        logger.error("Shell variable INDRA_DB_LITE_S3_BUCKET is not set.")
        sys.exit(1)
    download_local_db_from_s3(
        locations.S3_BUCKET,
        locations.S3_KEY,
        locations.INDRA_DB_LITE_LOCATION,
    )
