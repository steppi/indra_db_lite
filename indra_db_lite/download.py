"""Handles download (and upload) of local db from (and to) s3."""

import boto3
import logging
import os
import subprocess
import sys

from indra_db_lite import locations


logger = logging.getLogger(__file__)


def compress_local_db(sqlite_db_path: str, n_threads=1) -> None:
    assert isinstance(n_threads, int)
    thread_flag = "" if n_threads == 1 else f"-T{n_threads}"
    subprocess.run(
        ["xz", "-v", "-1", thread_flag, sqlite_db_path]
    )


def decompress_local_db(
        compressed_db_path: str, outpath: str
) -> None:
    subprocess.run(
        ["xz", "-v", "--decompress", "-1", compressed_db_path]
    )
    #  xz will not decompress if the file extension is not .xz. It's output
    # will have the same name as the input with the extension chopped off.
    # We can therefore assume that chopping off the last three characters
    # will give the correct path to the output of xz.
    os.rename(compressed_db_path[:-3], outpath)


def upload_to_s3(
        path: str,
        bucket: str = locations.S3_BUCKET,
        key: str = locations.S3_KEY,
) -> None:
    client = boto3.client('s3')
    client.upload_file(path, bucket, key)


def download_local_db_from_s3(
        bucket: str = locations.S3_BUCKET,
        key: str = locations.S3_KEY,
        outpath: str = locations.INDRA_DB_LITE_LOCATION
) -> None:
    client = boto3.client('s3')
    logger.info("Downloading compressed db from s3.")
    with open(outpath + '.xz', 'wb') as f:
        client.download_fileobj(bucket, key, f)
    logger.info("Decompressing local db.")
    decompress_local_db(outpath + '.xz', outpath)


if __name__ == '__main__':
    if locations.S3_BUCKET is None:
        logger.error("Shell variable INDRA_DB_LITE_S3_BUCKET is not set.")
        sys.exit(1)
    download_local_db_from_s3(
        locations.S3_BUCKET,
        locations.S3_KEY,
        locations.INDRA_DB_LITE_LOCATION,
    )
