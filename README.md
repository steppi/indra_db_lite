# indra_db_lite
Work with content from indra_db in a local sqlite database.

## Installation
Clone this repo and either add the folder into which it was cloned to your
PYTHONPATH, or run `pip install -e .` from within the top level of the folder into
which it was cloned.

If you have a compressed database file stashed
on Amazon S3, set the environment variable `INDRA_DB_LITE_S3_BUCKET` to the S3 bucket
where the database file is located and the environment variable
`INDRA_DB_LITE_LOCATION` to the path where would like the decompressed database file
to be stored on your machine. The database file will be over 150GB when
decompressed so ensure that there is enough space for it. If the S3 key for
the compressed db file is something other than `indra_lite.db.xz`, then set the
environment variable `INDRA_DB_LITE_S3_KEY` to the actual key.

Once these environment variables are set correctly, run

    $ python -m indra_db_lite.download

to download the sqlite db file to your machine and decompress it. Note that it may
take over an hour to download and decompress the db file. Once completed,
indra_db_lite should be ready to use.


Run each script in construct/tables separately
and then run construct/assembly
