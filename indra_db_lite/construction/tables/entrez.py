import argparse
from contextlib import closing
import gzip
import io
import logging
import os
import pandas as pd
import requests
import sqlite3

from indra.databases.hgnc_client import get_hgnc_from_entrez
from protmapper.uniprot_client import get_id_from_entrez

from indra_db_lite.construction import get_sqlite_tables


logger = logging.getLogger(__name__)


def _get_up_from_entrez_wrap(x):
    result = get_id_from_entrez(str(x))
    if result is None:
        return pd.NA
    return result


def _get_hgnc_from_entrez_wrap(x):
    result = get_hgnc_from_entrez(str(x))
    if result is None:
        return pd.NA
    return result


def download_entrez_pmids(outpath: str) -> None:
    """Download and decompress entrez gene to pmid file."""
    entrez_pmid_url = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz"
    response = requests.get(entrez_pmid_url)
    compressed_file = io.BytesIO(response.content)
    decompressed_file = gzip.GzipFile(fileobj=compressed_file)
    with open(outpath, 'wb') as f:
        f.write(decompressed_file.read())


def ensure_entrez_pmids_table(sqlite_db_path: str):
    query = """--
    CREATE TABLE IF NOT EXISTS entrez_pmids (
        id INTEGER PRIMARY KEY,
        taxon_id INTEGER,
        entrez_id INTEGER,
        uniprot_id TEXT,
        hgnc_id INTEGER,
        pmid INTEGER
    );
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def create_entrez_pmids_table(
        table_path: str, sqlite_db_path: str
) -> pd.DataFrame:
    ensure_entrez_pmids_table(sqlite_db_path)
    df = pd.read_csv(
        table_path,
        sep='\t',
        names=['taxon_id', 'entrez_id', 'pmid'],
        comment='#',
        dtype='Int64',
    )
    df['uniprot_id'] = df.entrez_id.apply(_get_up_from_entrez_wrap)
    df['hgnc_id'] = df.entrez_id.apply(_get_hgnc_from_entrez_wrap)
    df = df[['taxon_id', 'entrez_id', 'uniprot_id', 'hgnc_id', 'pmid']]
    df = df.reset_index().rename({'index': 'id'}, axis=1)
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        df.to_sql('entrez_pmids', conn, if_exists='append', index=False)
        conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("outpath")
    args = parser.parse_args()
    outpath = args.outpath

    entrez_pmids_csv_path = os.path.join(outpath, 'entrez_pmids.csv')
    entities_db_path = os.path.join(outpath, 'entities.db')

    logging.basicConfig(
        filename=os.path.join(outpath, 'entities.log'),
        filemode='a',
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M',
        level=logging.DEBUG,
        force=True,
    )
    logger = logging.getLogger(__name__)

    if (
            not os.path.exists(entities_db_path) or
            'entrez_pmids' not in get_sqlite_tables(entities_db_path)
    ):
        logger.info("Download entrez id to pmid map.")
        download_entrez_pmids(entrez_pmids_csv_path)
        logger.info("Creating entrez pmid sqlite table")
        create_entrez_pmids_table(entrez_pmids_csv_path, entities_db_path)
