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
from indra_db_lite.construction import import_csv_into_sqlite
from indra_db_lite.construction import query_to_csv

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


url = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz"


def download_gzip_file(url: str, outpath: str) -> None:
    """Download and decompress entrez gene to pmid file."""
    response = requests.get(url)
    compressed_file = io.BytesIO(response.content)
    decompressed_file = gzip.GzipFile(fileobj=compressed_file)
    with open(outpath, 'wb') as f:
        f.write(decompressed_file.read())


def load_entrez_gene_to_pmid_to_dataframe(path: str) -> pd.DataFrame:
    """Load entrez gene to pmid table into a pandas dataframe."""
    with open(path, 'rb') as f:
        table_bytes = io.BytesIO(f.read())
    df = pd.read_csv(
        table_bytes,
        sep='\t',
        names=['taxon_id', 'entrez_id', 'pmid'],
        comment='#',
        dtype='Int64',
    )
    df['uniprot_id'] = df.entrez_id.apply(_get_up_from_entrez_wrap)
    df['hgnc_id'] = df.entrez_id.apply(_get_hgnc_from_entrez_wrap)
    return df
