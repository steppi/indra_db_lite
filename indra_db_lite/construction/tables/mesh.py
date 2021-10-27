import argparse
from contextlib import closing
import gzip
import io
import logging
import os
import pandas as pd
import requests
import sqlite3

from famplex import load_equivalences
from indra.databases.identifiers import ensure_prefix_if_needed
from indra.databases.identifiers import get_ns_from_identifiers
from indra.databases.mesh_client import mesh_to_db
from indra.statements.validate import validate_id

from indra_db_lite.construction import import_csv_into_sqlite
from indra_db_lite.api import mesh_id_to_mesh_num
from indra_db_lite.construction import query_to_csv


def download_mesh_javert_xrefs(outpath: str) -> None:
    javert_url = "https://zenodo.org/record/4661382/files/xrefs.tsv.gz"
    response = requests.get(javert_url)
    compressed_file = io.BytesIO(response.content)
    decompressed_file = gzip.GzipFile(fileobj=compressed_file)
    df = pd.read_csv(decompressed_file, sep='\t', low_memory=False)
    from_mesh = df[df.prefix == 'mesh'].copy()
    to_mesh = df[df.xref_prefix == 'mesh'].copy()
    df = None
    from_mesh.loc[:, 'prefix'] = from_mesh.prefix.apply(
        get_ns_from_identifiers
    )
    from_mesh.loc[:, 'xref_prefix'] = from_mesh.xref_prefix.apply(
        get_ns_from_identifiers
    )
    from_mesh.dropna(inplace=True)
    from_mesh.loc[:, 'xref_identifier'] = from_mesh.apply(
        lambda row: ensure_prefix_if_needed(
            row.xref_prefix, row.xref_identifier
        ),
        axis=1,
    )
    from_mesh = from_mesh[
        from_mesh.apply(
            lambda row: validate_id(row.prefix, row.identifier) and
            validate_id(row.xref_prefix, row.xref_identifier),
            axis=1,
        )
    ]
    to_mesh.loc[:, 'prefix'] = to_mesh.prefix.apply(
        get_ns_from_identifiers
    )
    to_mesh.loc[:, 'xref_prefix'] = to_mesh.xref_prefix.apply(
        get_ns_from_identifiers
    )
    to_mesh.dropna(inplace=True)
    to_mesh.loc[:, 'identifier'] = to_mesh.apply(
        lambda row: ensure_prefix_if_needed(
            row.prefix, row.identifier
        ),
        axis=1,
    )
    to_mesh = to_mesh[
        to_mesh.apply(
            lambda row: validate_id(row.prefix, row.identifier) and
            validate_id(row.xref_prefix, row.xref_identifier),
            axis=1,
        )
    ]
    from_mesh['curie'] = from_mesh.apply(
        lambda row: f"{row.xref_prefix}:{row.xref_identifier}",
        axis=1,
    )
    to_mesh['curie'] = to_mesh.apply(
        lambda row: f"{row.prefix}:{row.identifier}",
        axis=1
    )
    from_mesh[['mesh_num', 'is_concept']] = from_mesh.apply(
        lambda row: mesh_id_to_mesh_num(row.identifier),
        axis=1,
        result_type="expand",
    )
    to_mesh[['mesh_num', 'is_concept']] = to_mesh.apply(
        lambda row: mesh_id_to_mesh_num(row.xref_identifier),
        axis=1,
        result_type="expand",
    )
    from_mesh = from_mesh[['mesh_num', 'is_concept', 'curie']]
    to_mesh = to_mesh[['mesh_num', 'is_concept', 'curie']]
    result = pd.concat([from_mesh, to_mesh])
    result.to_csv(outpath, sep='\t', index=False)


def mesh_annotations_to_csv(outpath: str) -> None:
    query = """
    SELECT DISTINCT
        id, pmid_num, mesh_num, major_topic, is_concept
    FROM
        mesh_ref_annotations
    """
    query_to_csv(query, outpath)


def ensure_mesh_table(sqlite_db_path: str) -> None:
    query = """--
    CREATE TABLE IF NOT EXISTS mesh (
    id INTEGER PRIMARY KEY,
    pmid_num INTEGER,
    mesh_num INTEGER,
    major_topic INTEGER,
    is_concept INTEGER
    );
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def insert_into_mesh_table(mesh_csv_path: str, sqlite_db_path: str) -> None:
    ensure_mesh_table(sqlite_db_path)
    import_csv_into_sqlite(mesh_csv_path, 'mesh', sqlite_db_path)


def add_indices_to_mesh_table(sqlite_db_path) -> None:
    query1 = """--
    CREATE INDEX IF NOT EXISTS
        mesh_mesh_num_idx
    ON
        mesh(mesh_num)
    """
    query2 = """--
    CREATE INDEX IF NOT EXISTS
        mesh_mesh_num_major_topic_idx
    ON
        mesh(mesh_num, major_topic)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            for query in query1, query2:
                cur.execute(query)
        conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("outpath")
    args = parser.parse_args()
    outpath = args.outpath
    mesh_db_path = os.path.join(outpath, 'mesh.db')
    csv_path = os.path.join(outpath, 'mesh.csv')
    logging.basicConfig(
        filename=os.path.join(outpath, 'mesh.log'),
        filemode='a',
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M',
        level=logging.DEBUG,
        force=True,
    )
    logger = logging.getLogger(__name__)
    if not os.path.exists(mesh_db_path):
        if not os.path.exists(csv_path):
            logger.info('Loading mesh annotations into csv file.')
            mesh_annotations_to_csv(csv_path)
        logger.info('Constructing mesh annotations table.')
        ensure_mesh_table(mesh_db_path)
    insert_into_mesh_table(csv_path, mesh_db_path)
    logger.info('Adding indices to mesh annotations table.')
    add_indices_to_mesh_table(mesh_db_path)
