"""Assembles tables from multiple sources into a single sqlite database

The process is to construct multiple sqlite database files with various tables,
and then to copy the tables into a single database. This module provides a
function to copy a table from one sqlite database to another. Indices are not
copied, so all functions to add indices to tables are placed in this module and
indices are added to the final combined table.

A function is also provided that automates the process of moving all tables
into the final db and adding indices.
"""
import argparse
from contextlib import closing
import os
import sqlite3

from .tables.agent_texts import ensure_agent_texts_table
from .tables.best_content import ensure_best_content_table
from .tables.entrez import ensure_entrez_pmids_table
from .tables.pmid_text_refs import ensure_pmid_text_ref_table
from .util import get_sqlite_tables


def add_indices_to_best_content_table(sqlite_db_path: str) -> None:
    """Make queries to best content table more efficient."""
    query1 = """--
    CREATE INDEX IF NOT EXISTS
        best_content_text_ref_id_idx
    ON
        best_content(text_ref_id)
    """
    query2 = """--
    CREATE INDEX IF NOT EXISTS
        best_content_text_type_id_idx
    ON
        best_content(text_type)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            for query in query1, query2:
                cur.execute(query)
        conn.commit()


def add_indices_to_entrez_pmids_table(sqlite_db_path: str) -> None:
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            for query in [
                    """--
                    CREATE INDEX IF NOT EXISTS
                    entrez_pmids_entrez_id_idx
                    ON
                        entrez_pmids(entrez_id)
                    """,
                    """--
                    CREATE INDEX IF NOT EXISTS
                        entrez_pmids_uniprot_id_idx
                    ON
                        entrez_pmids(uniprot_id)
                    """,
                    """--
                    CREATE INDEX IF NOT EXISTS
                        entrez_pmids_hgnc_id_idx
                    ON
                        entrez_pmids(hgnc_id)
                    """,
            ]:
                cur.execute(query)


def add_indices_to_agent_texts_table(sqlite_db_path: str) -> None:
    query = """--
    CREATE INDEX IF NOT EXISTS
        agent_texts_agent_text_idx
    ON
        agent_texts(agent_text)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def add_indices_to_pmid_text_refs_table(sqlite_db_path: str) -> None:
    query = """--
    CREATE INDEX IF NOT EXISTS
        pmid_text_refs_pmid_idx
    ON
        pmid_text_refs(pmid)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def add_indices_to_mesh_pmids_table(sqlite_db_path) -> None:
    query = """--
    CREATE INDEX IF NOT EXISTS
        mesh_pmids_mesh_num_is_concept_idx
    ON
        mesh_pmids(mesh_num, is_concept)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def add_indices_to_mesh_xrefs_table(sqlite_db_path) -> None:
    query1 = """--
    CREATE INDEX IF NOT EXISTS
        mesh_xrefs_curie_idx
    ON
        mesh_xrefs(curie)
    """
    query2 = """--
    CREATE INDEX IF NOT EXISTS
        mesh_xrefs_mesh_num_is_concept_idx
    ON
        mesh_xrefs(mesh_num, is_concept)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            for query in query1, query2:
                cur.execute(query)
        conn.commit()


def move_table(from_db_path: str, to_db_path: str, table_name: str):
    assert table_name in get_sqlite_tables(from_db_path)
    assert table_name in get_sqlite_tables(to_db_path)
    with closing(sqlite3.connect(to_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute("ATTACH ? AS from_db;", (from_db_path, ))
            query = f"""--
            INSERT INTO
                {table_name}
            SELECT * FROM from_db.{table_name};
            """
            cur.execute(query)
            conn.commit()
            cur.execute("DETACH from_db;")


def construct_local_database(
        outpath: str,
        agent_texts_db_path: str,
        best_content_db_path: str,
        entities_db_path: str,
        pmid_text_refs_db_path: str,
        mesh_db_path: str,
) -> None:
    agent_texts_db_path = os.path.realpath(agent_texts_db_path)
    best_content_db_path = os.path.realpath(best_content_db_path)
    entities_db_path = os.path.realpath(entities_db_path)
    pmid_text_refs_db_path = os.path.realpath(pmid_text_refs_db_path)
    ensure_agent_texts_table(outpath)
    ensure_best_content_table(outpath)
    ensure_entrez_pmids_table(outpath)
    ensure_pmid_text_ref_table(outpath)
    move_table(agent_texts_db_path, outpath, 'agent_texts')
    move_table(best_content_db_path, outpath, 'best_content')
    move_table(entities_db_path, outpath, 'entrez_pmids')
    move_table(pmid_text_refs_db_path, outpath, 'pmid_text_refs')
    move_table(mesh_db_path, outpath, 'mesh_pmids')
    move_table(mesh_db_path, outpath, 'mesh_xrefs')
    add_indices_to_agent_texts_table(outpath)
    add_indices_to_best_content_table(outpath)
    add_indices_to_entrez_pmids_table(outpath)
    add_indices_to_pmid_text_refs_table(outpath)
    add_indices_to_mesh_pmids_table(outpath)
    add_indices_to_mesh_xrefs_table(outpath)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("outpath")
    parser.add_argument("agent_texts_db_path")
    parser.add_argument("best_content_db_path")
    parser.add_argument("entities_db_path")
    parser.add_argument("pmid_text_refs_db_path")
    parser.add_argument("mesh_db_path")

    args = parser.parse_args()

    construct_local_database(
        outpath=args.outpath,
        agent_texts_db_path=args.agent_texts_db_path,
        best_content_db_path=args.best_content_db_path,
        entities_db_path=args.entities_db_path,
        pmid_text_refs_db_path=args.pmid_text_refs_db_path,
        mesh_db_path=args.mesh_db_path
    )