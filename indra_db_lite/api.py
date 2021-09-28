from contextlib import closing
import json
import sqlite3
from typing import Collection, Dict, List, Optional

from indra_db_lite.locations import INDRA_DB_LITE_LOCATION


def get_paragraphs_for_text_ref_ids(
        text_ref_ids: Collection[int],
        text_types: Optional[Collection[str]] = None,
) -> Dict[int, List[str]]:
    if text_types is None:
        text_types = ('fulltext', 'abstract', 'title')
    else:
        text_types = tuple(text_types)
    text_ref_ids = tuple(text_ref_ids)
    query = f"""SELECT
                text_ref_id, content
            FROM
                best_content
            WHERE
                text_ref_id IN ({','.join(['?']*len(text_ref_ids))}) AND
                text_type in ({','.join(['?']*len(text_types))})
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            paragraphs_list = cur.execute(
                query, text_ref_ids + text_types
            ).fetchall()
    return {
        text_ref_id: json.loads(paragraphs)
        for text_ref_id, paragraphs in paragraphs_list
    }


def get_text_ref_ids_for_pmids(
        pmids: Collection[int]
) -> Dict[int, int]:
    pmids = tuple(pmids)
    query = f"""--
    SELECT
        pmid, text_ref_id
    FROM
        pmid_text_refs
    WHERE
        pmid IN ({','.join(['?']*len(pmids))})
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            pmid_text_refs = cur.execute(query, pmids).fetchall()
    return {
        pmid: text_ref_id for pmid, text_ref_id in pmid_text_refs
    }


def get_pmids_for_text_ref_ids(
        text_ref_ids: Collection[int]
) -> Dict[int, int]:
    text_ref_ids = tuple(text_ref_ids)
    query = f"""--
    SELECT
        text_ref_id, pmid
    FROM
        pmid_text_refs
    WHERE
        text_ref_id IN ({','.join(['?']*len(text_ref_ids))}) AND
        pmid IS NOT NULL
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            text_ref_pmids = cur.execute(query, text_ref_ids).fetchall()
    return {
        text_ref_id: pmid for text_ref_id, pmid in text_ref_pmids
    }


def get_text_ref_ids_for_agent_text(agent_text: str) -> List[int]:
    query = """--
    SELECT
        text_ref_id
    FROM
        agent_texts
    WHERE
        agent_text = ?;
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(query, (agent_text, )).fetchall()
    return [row[0] for row in res]


def get_entrez_pmids_for_hgnc(hgnc_id: str) -> List[int]:
    query = """--
    SELECT
        pmid
    FROM
        entrez_pmids
    WHERE
        hgnc_id = ?;
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(query, (hgnc_id, )).fetchall()
    return [row[0] for row in res]


def get_entrez_pmids_for_uniprot(uniprot_id: str) -> List[int]:
    query = """--
    SELECT
        pmid
    FROM
        entrez_pmids
    WHERE
        uniprot_id = ?;
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(query, (uniprot_id, )).fetchall()
    return [row[0] for row in res]


def get_entrez_pmids(entrez_id: int) -> List[int]:
    query = """--
    SELECT
        pmid
    FROM
        entrez_pmids
    WHERE
        entrez_id = ?;
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(query, (entrez_id, )).fetchall()
    return [row[0] for row in res]
