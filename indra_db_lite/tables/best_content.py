from contextlib import closing
import json
import logging
import os
import sqlite3

from indra.literature.adeft_tools import universal_extract_paragraphs
from indra_db.util.helpers import unpack

from indra_db_lite.csv import import_csv_into_sqlite
from indra_db_lite.csv import query_to_csv

logger = logging.getLogger(__name__)


def _extract_then_dump(hex_string: str) -> str:
    """Extract compressed content into it's paragraphs."""
    return json.dumps(
        universal_extract_paragraphs(
            unpack(bytes.fromhex(hex_string))
        )
    )


def text_content_to_csv(outpath: str) -> None:
    """Dump indra databases text content table into a local csv file."""
    query = """
    SELECT
        id, text_ref_id, text_type, source, encode(content, 'hex')
    FROM
        text_content
    WHERE
        source NOT LIKE 'xdd%'
    """
    query_to_csv(query, outpath)


def ensure_text_content_table(sqlite_db_path: str) -> None:
    """Create the local text_content table if it doesn't exist."""
    query = \
        """-- Create text content table if it doesn't exist
        CREATE TABLE IF NOT EXISTS text_content (
            id INTEGER PRIMARY KEY,
            text_ref_id INTEGER NOT NULL,
            text_type TEXT NOT NULL,
            source TEXT NOT NULL,
            content TEXT NOT NULL
        );
        """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)


def import_into_text_content_table(
        table_path: str,
        sqlite_db_path: str
) -> None:
    """Load rows into the local text_content table."""
    ensure_text_content_table(sqlite_db_path)
    import_csv_into_sqlite(table_path, 'text_content', sqlite_db_path)


def add_indices_to_text_content_table(sqlite_db_path: str) -> None:
    """Make key queries to local text_content table more efficient."""
    query1 = """--
    CREATE INDEX IF NOT EXISTS
        text_content_text_ref_idx
    ON
        text_content(text_ref_id)
    """
    query2 = """--
    CREATE INDEX IF NOT EXISTS
        text_content_text_ref_text_type_idx
    ON
        text_content(text_ref_id, text_type)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query1)
            cur.execute(query2)
        conn.commit()


def delete_content_for_which_fulltext_exists(sqlite_db_path: str) -> None:
    """Delete titles and abstracts for articles for which fulltext exists."""
    query = """--
    DELETE FROM text_content
    WHERE text_ref_id IN (
        SELECT text_ref_id FROM text_content WHERE text_type = 'fulltext'
    ) AND (text_type = 'abstract' OR text_type = 'title')
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            conn.commit()


def delete_duplicate_fulltexts(sqlite_db_path: str) -> None:
    """Delete duplicate fulltexts, prioritizing duplicates by source."""
    query = """--
    DELETE FROM text_content
    WHERE
        text_type = 'fulltext' AND
        id NOT IN (
        SELECT id FROM (
            SELECT
                id, MIN(CASE source
                        WHEN 'pmc_oa' THEN 0
                        WHEN 'manuscripts' THEN 1
                        WHEN 'cord19_pmc_xml' THEN 2
                        WHEN 'elsevier' THEN 3
                        WHEN 'cord19_pdf' THEN 4
                        ELSE 5
                        END)
            FROM
                text_content
            GROUP BY text_ref_id)
    )
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            conn.commit()


def delete_duplicate_abstracts(sqlite_db_path: str) -> None:
    """Delete duplicate abstracts, prioritizing duplicates by source."""
    query = """--
    DELETE FROM text_content
    WHERE
        text_type = 'abstract' AND
        id NOT IN (
        SELECT id FROM (
            SELECT
                id, MIN(CASE source
                        WHEN 'pubmed' THEN 0
                        WHEN 'cord19_abstract' THEN 1
                        ELSE 2
                        END)
            FROM
                text_content
            GROUP BY text_ref_id)
    )
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            conn.commit()


def combine_abstracts_with_titles(sqlite_db_path: str) -> None:
    """Create new table with rows containing title and abstracts."""
    query = """--
    CREATE TABLE IF NOT EXISTS abstracts AS
    SELECT
        tc1.id AS tcid1, tc2.id AS tcid2,
        tc1.text_ref_id AS text_ref_id, tc1.text_type AS text_type,
        tc1.source AS source, tc2.content AS title, tc1.content AS abstract
    FROM
        text_content tc1
    INNER JOIN
        text_content tc2
    ON
        tc1.text_type = 'abstract' AND
        tc2.text_type = 'title' AND
        tc1.text_ref_id = tc2.text_ref_id
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def delete_abstracts(sqlite_db_path):
    query = """--
    DELETE FROM text_content
    WHERE text_type = 'abstract'
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            cur.execute('VACUUM')
            conn.commit()


def delete_titles_for_which_abstracts_exist(sqlite_db_path):
    query = """--
    DELETE FROM text_content
    WHERE text_ref_id IN (
        SELECT text_ref_id FROM abstracts
    ) AND text_type = 'title'
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            conn.commit()
            cur.execute('VACUUM')


def ensure_best_content_table(sqlite_db_path):
    query = """--
    CREATE TABLE IF NOT EXISTS best_content (
        id INTEGER PRIMARY KEY,
        text_ref_id INTEGER,
        text_content_id1 INTEGER,
        text_content_id2 INTEGER,
        text_type TEXT,
        content TEXT,
        UNIQUE(text_content_id1),
        UNIQUE(text_ref_id))
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def abstracts_generator(sqlite_db_path, batch_size=10000):
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute('SELECT * FROM abstracts')
            while True:
                result = cur.fetchmany(batch_size)
                if not result:
                    break
                yield [
                    [
                        None,
                        text_ref_id,
                        tcid1,
                        tcid2,
                        text_type,
                        json.dumps(
                            [
                                unpack(bytes.fromhex(title)),
                                unpack(bytes.fromhex(abstract)),
                            ]
                        )
                    ]
                    for (
                            tcid1,
                            tcid2,
                            text_ref_id,
                            text_type,
                            _,
                            title,
                            abstract,
                    ) in result
                ]


def fulltexts_and_titles_generator(sqlite_db_path, batch_size=1000):
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute('SELECT * FROM text_content')
            while True:
                result = cur.fetchmany(batch_size)
                if not result:
                    break
                yield [
                    [
                        None,
                        text_ref_id,
                        tcid,
                        None,
                        text_type,
                        _extract_then_dump(content),
                    ]
                    for (
                            tcid,
                            text_ref_id,
                            text_type,
                            _,
                            content
                    ) in result
                ]


def load_best_content_table(sqlite_db_path, row_generator):
    """Load best content table with rows from generator."""
    insertion_query = """--
    INSERT INTO
        best_content (id, text_ref_id,
                      text_content_id1, text_content_id2, text_type, content)
    VALUES (?, ?, ?, ?, ?, ?);
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute('PRAGMA journal_mode = WAL')
            cur.execute('PRAGMA synchronous = NORMAL')
            ensure_best_content_table(sqlite_db_path)
            for block, row_list in enumerate(row_generator):
                logger.info(
                    f"Inserting {len(row_list)} rows from block {block}"
                )
                cur.executemany(insertion_query, row_list)
                conn.commit()
            cur.execute('PRAGMA journal_mode = DELETE')


if __name__ == '__main__':
    db_path = '/adeft/db_lite/text_content.db'
    logger.info('loading text content to csv file')
    text_content_to_csv('/adeft/text_content.csv')
    logger.info('importing text content into sqlite db')
    import_into_text_content_table(
        '/adeft/text_content.db', db_path,
    )
    logger.info('removing text content csv')
    os.remove('/adeft/text_content.csv')
    logger.info('adding indices to text content table of sqlite db')
    add_indices_to_text_content_table(db_path)
    logger.info('Removing lesser content for which fulltexts exist.')
    delete_content_for_which_fulltext_exists(db_path)
    logger.info('Removing duplicate fulltexts')
    delete_duplicate_fulltexts(db_path)
    logger.info('Removing duplicate abstracts')
    delete_duplicate_abstracts(db_path)
    logger.info('combining abstracts with titles into new table')
    combine_abstracts_with_titles(db_path)
    logger.info('deleting abstracts from text content table')
    delete_abstracts(db_path)
    logger.info('deleting remaining titles that aren\'t best content')
    delete_titles_for_which_abstracts_exist(db_path)
    logger.info('loading abtracts into best content table')
    gen = abstracts_generator(db_path, batch_size=500000)
    load_best_content_table(db_path, gen)
    logger.info('dropping temporary abstracts table')
    with closing(sqlite3.connect(db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute('DROP TABLE abstracts')
            cur.execute('VACUUM')
    logger.info('loading fulltexts and titles into best content table')
    gen = fulltexts_and_titles_generator(db_path, batch_size=10000)
    load_best_content_table(db_path, gen)
    logger.info('removing temporary text content table')
    with closing(sqlite3.connect(db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute('DROP TABLE text_content')
            cur.execute('VACUUM')
