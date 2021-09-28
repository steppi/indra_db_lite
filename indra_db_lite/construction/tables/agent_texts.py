import argparse
from contextlib import closing
import logging
import os
import sqlite3

from indra_db_lite.construction import get_sqlite_tables
from indra_db_lite.construction import import_csv_into_sqlite
from indra_db_lite.construction import query_to_csv

logger = logging.getLogger(__name__)


def agent_text_stmts_to_csv(outpath: str) -> None:
    query = """
    SELECT
        id, db_id, stmt_id
    FROM
        raw_agents
    WHERE
        db_name = 'TEXT' AND
        stmt_id IS NOT NULL
    """
    query_to_csv(query, outpath)


def stmts_readings_to_csv(outpath: str) -> None:
    query = """
    SELECT
        id, reading_id
    FROM
        raw_statements
    WHERE
        reading_id IS NOT NULL
    """
    query_to_csv(query, outpath)


def readings_content_to_csv(outpath: str) -> None:
    query = """
    SELECT
        id, text_content_id
    FROM
        reading
    """
    query_to_csv(query, outpath)


def content_text_refs_to_csv(outpath: str) -> None:
    query = """
    SELECT
        id, text_ref_id
    FROM
        text_content
    """
    query_to_csv(query, outpath)


def create_temp_agent_text_tables(
        agent_stmts_path: str,
        stmt_readings_path: str,
        reading_content_path: str,
        content_text_refs_path: str,
        sqlite_db_path: str,
) -> None:
    query1 = \
        """--
        CREATE TABLE IF NOT EXISTS agent_stmts (
        id INTEGER PRIMARY KEY,
        agent_text TEXT,
        stmt_id INTEGER
        );
        """
    query2 = \
        """--
        CREATE TABLE IF NOT EXISTS stmt_readings (
        stmt_id INTEGER PRIMARY KEY,
        reading_id INTEGER
        );
        """
    query3 = \
        """--
        CREATE TABLE IF NOT EXISTS reading_content (
        reading_id INTEGER PRIMARY KEY,
        text_content_id INTEGER
        );
        """
    query4 = \
        """--
        CREATE TABLE IF NOT EXISTS content_text_refs (
        text_content_id INTEGER PRIMARY KEY,
        text_ref_id INTEGER
        );
        """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            for query in query1, query2, query3, query4:
                cur.execute(query)
        conn.commit()

    import_csv_into_sqlite(agent_stmts_path, 'agent_stmts', sqlite_db_path)
    import_csv_into_sqlite(stmt_readings_path, 'stmt_readings', sqlite_db_path)
    import_csv_into_sqlite(
        reading_content_path, 'reading_content', sqlite_db_path
    )
    import_csv_into_sqlite(
        content_text_refs_path, 'content_text_refs', sqlite_db_path
    )


def add_indices_to_temp_agent_text_tables(sqlite_db_path: str) -> None:
    query1 = """--
    CREATE INDEX IF NOT EXISTS
        agent_stmts_stmt_id_idx
    ON
        agent_stmts(stmt_id)
    """
    query2 = """--
    CREATE INDEX IF NOT EXISTS
        stmt_readings_reading_id_idx
    ON
        stmt_readings(reading_id)
    """
    query3 = """--
    CREATE INDEX IF NOT EXISTS
        reading_content_text_content_id_idx
    ON
        stmt_readings(reading_id)
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            for query in query1, query2, query3:
                cur.execute(query)
        conn.commit()


def ensure_agent_texts_table(sqlite_db_path: str) -> None:
    query = """--
    CREATE TABLE IF NOT EXISTS agent_texts (
    id INTEGER PRIMARY KEY,
    agent_text TEXT,
    text_ref_id INTEGER
    );
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def create_agent_texts_table(sqlite_db_path: str) -> None:
    all_tables = get_sqlite_tables(sqlite_db_path)
    needed_tables = {
        'agent_stmts', 'stmt_readings', 'reading_content', 'content_text_refs'
    }
    try:
        assert needed_tables <= set(all_tables)
    except AssertionError:
        logger.exception('Necessary temporary tables do not exist.')
    ensure_agent_texts_table(sqlite_db_path)
    query = """--
    INSERT OR IGNORE INTO
        agent_texts
    SELECT
        NULL, ags.agent_text, ct.text_content_id
    FROM
        agent_stmts ags
    JOIN
        stmt_readings sr
    ON
        ags.stmt_id = sr.stmt_id
    JOIN
        reading_content rc
    ON
        sr.reading_id = rc.reading_id
    JOIN
        content_text_refs ct
    ON
        rc.text_content_id = ct.text_content_id
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('outpath')
    args = parser.parse_args()
    outpath = args.outpath

    logging.basicConfig(
        filename=os.path.join(outpath, 'agent_texts.log'),
        filemode='a',
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M',
        level=logging.DEBUG,
        force=True,
    )
    logger = logging.getLogger(__name__)
    logger.info('Constructing agent texts table')
    csv_files = [
        os.path.join(outpath, csv_file) for csv_file in
        [
            'agent_stmts.csv',
            'stmt_readings.csv',
            'reading_content.csv',
            'content_text_refs.csv',
        ]
    ]

    agent_texts_db_path = os.path.join(outpath, 'agent_texts.db')
    for filepath, function in zip(
            csv_files,
            (
                agent_text_stmts_to_csv,
                stmts_readings_to_csv,
                readings_content_to_csv,
                content_text_refs_to_csv,
            )
    ):
        if os.path.exists(filepath):
            continue
        logger.info(f'Dumping to csv {function.__name__}')
        function(filepath)

    if not os.path.exists(agent_texts_db_path):
        logger.info('Loading csv files into temporary tables in sqlite.')
        create_temp_agent_text_tables(*csv_files, agent_texts_db_path)
        logger.info('Adding indices to temporary tables.')
        add_indices_to_temp_agent_text_tables(agent_texts_db_path)
        for filepath in csv_files:
            os.remove(filepath)
    if 'agent_texts' not in get_sqlite_tables(agent_texts_db_path):
        logger.info('Constructing agent texts table with one big join.')
        create_agent_texts_table(agent_texts_db_path)
