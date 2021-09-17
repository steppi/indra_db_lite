from contextlib import closing
from contextlib import contextmanager
import logging
from sqlalchemy import text
import sqlite3
import subprocess
from typing import List


from indra_db.config import get_databases
from indra_db.util import get_db


logger = logging.getLogger(__name__)


@contextmanager
def managed_db(db_label: str = "primary", protected: bool = False):
    db = get_db(db_label, protected)
    try:
        yield db
    finally:
        db.session.rollback()
        db.session.close()


def _get_postgres_tables(db_label: str = "primary") -> List[str]:
    query = "SELECT table_name FROM information_schema.tables"
    with managed_db(db_label) as db:
        res = db.session.execute(query)
    if not res:
        return []
    return [row[0] for row in res]


def _get_sqlite_tables(sqlite_db_path: str) -> List[str]:
    query = """--
    SELECT
        name
    FROM
        sqlite_master
    WHERE
        type = 'table' AND
        name NOT LIKE 'sqlite_%'
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            result = cur.execute(query).fetchall()
    if not result:
        return []
    return [row[0] for row in result]


def get_row_count_postgres(table_name: str, db_label: str = "primary") -> int:
    assert table_name in _get_postgres_tables(db_label)
    query = text(f"SELECT COUNT(*) FROM {table_name}")
    with managed_db(db_label) as db:
        result = db.session.execute(query).fetchall()
    if not result:
        return 0
    return result[0][0]


def get_row_count_sqlite(table_name: str, sqlite_db_path: str) -> int:
    assert table_name in _get_sqlite_tables(sqlite_db_path)
    query = f"SELECT COUNT(*) FROM {table_name}"
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            result = cur.execute(query).fetchall()
    return result[0][0]


def query_to_csv(
        query: str, output_location: str, db: str = "primary"
) -> None:
    """Dump results of query into a csv file.

    Parameters
    ----------
    query : str
        Postgresql query that returns a list of rows. Queries that perform
        writes of any kind should not be used. A simple keyword search is
        used to check for queries that may perform writes, but be careful,
        it shouldn't be trusted completely.

    output_location : str
        Path to file where output is to be stored.

    db : Optional[str]
        Database from list of defaults in indra_db config. Default: "primary".

    Raises
    ------
    AssertionError
        If input query contains a disallowed keyword that could lead to
        modifications of the database.
    """

    disallowed_keywords = _find_disallowed_keywords(query)

    try:
        assert not disallowed_keywords
    except AssertionError:
        logger.exception(
            f'Query "{query}" uses disallowed keywords: {disallowed_keywords}'
        )

    defaults = get_databases()
    try:
        db_uri = defaults[db]
    except KeyError:
        logger.exception(f"db {db} not available. Check db_config.ini")

    command = [
        "psql",
        db_uri,
        "-c",
        f"\\copy ({query}) to {output_location} with csv",
    ]
    subprocess.run(command)


def _find_disallowed_keywords(query: str) -> list:
    """Returns list of disallowed keywords in query if any exist.

    Keywords are disallowed if they can lead to modifications of the
    database or its settings. The disallowed keywords are:

    alter, call, commit, create, delete, drop, explain, grant, insert,
    lock, merge, rename,  revoke, savepoint, set, rollback, transaction,
    truncate, update.

    Matching is case insensitive.

    Parameters
    ----------
    query : str
        A string containing a Postgresql query.

    Returns
    -------
    list
        List of keywords within the query that are within the disallowed
        list. Entries are returned in all lower case.
    """
    disallowed = [
        "alter",
        "call",
        "commit",
        "create",
        "delete",
        "drop",
        "explain",
        "grant",
        "insert",
        "lock",
        "merge",
        "rename",
        "revoke",
        "savepoint",
        "set",
        "rollback",
        "transaction",
        "truncate",
        "update",
    ]

    query_token_set = set(token.lower() for token in query.split())
    return list(query_token_set & set(disallowed))


def import_csv_into_sqlite(
        csv_table_path: str,
        table_name: str,
        sqlite_db_path: str
) -> None:
    """Load csv into sqlite database."""
    subprocess.run(
        [
            'sqlite3',
            '-separator',
            ',',
            sqlite_db_path,
            f".import {csv_table_path} {table_name}",
        ]
    )
