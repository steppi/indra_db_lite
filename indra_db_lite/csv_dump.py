import logging
import subprocess
from indra_db.config import get_databases

logger = logging.getLogger(__name__)


def find_disallowed_keywords(query: str) -> list:
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

    disallowed_keywords = find_disallowed_keywords(query)

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


def best_content_table(output_location: str) -> None:
    """Dumps table with best content for each text_ref_id to csv

    Output table has columns text_ref_id, pmid, text_type, content
    where text_ref_id is an id for a row in indra_db's text_ref table,
    pmid is the corresponding pmid if one exists, text_type is the
    type of content (one of "fulltext", "abstract", or "title"), content
    is a binary blob containing compressed text content.

    Parameters
    ----------
    output_location : str
        Path to where output is to be stored.
    """
    query = """
    SELECT DISTINCT ON (text_ref.id)
        text_ref.id as text_ref_id, pmid_num as pmid, text_type, content
    FROM
        text_content
    JOIN
        text_ref
    ON
        text_ref.id = text_content.text_ref_id AND
        content is NOT NULL
    ORDER BY
        text_ref.id,
        CASE text_type
            WHEN 'fulltext' THEN 0
            WHEN 'abstract' THEN 1
            WHEN 'title' THEN 2
            ELSE 3
        END,
        pmid_num
    """
    query_to_csv(query, output_location)
