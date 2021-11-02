import re
from contextlib import closing
import json
import sqlite3
from typing import Collection, Dict, List, Iterator, Optional, Tuple, Union

from indra_db_lite.locations import INDRA_DB_LITE_LOCATION


__all__ = [
    "get_entrez_pmids",
    "get_entrez_pmids_for_hgnc",
    "get_entrez_pmids_for_uniprot",
    "get_mesh_terms_for_grounding",
    "get_paragraphs_for_text_ref_ids",
    "get_plaintexts_for_text_ref_ids",
    "get_pmids_for_mesh_term",
    "get_pmids_for_text_ref_ids",
    "get_taxon_id_for_uniprot",
]


def filter_paragraphs(
        paragraphs: List[int],
        contains: Optional[Union[Collection[str], str]] = None
):
    """Filter paragraphs to only those containing one of a list of strings

    Parameters
    ----------
    paragraphs : list of str
        List of plaintext paragraphs from an article

    contains : str or list of str
        Exclude paragraphs not containing this string as a token, or
        at least one of the strings in contains if it is a list

    Returns
    -------
    str
        Plaintext consisting of all input paragraphs containing at least
        one of the supplied tokens.
    """
    if contains is None:
        pattern = ''
    else:
        if isinstance(contains, str):
            contains = [contains]
        pattern = '|'.join(r'(^|[^\w])%s([^\w]|$)' % re.escape(shortform)
                           for shortform in contains)
    paragraphs = [p for p in paragraphs if re.search(pattern, p)]
    return '\n'.join(paragraphs) + '\n'


class TextContent:
    __slots__ = ['fulltexts', 'abstracts', 'titles', 'processed']
    """Stores text content results returned from local db

    Attributes
    ----------
    processed : bool
        If False, then each piece of text content is stored as a list of
        paragraphs. Processing entails converting each list of paragraphs into
        a concatentation of the paragraphs joined by newlines with the option
        of filtering only to paragraphs containing a token or n-gram within
        a given list of tokens or n-grams. The processing step also allows
        filtering to content from only certain text types.
    fulltexts : dict
        Dictionary mapping text_ref_ids to fulltext content.
    abstracts : dict
        Dictionary mapping text_ref_ids to content from abstracts.
    titles: dict
        Dictionary mapping text_ref_ids to content from titles.
    """
    def __init__(
            self, content_rows: Iterator[Tuple[int, str, List[str]]]
    ) -> None:
        self.processed: bool = False
        self.fulltexts: Dict[int, Union[List[str], str]] = {}
        self.abstracts: Dict[int, Union[List[str], str]] = {}
        self.titles: Dict[int, Union[List[str], str]] = {}
        for text_ref_id, text_type, content in content_rows:
            content = json.loads(content)
            if text_type == 'fulltext':
                self.fulltexts[text_ref_id] = content
            if text_type == 'abstract':
                self.abstracts[text_ref_id] = content
            if text_type == 'title':
                self.titles[text_ref_id] = content

    def __len__(self) -> int:
        return len(self.fulltexts) + len(self.abstracts) + len(self.titles)

    def __iter__(self) -> Iterator[str]:
        for content in self.fulltexts.values():
            yield content
        for content in self.abstracts.values():
            yield content
        for content in self.titles.values():
            yield content

    def process(
            self,
            contains: Optional[Union[Collection[str], str]] = None,
            text_types: Optional[Collection[str]] = None,
    ) -> None:
        """Processes content and updates object in place

        Before processing, each piece of content is stored as a list of
        paragraphs. Processing concatenates the paragraphs, separating by
        newline. There is an option to filter to paragraphs containing only
        certain unigrams or n-grams. There is also an option to filter to
        content only of specific text types.

        A TextContent object can only be processed once. This is an
        irreversible operation.

        contains : Optional[str of Collection of str]
            If a single string, filter to only paragraphs containing this
            token, (or n-gram in the case where the string contains multiple
            tokens separated by space characters). If a list of strings is
            passed, filter to only paragraphs that contain one or more of these
            tokens or n-grams.

        text_type : Optional[Collection of str]
            A Collection containing one or more of the strings "fulltext",
            "abstract", or "title". If None is passed, then all text_types will
            be included. The dictionary attributes for text_types not included
            will be set to empty in place.
        """
        if self.processed:
            return
        if text_types is None:
            text_types = ['fulltexts', 'abstracts', 'titles']
        if 'fulltexts' in text_types:
            fulltexts = {
                text_ref_id: filter_paragraphs(paragraphs, contains=contains)
                for text_ref_id, paragraphs in self.fulltexts.items()
            }
            self.fulltexts = {
                text_ref_id: text for text_ref_id, text in fulltexts.items()
                if len(text) > 1
            }
        else:
            self.fulltexts = {}
        if 'abstracts' in text_types:
            abstracts = {
                text_ref_id: filter_paragraphs(paragraphs, contains=contains)
                for text_ref_id, paragraphs in self.abstracts.items()
            }
            self.abstracts = {
                text_ref_id: text for text_ref_id, text in abstracts.items()
                if len(text) > 1
            }
        else:
            self.abstracts = {}
        if 'titles' in text_types:
            titles = {
                text_ref_id: filter_paragraphs(paragraphs, contains=contains)
                for text_ref_id, paragraphs in self.titles.items()
            }
            self.titles = {
                text_ref_id: text for text_ref_id, text in titles.items()
                if len(text) > 1
            }
        else:
            self.titles = {}
        self.processed = True

    def __str__(self):
        return (
            f"TextContent({len(self.fulltexts)} fulltexts,"
            f" {len(self.abstracts)} abstracts,"
            f" {len(self.titles)} titles)"
        )

    def __repr__(self):
        return str(self)


def get_paragraphs_for_text_ref_ids(
        text_ref_ids: Collection[int]
) -> TextContent:
    """Return TextContent object containing unprocessed content for input ids

    Each piece of content is stored as a list of paragraphs. This function may
    be useful for cases where the same data needs to be processed in multiple
    different ways. Implementing such processing is then left to the user. It
    is usually preferable to use the function `get_plaintexts_for_text_ref_ids`
    which will perform processing and filtering before returning the
    TextContent object.

    The local database contains the best piece of content for each text_ref_id
    within an indra_db instance from which the local db was constructed, at the
    time of construction. The prioritization of content is

        fulltext > abstract > title

    Fulltexts are futher prioritized by source, so that

        pmc_oa > manuscripts > cord19_pmc_xml > elsevier > cord19_pdf

    Abstracts are prioritized by source according to

        pubmed > cord19_abstract

    It is not common, but sometimes the same piece of content is available from
    multiple sources.

    Parameters
    ----------
    text_ref_ids : Collection of int
        A collection of text_ref_ids. These are ids into the text_ref table of
        the indra_db instance that has been dumped into the local db.

    Returns
    -------
    py:class:`indra_db_lite.api.TextContent`
        A TextContent object containing the best pieces of content in the local
        db corresponding to the given text_ref_ids.
    """
    text_ref_ids = tuple(text_ref_ids)
    query = f"""SELECT
                text_ref_id, text_type, content
            FROM
                best_content
            WHERE
                text_ref_id IN ({','.join(['?']*len(text_ref_ids))})
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            rows = (
                tuple(row) for row in cur.execute(
                    query, text_ref_ids
                ).fetchall()
            )
    return TextContent(rows)


def get_plaintexts_for_text_ref_ids(
        text_ref_ids: Collection[int],
        contains: Optional[Union[List[str], str]] = None,
) -> TextContent:
    """Returns processed plaintexts associated to input text_ref_ids.

    The local database contains the best piece of content for each text_ref_id
    within an indra_db instance from which the local db was constructed, at the
    time of construction. The prioritization of content is

        fulltext > abstract > title

    Fulltexts are futher prioritized by source, so that

        pmc_oa > manuscripts > cord19_pmc_xml > elsevier > cord19_pdf

    Abstracts are prioritized by source according to

        pubmed > cord19_abstract

    It is not common, but sometimes the same piece of content is available from
    multiple sources.

    Parameters
    ----------
    text_ref_ids : Collection of int
        A collection of text_ref_ids. These are ids into the text_ref table of
        the indra_db instance that has been dumped into the local db.

    contains : Optional[str of Collection of str]
        If a single string, filter to only paragraphs containing this
        token, (or n-gram in the case where the string contains multiple
        tokens separated by space characters). If a list of strings is
        passed, filter to only paragraphs that contain one or more of these
        tokens or n-grams.

    text_type : Optional[Collection of str]
        A Collection containing one or more of the strings "fulltext",
        "abstract", or "title". If None is passed, then all text_types will
        be included. Output TextContent object will only contain the included
        text types.

    Returns
    -------
    py:class:`indra_db_lite.api.TextContent`
        A TextContent object containing the best pieces of content in the local
        db corresponding to the given text_ref_ids.
    """
    content = get_paragraphs_for_text_ref_ids(text_ref_ids)
    content.to_plaintexts(contains=contains)
    return content


def get_text_ref_ids_for_pmids(
        pmids: Collection[int]
) -> Dict[int, int]:
    """Return dictionary mapping input pmids to corresponding text_ref_ids

    text_ref_id stands for the primary key into indra_db's text_ref table.
    The local database indexes content by text_ref_id, but it is often
    necessary to search for content by pmid. This function maps pmids to
    text_ref_ids so that the associated content can be found in the local db.

    Parameters
    ----------
    pmids : Collection of int

    Returns
    -------
    dict[int, int]
        dict mapping pmids to text_ref_ids. Not every pmid has an associated
        entry in the local database. It's possible that a pmid is for an
        article that was not read into indra_db or that the local db is out
        of date and does not contain content for that article. Such pmids will
        not appear as keys in the output dictionary. It is up to the user to
        track them if needed.
    """
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
    """Returns dict mapping input text_ref_ids to the associated pmids.

    Parameters
    ----------
    text_ref_id : Collection of int

    Returns
    -------
    dict[int, int]
    """
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
    """Get text_ref_ids for articles with extraction for agent_text in indra_db

    text_ref_ids for articles with at least one INDRA statement extracted with
    raw agent text equal to the input.

    Parameters
    ----------
    agent_text : str
        Raw agent text for some biological entity

    Returns
    -------
    list of int
        List of text_ref_ids
    """
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


def get_entrez_pmids_for_hgnc(hgnc_id: Union[int, str]) -> List[int]:
    """Get pmids for articles annotated as mentioning input gene in Entrez.

    Parameters
    ----------
    hgnc_id : int or str
        Hugo Gene Nomenclature ID for a human gene, e.g. 6091 for INSR.
        Can be either an int or str.

    Returns
    -------
    list of int
        List of pmids for articles annotated as mentioning input gene in
        Entrez.
    """
    hgnc_id = str(hgnc_id)
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
    """Get pmids for articles annotated as mentioning input protein in Entrez.

    Parameters
    ----------
    uniprot_id : str
        Uniprot ID for a protein, e.g. P06213 for INSR, human insulin receptor

    Returns
    -------
    list of int
        List of pmids for articles annotated as mentioning input protein in
        Entrez.
    """
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
    """Get pmids for articles annotated as mentioning Entrez gene.

    Parameters
    ----------
    entrez_id : str
        Entrez gene ID,  e.g. 3643 for INSR

    Returns
    -------
    list of int
        List of pmids for articles annotated as mentioning input protein in
        Entrez.
    """
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


def get_taxon_id_for_uniprot(uniprot_id: int) -> int:
    query = """--
    SELECT
        taxon_id
    FROM
        entrez_pmids
    WHERE
        uniprot_id = ?;
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(query, (uniprot_id, )).fetchall()
    return res[0][0] if res else None


def mesh_id_to_mesh_num(mesh_id: str) -> Tuple[int, bool]:
    if mesh_id[0] not in ['C', 'D']:
        return None
    is_concept = 1 if mesh_id[0] == 'C' else 0
    return (int(mesh_id[1:]), is_concept)


def get_pmids_for_mesh_term(mesh_id: str, major_topic=True) -> List[int]:
    mesh_num_is_concept = mesh_id_to_mesh_num(mesh_id)
    if mesh_num_is_concept is None:
        return []
    mesh_num, is_concept = mesh_num_is_concept
    query = """--
    SELECT
        pmid_num
    FROM
        mesh_pmids
    WHERE
        mesh_num = ? AND is_concept = ? AND major_topic = ?
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(
                query, (mesh_num, is_concept, major_topic)
            ).fetchall()
    return [row[0] for row in res]


def get_mesh_terms_for_grounding(
        namespace: str, identifier: str
) -> List[Tuple[int, int]]:
    curie = f"{namespace}:{identifier}"
    query = """--
    SELECT
        mesh_num, is_concept
    FROM
        mesh_xrefs
    WHERE
        curie = ?
    """
    with closing(sqlite3.connect(INDRA_DB_LITE_LOCATION)) as conn:
        with closing(conn.cursor()) as cur:
            res = cur.execute(query, (curie, )).fetchall()
    return [tuple(row) for row in res]
