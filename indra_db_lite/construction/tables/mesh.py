import argparse
from contextlib import closing
import csv
import gzip
from hashlib import md5
import io
import logging
from typing import Iterable

import lxml.etree as etree
import os
import pandas as pd
import re
import requests
import sqlite3

import famplex
from bs4 import BeautifulSoup

from indra.databases.identifiers import ensure_prefix_if_needed
from indra.databases.identifiers import get_ns_from_identifiers
from indra.databases.mesh_client import mesh_to_db
from indra.statements.validate import validate_id

from indra_db_lite.construction import get_sqlite_tables
from indra_db_lite.construction import import_csv_into_sqlite
from indra_db_lite.api import mesh_id_to_mesh_num


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
    result.to_csv(outpath, sep=',', index=False)


def create_mesh_xrefs_csv(
        javert_mesh_xrefs_path: str,
        outpath: str,
) -> None:
    javert_mesh_df = pd.read_csv(javert_mesh_xrefs_path, sep=',')
    indra_mesh_mappings = {
        key: ':'.join(val) for key, val in mesh_to_db.items()
    }
    indra_mesh_df = pd.DataFrame(
        indra_mesh_mappings.items(), columns=['mesh_id', 'curie']
    )
    indra_mesh_df[
        ['mesh_num', 'is_concept']
    ] = indra_mesh_df.apply(
        lambda row: mesh_id_to_mesh_num(row.mesh_id),
        axis=1,
        result_type="expand",
    )
    indra_mesh_df = indra_mesh_df[
        ['mesh_num', 'is_concept', 'curie']
    ]
    famplex_mesh = [
        [mesh_id, f"FPLX:{famplex_id}"]
        for ns, mesh_id, famplex_id in famplex.load_equivalences()
        if ns == 'MESH'
    ]
    famplex_mesh_df = pd.DataFrame(
        famplex_mesh, columns=['mesh_id', 'curie']
    )
    famplex_mesh_df[
        ['mesh_num', 'is_concept']
    ] = famplex_mesh_df.apply(
        lambda row: mesh_id_to_mesh_num(row.mesh_id),
        axis=1,
        result_type="expand",
    )
    famplex_mesh_df = famplex_mesh_df[
        ['mesh_num', 'is_concept', 'curie']
    ]
    result = pd.concat([javert_mesh_df, indra_mesh_df, famplex_mesh_df])
    result.drop_duplicates(inplace=True)
    result.reset_index(inplace=True, drop=True)
    result.to_csv(outpath, sep=',', index=True, header=False)

def get_url_paths(url: str) -> Iterable[str]:
    """Get the paths to all XML files on the PubMed FTP server."""
    logger.info("Getting URL paths from %s" % url)

    # Get page
    response = requests.get(url)
    response.raise_for_status()

    # Make soup
    soup = BeautifulSoup(response.text, "html.parser")

    # Append trailing slash if not present
    url = url if url.endswith("/") else url + "/"

    # Loop over all links
    for link in soup.find_all("a"):
        href = link.get("href")
        # yield if href matches
        # 'pubmed<2 digit year>n<4 digit file index>.xml.gz'
        # but skip the md5 files
        if href and href.startswith("pubmed") and href.endswith(".xml.gz"):
            yield url + href

def download_medline_pubmed_data(outpath: str) -> None:
    if not os.path.exists(outpath):
        os.mkdir(outpath)
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    basefiles = [u for u in get_url_paths(base_url)]
    for url in basefiles:
        xml_file = url.split("/")[-1]
        response = requests.get(url)
        md5_response = requests.get(base_url + xml_file + '.md5')
        actual_checksum = md5(response.content).hexdigest()
        expected_checksum = re.search(
            r'[0-9a-z]+(?=\n)', md5_response.content.decode('utf-8')
        ).group()
        if actual_checksum != expected_checksum:
            logger.warning(f'Checksum does not match for {xml_file}')
            continue
        with open(os.path.join(outpath, xml_file), 'w') as f:
            f.write(gzip.decompress(response.content).decode('utf-8'))


def extract_info_from_medline_xml(xml_path: str):
    tree = etree.parse(xml_path)
    elements = tree.xpath('//MedlineCitation')
    result = []
    for element in elements:
        pmid_element = element.xpath('PMID')[0]
        pmid = int(pmid_element.text)
        mesh_heading_list = element.xpath('MeshHeadingList')
        if not mesh_heading_list:
            continue
        mesh_heading_list = mesh_heading_list[0]
        for mesh_element in mesh_heading_list.getchildren():
            descriptor = mesh_element.xpath('DescriptorName')[0]
            attributes = descriptor.attrib
            mesh_id = attributes['UI']
            major_topic = attributes['MajorTopicYN'] == 'Y'
            mesh_num, is_concept = mesh_id_to_mesh_num(mesh_id)
            major_topic = attributes['MajorTopicYN'] == 'Y'
            result.append([mesh_num, is_concept, major_topic, pmid])
    return result


def mesh_citations_to_csv(mesh_xml_dir: str, outpath: str) -> None:
    for xml_file in os.listdir(mesh_xml_dir):
        xml_path = os.path.join(mesh_xml_dir, xml_file)
        new_rows = extract_info_from_medline_xml(xml_path)
        with open(outpath, 'a', newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerows(new_rows)


def ensure_mesh_xrefs_table(sqlite_db_path: str) -> None:
    query = """--
    CREATE TABLE IF NOT EXISTS mesh_xrefs (
    id INTEGER PRIMARY KEY,
    mesh_num INTEGER,
    is_concept INTEGER,
    curie TEXT
    );
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def insert_into_mesh_xrefs_table(
        mesh_xrefs_csv_path, sqlite_db_path: str
) -> None:
    ensure_mesh_xrefs_table(sqlite_db_path)
    import_csv_into_sqlite(
        mesh_xrefs_csv_path, 'mesh_xrefs', sqlite_db_path
    )


def ensure_mesh_pmids_table(sqlite_db_path: str) -> None:
    query = """--
    CREATE TABLE IF NOT EXISTS mesh_pmids (
    mesh_num INTEGER,
    is_concept INTEGER,
    major_topic INTEGER,
    pmid_num INTEGER
    );
    """
    with closing(sqlite3.connect(sqlite_db_path)) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
        conn.commit()


def insert_into_mesh_pmids_table(
        mesh_csv_path: str, sqlite_db_path: str
) -> None:
    ensure_mesh_pmids_table(sqlite_db_path)
    import_csv_into_sqlite(mesh_csv_path, 'mesh_pmids', sqlite_db_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("outpath")
    args = parser.parse_args()
    outpath = args.outpath
    mesh_db_path = os.path.join(outpath, 'mesh.db')
    medline_xmls_path = os.path.join(outpath, 'medline')
    mesh_csv_path = os.path.join(outpath, 'mesh.csv')
    javert_xrefs_path = os.path.join(outpath, 'javert_xrefs.csv')
    mesh_xrefs_path = os.path.join(outpath, 'mesh_xrefs.csv')
    logging.basicConfig(
        filename=os.path.join(outpath, 'mesh.log'),
        filemode='a',
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M',
        level=logging.DEBUG,
        force=True,
    )
    logger = logging.getLogger(__name__)
    if not os.path.exists(medline_xmls_path):
        download_medline_pubmed_data(medline_xmls_path)
    if not os.path.exists(mesh_csv_path):
        logger.info('Loading mesh annotations into csv file.')
        mesh_citations_to_csv(medline_xmls_path, mesh_csv_path)
    if (
            not os.path.exists(mesh_db_path) or
            'mesh_pmids' not in get_sqlite_tables(mesh_db_path)
    ):
        logger.info('Inserting into mesh pmids table.')
        insert_into_mesh_pmids_table(mesh_csv_path, mesh_db_path)
    if not os.path.exists(javert_xrefs_path):
        logger.info('Downloading javert mesh xrefs')
        download_mesh_javert_xrefs(javert_xrefs_path)
    if not os.path.exists(mesh_xrefs_path):
        logger.info('Constructing mesh xrefs csv')
        create_mesh_xrefs_csv(javert_xrefs_path, mesh_xrefs_path)
    if 'mesh_xrefs' not in get_sqlite_tables(mesh_db_path):
        logger.info('Inserting into mesh xrefs table.')
        insert_into_mesh_xrefs_table(
            mesh_xrefs_path, mesh_db_path)
