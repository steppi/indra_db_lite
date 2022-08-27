import argparse
import random
import sqlite3
import zlib

from contextlib import closing
from gensim.corpora import Dictionary
from typing import Optional

from opaque.nlp.featurize import BaselineTfidfVectorizer

from .best_content import ensure_best_content_table


class TextFuzzer:
    def __init__(self, seed=None, no_above=0.2, no_below=5):
        vectorizer = BaselineTfidfVectorizer()
        dictionary = Dictionary.load(vectorizer.path)
        dictionary.filter_extremes(no_above=no_above, no_below=no_below)
        self.vectorizer = vectorizer
        self.dictionary = dictionary
        self.rng = random.Random(seed)

    def __call__(self, text):
        tokens = self.vectorizer._preprocess(text)
        tokens = [
            token for token in tokens if token in self.dictionary.token2id
        ]
        self.rng.shuffle(tokens)
        return ' '.join(tokens)


def make_compressed_best_content_table(
        from_db_path: str, to_db_path: str, fuzz: Optional[bool] = False
):
    fuzzer = TextFuzzer(seed=1729)
    ensure_best_content_table(to_db_path)
    select_query = """--
    SELECT
        id, text_ref_id, text_content_id1, text_content_id2, text_type, content
    FROM
        best_content
    """
    insertion_query = """--
    INSERT OR IGNORE INTO
        best_content (id, text_ref_id,
                      text_content_id1, text_content_id2, text_type, content)
    VALUES (?, ?, ?, ?, ?, ?);
    """
    with closing(sqlite3.connect(from_db_path)) as conn1, closing(conn1.cursor()) as cur1:
        rows = cur1.execute(select_query)
        with closing(sqlite3.connect(to_db_path)) as conn2, closing(conn2.cursor()) as cur2:
            cur2.execute('PRAGMA journal_mode = WAL')
            cur2.execute('PRAGMA synchronous = NORMAL')
            for row in rows:
                content = row[-1]
                if fuzz:
                    content = fuzzer(content)
                content = zlib.compress(content.encode("utf-8"))
                new_row = list(row[:-1]) + [content]
                cur2.execute(insertion_query, new_row)
            cur2.execute('PRAGMA journal_mode = DELETE')
            conn2.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("from_db_path")
    parser.add_argument("to_db_path")
    parser.add_argument("--fuzz", action="store_true")
    args = parser.parse_args()
    make_compressed_best_content_table(
        args.from_db_path, args.to_db_path, fuzz=args.fuzz
    )
