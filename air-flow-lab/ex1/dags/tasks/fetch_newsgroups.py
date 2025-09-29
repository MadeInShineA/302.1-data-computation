# tasks/fetch_newsgroups.py
import os
import re
from typing import Iterable, Optional
from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s]")  # strip punctuation (keep letters/digits/_)

def _clean(txt: str, stopwords: Iterable[str]) -> str:
    txt = txt.lower()
    txt = _PUNCT.sub(" ", txt)
    tokens = [t for t in txt.split() if t not in stopwords]
    return _WS.sub(" ", " ".join(tokens)).strip()

def fetch_newsgroups_task(
    base_path: str = ".",
    subset: str = "train",
    categories: Optional[list[str]] = None,
    remove_stopwords: bool = True,
    outfile: str = "corpus.txt",
) -> str:
    """
    Downloads 20 Newsgroups and writes a cleaned, one-doc-per-line corpus to:
      {base_path}/output/corpus.txt
    Also writes simple metadata to:
      {base_path}/output/meta.tsv
    """
    out_dir = os.path.join(base_path, "output")
    os.makedirs(out_dir, exist_ok=True)

    ds = fetch_20newsgroups(subset=subset, categories=categories, remove=())
    stop = ENGLISH_STOP_WORDS if remove_stopwords else set()

    with open(os.path.join(out_dir, outfile), "w", encoding="utf-8") as f:
        for doc in ds.data:
            f.write(_clean(doc, stop) + "\n")

    with open(os.path.join(out_dir, "meta.tsv"), "w", encoding="utf-8") as f:
        f.write("idx\ttarget\ttarget_name\n")
        for i, y in enumerate(ds.target):
            f.write(f"{i}\t{y}\t{ds.target_names[y]}\n")

    return outfile

__all__ = ["fetch_newsgroups_task"]
