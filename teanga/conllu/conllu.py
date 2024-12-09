from conllu import parse_incr
from typing import TextIO
from io import StringIO
import teanga

def conllu_corpus(db : str = None) -> teanga.Corpus:
    """Create a new empty Teanga Corpus object with metadata fields as
    specified in the CoNLL-U format.
    """
    corpus = teanga.Corpus()
    corpus.add_layer_meta('text', 'characters')
    corpus.add_layer_meta('tokens', 'span', base='text')
    corpus.add_layer_meta('lemma', 'seq', base='tokens', data='string')
    corpus.add_layer_meta('upos', 'seq', base='tokens', data=["ADJ", "ADP", "ADV", "AUX", "CCONJ", "DET", "INTJ", "NOUN", "NUM", "PART", "PRON", "PROPN", "PUNCT", "SCONJ", "VERB", "X" ])
    corpus.add_layer_meta('xpos', 'seq', base='tokens', data='string')
    corpus.add_layer_meta('feats', 'seq', base='tokens', data='string')
    corpus.add_layer_meta('dep', 'seq', base='tokens', data='link', link_types=["acl", "advcl", "advmod", "amod", "appos", "aux", "case", "cc", "ccomp", "compound", "conj", "cop", "csubj", "dep", "det", "discourse", "dislocated", "expl", "fixed", "flat", "goeswith", "iobj", "list", "mark", "nmod", "nsubj", "nummod", "obj", "obl", "orphan", "parataxis", "punct", "reparandum", "root", "vocative", "xcomp" ])
    corpus.add_layer_meta('misc', 'seq', base='tokens', data='string')

    return corpus

def read_conllu_str(s : str, db: str=None) -> teanga.Corpus:
    """Read a CoNLL-U string and return a Teanga Corpus object.
    
    Args:

    s: str
        The CoNLL-U string to read.
    db: str
        The DB location to use of the Teanga corpus
    """
    corpus = conllu_corpus(db)

    read_conllu(StringIO(s), corpus)

    return corpus


def read_conllu_file(file : str, db: str=None) -> teanga.Corpus:
    """Read a CoNLL-U file and return a Teanga Corpus object.
    
    Args:

    file: str
        The CoNLL-U filename to read.
    db: str
        The DB location to use of the Teanga corpus
    """
    corpus = conllu_corpus(db)

    read_conllu(open(file), corpus)

    return corpus

def read_conllu(obj : TextIO, corpus : teanga.Corpus):
    """Read a CoNLL-U object and return a Teanga Corpus object.
    
    Args:
    obj: TextIO
        The CoNLL-U object to read.
    corpus: teanga.Corpus
        The Teanga corpus to populate
    """
    for sentence in parse_incr(obj):
        if "text" in sentence.metadata:
            text = sentence.metadata["text"]
        else:
            text = " ".join([token['form'] for token in sentence])
        doc = corpus.add_doc(text)
        for key, value in sentence.metadata.items():
            if key == "text":
                continue
            doc.metadata[key] = value
        doc.tokens = find_spans([token['form'] for token in sentence], text)
        doc.lemma = [token['lemma'] for token in sentence]
        if all(token['upos'] is not None for token in sentence):
            doc.upos = [token['upos'] for token in sentence]
        if all(token['xpos'] is not None for token in sentence):
            doc.xpos = [token['xpos'] for token in sentence]
        if any(token['feats'] is not None for token in sentence):
            doc.feats = [map_feats(token['feats']) for token in sentence]
        if (all(token['head'] is not None for token in sentence) and
            all(token['deprel'] is not None for token in sentence)):
            doc.dep = [[token['head'], token['deprel']] for token in sentence]
        if all(token['misc'] is not None for token in sentence):
            doc.misc = [token['misc'] for token in sentence]

    return corpus

def map_feats(d: dict):
    if d is None:
        return ""
    else:
        return "|".join(f"{k}={v}" for k, v in d.items())

def find_spans(tokens, text):
    i = 0
    tk_idx = 0
    spans = []
    while i < len(text) and tk_idx < len(text):
        if text[i:].startswith(tokens[tk_idx]):
            spans.append([i, i+len(tokens[tk_idx])])
            i += len(tokens[tk_idx])
            tk_idx += 1
        else:
            i += 1
    if tk_idx < len(tokens):
        raise ValueError("Tokenization mismatch")
    return spans
