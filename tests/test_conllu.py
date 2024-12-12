from teanga import read_yaml_str
from teanga.conllu import read_conllu_str
import json

CONLLU_1 = """
# text = The quick brown fox jumps over the lazy dog.
1   The     the    DET    DT   Definite=Def|PronType=Art   4   det     _   _
2   quick   quick  ADJ    JJ   Degree=Pos                  4   amod    _   _
3   brown   brown  ADJ    JJ   Degree=Pos                  4   amod    _   _
4   fox     fox    NOUN   NN   Number=Sing                 5   nsubj   _   _
5   jumps   jump   VERB   VBZ  Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin   0   root    _   _
6   over    over   ADP    IN   _                           9   case    _   _
7   the     the    DET    DT   Definite=Def|PronType=Art   9   det     _   _
8   lazy    lazy   ADJ    JJ   Degree=Pos                  9   amod    _   _
9   dog     dog    NOUN   NN   Number=Sing                 5   nmod    _   SpaceAfter=No
10  .       .      PUNCT  .    _                           5   punct   _   _

"""

TEANGA_1 = """_meta:
  text:
    type: characters
  tokens:
    base: text
    type: span
  lemma:
    type: seq
    base: tokens
    data: string
  upos:
    data: ["ADJ", "ADP", "ADV", "AUX", "CCONJ", "DET", "INTJ", "NOUN", "NUM", "PART", "PRON", "PROPN", "PUNCT", "SCONJ", "VERB", "X" ]
    base: tokens
    type: seq
  xpos:
    data: string
    base: tokens
    type: seq
  feats:
    data: string
    base: tokens
    type: seq
  dep:
    data: link
    link_types: ["acl", "advcl", "advmod", "amod", "appos", "aux", "case", "cc", "ccomp", "compound", "conj", "cop", "csubj", "dep", "det", "discourse", "dislocated", "expl", "fixed", "flat", "goeswith", "iobj", "list", "mark", "nmod", "nsubj", "nummod", "obj", "obl", "orphan", "parataxis", "punct", "reparandum", "root", "vocative", "xcomp"]
    base: tokens
    type: seq
  misc:
    type: seq
    base: tokens
    data: string
/KOa:
  text: The quick brown fox jumps over the lazy dog.
  tokens: [[0, 3], [4, 9], [10, 15], [16, 19], [20, 25], [26, 30], [31, 34], [35, 39], [40, 43], [43, 44]]
  lemma: ["the", "quick", "brown", "fox", "jump", "over", "the", "lazy", "dog", "."]
  upos: ["DET", "ADJ", "ADJ", "NOUN", "VERB", "ADP", "DET", "ADJ", "NOUN", "PUNCT"]
  xpos: ["DT", "JJ", "JJ", "NN", "VBZ", "IN", "DT", "JJ", "NN", "."]
  dep: [[4, "det"], [4, "amod"], [4, "amod"], [5, "nsubj"], [0, "root"], [9, "case"], [9, "det"], [9, "amod"], [5, "nmod"], [5, "punct"]]
  feats: ["Definite=Def|PronType=Art", "Degree=Pos", "Degree=Pos", "Number=Sing", "Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin", "", "Definite=Def|PronType=Art", "Degree=Pos", "Number=Sing", ""]
"""

CONLLU_2 = """# newdoc id = n01001
# sent_id = n01001011
# text = “While much of the digital transition is unprecedented in 
2	While	while	SCONJ	IN	_	9	mark	9:mark	_
3	much	much	ADJ	JJ	Degree=Pos	9	nsubj	9:nsubj	_
4	of	of	ADP	IN	_	7	case	7:case	_
5	the	the	DET	DT	Definite=Def|PronType=Art	7	det	7:det	_
6	digital	digital	ADJ	JJ	Degree=Pos	7	amod	7:amod	_
7	transition	transition	NOUN	NN	Number=Sing	3	nmod	3:nmod:of	_
8	is	be	AUX	VBZ	Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin	9	cop	9:cop	_
9	unprecedented	unprecedented	ADJ	JJ	Degree=Pos	20	advcl	20:advcl:while	_
10	in	in	ADP	IN	_	13	case	13:case	_
"""

TEANGA_2 = """_meta:
  text:
    type: characters
  tokens:
    base: text
    type: span
  lemma:
    type: seq
    base: tokens
    data: string
  upos:
    data: ["ADJ", "ADP", "ADV", "AUX", "CCONJ", "DET", "INTJ", "NOUN", "NUM", "PART", "PRON", "PROPN", "PUNCT", "SCONJ", "VERB", "X" ]
    base: tokens
    type: seq
  xpos:
    data: string
    base: tokens
    type: seq
  feats:
    data: string
    base: tokens
    type: seq
  dep:
    data: link
    link_types: ["acl", "advcl", "advmod", "amod", "appos", "aux", "case", "cc", "ccomp", "compound", "conj", "cop", "csubj", "dep", "det", "discourse", "dislocated", "expl", "fixed", "flat", "goeswith", "iobj", "list", "mark", "nmod", "nsubj", "nummod", "obj", "obl", "orphan", "parataxis", "punct", "reparandum", "root", "vocative", "xcomp"]
    base: tokens
    type: seq
  misc:
    type: seq
    base: tokens
    data: string
3qzG:
  text: “While much of the digital transition is unprecedented in
  tokens: [[1, 6], [7, 11], [12, 14], [15, 18], [19, 26], [27, 37], [38, 40], [41, 54], [55, 57]]
  lemma: ["while", "much", "of", "the", "digital", "transition", "be", "unprecedented", "in"]
  upos: ["SCONJ", "ADJ", "ADP", "DET", "ADJ", "NOUN", "AUX", "ADJ", "ADP"]
  xpos: ["IN", "JJ", "IN", "DT", "JJ", "NN", "VBZ", "JJ", "IN"]
  feats: ["", "Degree=Pos", "", "Definite=Def|PronType=Art", "Degree=Pos", "Number=Sing", "Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin", "Degree=Pos", ""]
  dep: [[9, "mark"], [9, "nsubj"], [7, "case"], [7, "det"], [7, "amod"], [3, "nmod"], [9, "cop"], [20, "advcl"], [13, "case"]]
  "_newdoc id": n01001
  _sent_id: n01001011"""


def test_conllu_1():
    teanga_corpus = read_yaml_str(TEANGA_1)
    conllu_corpus = read_conllu_str(CONLLU_1)
    print(json.dumps(conllu_corpus.to_json_str()))
    assert teanga_corpus == conllu_corpus


def test_conllu_2():
    teanga_corpus = read_yaml_str(TEANGA_2)
    conllu_corpus = read_conllu_str(CONLLU_2)
    assert teanga_corpus.doc_ids == conllu_corpus.doc_ids
    for doc in teanga_corpus.doc_ids:
        for layer in teanga_corpus.doc_by_id(doc).layers:
            print("layer=",layer)
            assert teanga_corpus.doc_by_id(doc)[layer] == conllu_corpus.doc_by_id(doc)[layer]
    assert teanga_corpus == conllu_corpus
