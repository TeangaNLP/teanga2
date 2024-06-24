import rdflib
import teanga.rdf as rdf
import teanga

def test_teanga_corpus_to_rdf_1():
    corpus = teanga.Corpus()
    corpus.add_layer_meta("text", layer_type="characters")
    corpus.add_doc(text="Hello")
    graph = rdflib.Graph()
    rdf.teanga_corpus_to_rdf(graph, corpus, "http://example.org/corpus")
    assert len(graph) == 4

def test_teanga_corpus_to_rdf_2():
    corpus = teanga.Corpus()
    corpus.add_layer_meta("text", layer_type="characters")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    doc = corpus.add_doc("Hello there! Goodbye!")
    doc.words = [(0, 5), (6, 12), (14, 22)]
    graph = rdflib.Graph()
    rdf.teanga_corpus_to_rdf(graph, corpus, "http://example.org/corpus")
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb"), 
             rdflib.URIRef("http://teanga.io/teanga#text"), 
             rdflib.Literal("Hello there! Goodbye!")) in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb"),
             rdflib.URIRef("http://teanga.io/teanga#words"),
             rdflib.URIRef("http://example.org/corpus#xQAb&layer=words&idx=0")) 
            in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=words&idx=0"),
             rdflib.URIRef("http://teanga.io/teanga#idx"),
             rdflib.Literal(0)) in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=words&idx=0"),
             rdflib.URIRef("http://teanga.io/teanga#ref"),
             rdflib.URIRef("http://example.org/corpus#xQAb&layer=text&char=0,5")) 
            in graph)
    
def test_teanga_corpus_to_rdf_3():
    corpus = teanga.Corpus()
    corpus.add_layer_meta("text", layer_type="characters")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data=["NN", "VB"])
    corpus.add_layer_meta("lemma", layer_type="element", base="words", data="string")
    corpus.add_layer_meta("dep", layer_type="seq", base="words", data="link", 
                          link_types=["subj", "obj"])
    corpus.add_layer_meta("cons", layer_type="seq", base="words", data="link")
    corpus.add_layer_meta("document", layer_type="div", base="text", default=[0])
    corpus.add_layer_meta("author", layer_type="seq", base="document", data="string")
    doc = corpus.add_doc("Llamas eat")
    doc.words = [(0, 6), (7, 10)]
    doc.pos = ["NN", "VB"]
    doc.lemma = [(0, "llama"), (1, "eat")]
    doc.dep = [(1, "subj"), (0, "obj")]
    doc.cons = [1, 0]
    doc.author = ["Alice"]
    graph = rdflib.Graph()
    rdf.teanga_corpus_to_rdf(graph, corpus, "http://example.org/corpus")

    assert((rdflib.URIRef("http://example.org/corpus#3I1a"),
            rdflib.URIRef("http://teanga.io/teanga#text"),
            rdflib.Literal("Llamas eat")) in graph)
    assert((rdflib.URIRef("http://example.org/corpus#3I1a&layer=cons&idx=0"),
            rdflib.URIRef("http://teanga.io/teanga#link"),
            rdflib.URIRef("http://example.org/corpus#3I1a&layer=words&idx=1")) in graph)
    assert((rdflib.URIRef("http://example.org/corpus#3I1a&layer=dep&idx=0"),
            rdflib.URIRef("http://example.org/corpus#subj"),
            rdflib.URIRef("http://example.org/corpus#3I1a&layer=words&idx=1")) in graph)


def test_teanga_corpus_to_nif():
    corpus = teanga.Corpus()
    corpus.add_layer_meta("text", layer_type="characters")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    doc = corpus.add_doc("Hello there! Goodbye!")
    doc.words = [(0, 5), (6, 12), (14, 22)]
    graph = rdflib.Graph()
    rdf.teanga_corpus_to_nif(graph, corpus, "http://example.org/corpus")
    print("NIF Conversion")
    for s, p, o in graph:
        print(s, p, o)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=text"), 
             rdflib.URIRef("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString"),
             rdflib.Literal("Hello there! Goodbye!")) in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=text"),
             rdflib.URIRef("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#referenceContext"),
             rdflib.URIRef("http://example.org/corpus#xQAb")) in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=words&idx=0"), 
             rdflib.URIRef("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex"),
             rdflib.Literal(0, datatype=rdflib.XSD.nonNegativeInteger)) in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=words&idx=0"), 
             rdflib.URIRef("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex"),
             rdflib.Literal(5, datatype=rdflib.XSD.nonNegativeInteger)) in graph)
    assert ((rdflib.URIRef("http://example.org/corpus#xQAb&layer=words&idx=0"),
             rdflib.URIRef("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#superString"),
             rdflib.URIRef("http://example.org/corpus#xQAb&layer=text")) 
            in graph)

def test_teanga_corpus_to_webanno():
    corpus = teanga.Corpus()
    corpus.add_layer_meta("text", layer_type="characters")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    doc = corpus.add_doc("Hello there! Goodbye!")
    doc.words = [(0, 5), (6, 12), (14, 22)]
    objs = rdf.teanga_corpus_to_webanno(corpus, "http://example.org/corpus")
    for obj in objs:
        print(obj)

    assert objs[0] == {'id': '#xQAb&layer=words&idx=0', 
                       'type': 'Annotation', 
                       'target': {
                           'source': 'http://example.org/corpus#xQAb&layer=text', 
                           'selector': {
                               'type': 'TextPositionSelector', 
                               'start': 0, 
                               'end': 5}}, 
                       'body': {
                           'value': {
                               '@id': 'http://teanga.io/teanga#words'}
                           }
                       }

 
