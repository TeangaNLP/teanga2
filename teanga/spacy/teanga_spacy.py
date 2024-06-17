try:
    import spacy
except ImportError:
    raise ImportError("SpaCY is required for the SpaCY service")
from teanga import Service

class SpaCy(Service):
    """A service that uses spaCy to tokenize and tag text.

    This service requires a SpaCY model name as a parameter. The model
    name is the name of a SpaCY model that has been installed on the
    system. For example, the model "en_core_web_sm" is a small English
    model that can be installed with the command:

    python -m spacy download en_core_web_sm

    The SpaCY model is loaded in the setup() method. The model is then
    applied to the text in the execute() method. The SpaCY model
    produces a number of annotations, including tokens, part-of-speech
    tags, lemmas, and dependency relations. These annotations are
    converted to the Teanga format and added to the document.

    Example:
    --------

    >>> from teanga import Document, Corpus
    >>> corpus = Corpus()
    >>> corpus.add_layer_meta("text")
    >>> service = SpaCy("en_core_web_sm")
    >>> service.setup()
    >>> doc = corpus.add_doc("This is a test.")
    >>> corpus.apply(service)
    """
    def __init__(self, model_name:str, excludes:list=[]):
        """Create a service for the SpaCY model name"""
        super().__init__()
        self.model_name = model_name
        self.exclude = excludes

    def setup(self):
        """Load the SpaCY model"""
        if not hasattr(self, "nlp") or not self.nlp:
            self.nlp = spacy.load(self.model_name)

    def requires(self):
        """Return the requirements for this service"""
        return {"text": { "type": "characters" }}

    def produces(self):
        """Return the output of this service"""
        if not hasattr(self, "nlp") or not self.nlp:
            raise Exception("SpaCY model not loaded. "
            + "Please call setup() on the service.")
        tag_labels = "string"
        dep_labels = "string"
        ner_labels = "string"
        for module, pipe in self.nlp.components:
            if module == "tagger":
                tag_labels = [t for t in pipe.labels]
            if module == "parser":
                dep_labels = [t for t in pipe.labels]
            if module == "ner":
                ner_labels = [t for t in pipe.labels]

        result = {
                "tokens": {"type": "span", "base": "text" },
                "pos": {"type": "seq", "base": "tokens", "data": 
                        ["ADJ","ADP","PUNCT","ADV","AUX","SYM","INTJ",
                         "CCONJ","X","NOUN","DET","PROPN","NUM","VERB",
                         "PART","PRON","SCONJ", "SPACE", "EOL"]},
                "tag": {"type": "seq", "base": "tokens", "data": tag_labels },
                "lemma": {"type": "seq", "base": "tokens", "data": "string" },
                "morph": {"type": "seq", "base": "tokens", "data": "string" },
                "dep": {"type": "seq", "base": "tokens",
                        "data": "links", "link_types": dep_labels },
                "entity": {"type": "span", "base": "tokens", "data": ner_labels },
                "sentences": {"type": "div", "base": "tokens" }
        }

        for e in self.exclude:
            if e in result:
                del result[e]
        return result

    def execute(self, doc):
        """Execute SpaCy on the document"""
        if not hasattr(self, "nlp") or not self.nlp:
            raise Exception("SpaCY model not loaded. "
            + "Please call setup() on the service.")
        result = self.nlp(doc.text.raw)
        doc.tokens = [(w.idx, w.idx + len(w)) for w in result]
        if "pos" not in self.exclude:
            doc.pos = [w.pos_ for w in result]
        if "tag" not in self.exclude:
            doc.tag = [w.tag_ for w in result]
        if "lemma" not in self.exclude:
            doc.lemma = [w.lemma_ for w in result]
        if "morph" not in self.exclude:
            doc.morph = [str(w.morph) for w in result]
        if "dep" not in self.exclude:
            doc.dep = [(w.head.i, w.dep_) for w in result]
        if "entity" not in self.exclude:
            doc.entity = [(e.start, e.end, e.label_) for e in result.ents]
        if "sentences" not in self.exclude:
            doc.sentences = [s.start for s in result.sents]


