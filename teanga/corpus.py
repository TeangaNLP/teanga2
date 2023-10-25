from .document import Document
from .utils import teanga_id_for_doc
import teangadb
import shutil
import os
from collections import namedtuple
import json

LayerDesc = namedtuple("LayerDesc",
                       ["layer_type", "on", "data", "values", "target", "default"],
                       defaults=[None, None, None, None, None, None])

class Corpus:
    """Corpus class for storing and processing text data.
        
        Examples:
        ---------

        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
    """
    def __init__(self, db=None, new=False):
        if db:
            if new and os.path.exists(db):
                shutil.rmtree(db)
            self.corpus = teangadb.Corpus(db)
            self.meta = self.corpus.meta
        else:
            self.corpus = None
            self.meta = {}
            self.docs = []
            self.order = []

    def add_layer_meta(self, name:str=None,
                  layer_type:str="characters", on:str=None, 
                  data=None, values:list[str]=None,
                  target:str=None, default=None):
        """Add a layer to the corpus.
        
        Parameters:
        -----------
        name: str
            Name of the layer.
        layer_type: str
            The type of the layer, can be "characters", "span", "seq", 
            "element" or "div".
        on: str
            The name of the layer on which the new layer is based.
        data: list
            The data of the layer, this can be the value "string", "link" or 
            a list of strings, for an enumeration of values
        values: list
            The types of the links, if the data is links.
        target: str
            The name of the target layer, if the data is links.
        default:
            A default value if none is given
    """
        if self.corpus:
            if not on:
                on = ""
            self.corpus.add_layer_meta(
                    name, layer_type, on, data, values, target, default)
        if name is None:
            raise Exception("Name of the layer is not specified.")
        if name in self.meta:
            raise Exception("Layer with name " + name + " already exists.")
        if layer_type not in ["characters", "span", "seq", "div", "element"]:
            raise Exception("Type of the layer is not valid.")
        if layer_type == "characters" and on is not None and on != "":
            raise Exception("Layer of type characters cannot be based on" +
            " another layer.")
        if layer_type == "characters":
            self.meta[name] = LayerDesc("characters")
            return
        if on is None:
            raise Exception("Layer of type " + layer_type + " must be based on " +
            "another layer.")
        self.meta[name] = LayerDesc(layer_type, on, data, values, target, default)

    def add_doc(self, *args, **kwargs) -> Document:
        """Add a document to the corpus.
        
        Parameters:
        -----------

        If the corpus has only a single layer, the document can be added as a
        string. If the corpus has multiple layers, the document must be added
        by specifying the names of the layers and the data for each layer as
        keyword arguments.

        Examples:
        ---------
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")

        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("en", layer_type="characters")
        >>> corpus.add_layer_meta("nl", layer_type="characters")
        >>> doc = corpus.add_doc(en="This is a document.", nl="Dit is een document.")

        >>> corpus = Corpus("tmp",new=True)
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")

        """
        char_layers = [name for (name, layer) in self.get_meta().items()
                       if layer.layer_type == "characters"]
        if len(char_layers) == 0:
            raise Exception("No character layer found. " +
            "Please add at least one character layer.")
        elif len(char_layers) == 1:
            if len(args) == 1:
                doc_id = teanga_id_for_doc(self.get_doc_ids(),
                        **{char_layers[0]: args[0]})
                doc = Document(self.meta, id=doc_id, **{char_layers[0]: args[0]})
                if self.corpus:
                    self.corpus.add_doc({ char_layers[0]: args[0] })
                    doc.corpus = self.corpus
                else:
                    self.docs.append((doc_id, doc))
                return doc
            elif len(kwargs) == 1 and list(kwargs.keys())[0] == char_layers[0]["name"]:
                doc_id = teanga_id_for_doc(self.get_doc_ids(),
                                           **kwargs)
                doc = Document(self.meta, id=doc_id, **kwargs)
                if self.corpus:
                    self.corpus.add_doc(**kwargs)
                    doc.corpus = self.corpus
                else:
                    self.docs.append((doc_id, doc))
                return doc
            else:
                raise Exception("Invalid arguments, please specify the text " +
                                "or use correct layer names.")
        else:
            if set(kwargs.keys()) == set(char_layers):
                doc_id = teanga_id_for_doc(self.get_doc_ids(),
                                           **kwargs)
                doc = Document(self.meta, id=doc_id, **kwargs)
                if self.corpus:
                    self.corpus.add_doc(kwargs)
                    doc.corpus = self.corpus
                else:
                    self.docs.append((doc_id, doc))
                return doc
            else:
                raise Exception("Invalid arguments, please specify the text " +
                                "or use correct layer names.")

    def get_layers(self, layer:str):
        """Return all the values of a specific layer in the corpus.

        Parameters:
        -----------
        layer: str
            The name of the layer.

        Examples:
        ---------

        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> list(corpus.get_layers("text"))
        [CharacterLayer('This is a document.')]

        >>> corpus = Corpus("tmp", new=True)
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> list(corpus.get_layers("text"))
        [CharacterLayer('This is a document.')]

        """
        if layer not in self.meta:
            raise Exception("Layer with name " + layer + " does not exist.")
        return (doc.get_layer(layer) for doc in self.get_docs())

    def get_doc_ids(self):
        """Return the document ids of the corpus.

        Examples:
        ---------
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> corpus.get_doc_ids()
        ['Kjco']
        >>> corpus = Corpus("tmp",new=True)
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> corpus.get_doc_ids()
        ['Kjco']
        """
        if self.corpus:
            return self.corpus.get_docs()
        else:
            return [doc[0] for doc in self.docs]

    def get_docs(self):
        """Get all the documents in the corpus

        Examples:
        ---------
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> list(corpus.get_docs())
        [Document('Kjco', {'text': CharacterLayer('This is a document.')})]

        >>> corpus = Corpus("tmp",new=True)
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> list(corpus.get_docs())
        [Document('Kjco', {'text': CharacterLayer('This is a document.')})]
        """
        if self.corpus:
            return (Document(self.meta, id=doc_id, **self.corpus.get_doc_by_id(doc_id))
                    for doc_id in self.corpus.get_docs())
        else:
            return (doc[1] for doc in self.docs)

    def get_doc_by_id(self, doc_id:str) -> Document:
        """
        Get a document by its id.

        Parameters:
        -----------

        doc_id: str
            The id of the document.

        Examples:
        ---------

        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> corpus.get_doc_by_id("Kjco")
        Document('Kjco', {'text': CharacterLayer('This is a document.')})

        >>> corpus = Corpus("tmp",new=True)
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
        >>> corpus.get_doc_by_id("Kjco")
        Document('Kjco', {'text': CharacterLayer('This is a document.')})
        """
        if self.corpus:
            return Document(self.meta, id=doc_id, **self.corpus.get_doc_by_id(doc_id))
        else:
            return next(doc for doc in self.docs if doc[0] == doc_id)[1]

    def get_meta(self):
        """Return the meta data of the corpus.

        Examples:
        ---------
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> corpus.get_meta()
        {'text': LayerDesc(layer_type='characters', on=None, data=None, values=None, \
target=None, default=None)}
        """
        if self.corpus:
            return self.corpus.meta
        else:
            return self.meta

def _layer_desc(type="characters", on=None, data=None, values=None, 
                target=None, default=None):
    return LayerDesc(type, on, data, values, target, default)


def _corpus_hook(dct : dict) -> Corpus:
    c = Corpus()
    if "_meta" not in dct:
        return dct
    c.meta = {key: _layer_desc(**value) for key, value in dct["_meta"].items()}
    if "_order" in dct:
        c.order = dct["_order"]
    if c.order:
        for doc_id in c.order:
            c.docs.append((doc_id, Document(c.meta, id=doc_id, **dct[doc_id])))
    else:
        for doc_id, value in dct.items():
            if not doc_id.startswith("_"):
                c.docs.append((doc_id, Document(c.meta, id=doc_id, **value)))

def read_json_str(json_str, db_file=None):
    """Read a corpus from a json string.

    Parameters:
    -----------

    json_str: str
        The json string.
    db_file: str
        The path to the database file, if the corpus should be stored in a
        database.

    Examples:
    ---------

    >>> corpus = read_json_str('{"_meta": {"text": {"type": \
"characters"}},"Kjco": {"text": "This is a document."}}', "tmp")
    >>> corpus = read_json_str('{"_meta": {"text": {"type": \
"characters"}},"Kjco": {"text": "This is a document."}}')
    """
    if db_file:
        teangadb.read_corpus_from_json_string(json_str, db_file)
    else:
        json.loads(json_str, object_hook=_corpus_hook)

def read_json(path_or_buf, db_file=None):
    """Read a corpus from a json file.

    Parameters:
    -----------

    path_or_buf: str
        The path to the json file or a buffer.
    db_file: str
        The path to the database file, if the corpus should be stored in a
        database.
    """
    if db_file:
        teangadb.read_corpus_from_json_file(path_or_buf, db_file)
    else:
        json.load(path_or_buf, object_hook=_corpus_hook)
