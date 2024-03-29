# Description: This file contains the model class for the corpus.
from abc import ABC, abstractmethod
from typing import Generator
import numbers
from itertools import chain, pairwise
from deprecated import deprecated
from typing import Union, Tuple
from .layer_desc import LayerDesc

class Document:
    """Document class for storing and processing text data."""
    def __init__(self, meta:dict[str,Union[LayerDesc,dict]], 
                 corpus=None, id=None, **kwargs):
        self._meta = meta
        self.layers = {}
        self.corpus = corpus
        self.id = id
        self.add_layers({key: value 
                         for key, value in kwargs.items() 
                         if not key.startswith("_")})

    @deprecated(reason="Use __setitem__ instead, e.g., doc['text'] = \
'This is a document.'")
    def add_layer(self, name:str, value) -> 'Layer':
        self[name] = value

    def __setitem__(self, name:str, value) -> 'Layer':
        """Add or set a layer to the document.
        
        Parameters:
        -----------
        name: str
            Name of the layer.
        value: str
            Value of the layer, a list of values that are suitable for the 
            Teanga layer type.

        Examples:
        ---------
        >>> from teanga import Corpus
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> corpus.add_layer_meta("words", layer_type="span", base="text")
        >>> corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
        >>> doc = corpus.add_doc("This is a document.")
        >>> doc["words"] = [(0,4), (5,7), (8,9), (10,18), (18,19)]
        >>> doc["pos"] = ["DT", "VBZ", "DT", "NN", "."]
        >>> doc
        Document('Kjco', {'text': CharacterLayer('This is a document.'), \
'words': SpanLayer([(0, 4), (5, 7), (8, 9), (10, 18), (18, 19)]), \
'pos': SeqLayer(['DT', 'VBZ', 'DT', 'NN', '.'])})
        >>> corpus.doc_by_id("Kjco")
        Document('Kjco', {'text': CharacterLayer('This is a document.'), \
'words': SpanLayer([(0, 4), (5, 7), (8, 9), (10, 18), (18, 19)]), \
'pos': SeqLayer(['DT', 'VBZ', 'DT', 'NN', '.'])})
        """
        if name not in self._meta:
            raise Exception("Layer with name " + name + " does not exist.")
        if value is None and self._meta[name].default is not None:
            value = self._meta[name].default
        if self._meta[name].layer_type == "characters":
            self.layers[name] = CharacterLayer(name, self, str(value))
        elif self._meta[name].base not in self.layers:
            raise Exception("Cannot add layer " + name + " because sublayer " +
            self._meta[name].base + " does not exist.")
        elif self._meta[name].layer_type == "seq":
            if not isinstance(value, list):
                raise Exception("Value of layer " + name + " must be a list.")
            if len(value) != len(self.layers[self._meta[name].base]):
                raise Exception("Value of layer " + name + " must have the " +
                "same length as layer " + self._meta[name].base + ".")
            self.layers[name] = SeqLayer(name, self, value)
        elif self._meta[name].layer_type == "span":
            if not isinstance(value, list):
                raise Exception("Value of layer " + name + " must be a list.")
            self.layers[name] = SpanLayer(name, self, value)
        elif self._meta[name].layer_type == "div":
            if not isinstance(value, list):
                raise Exception("Value of layer " + name + " must be a list.")
            self.layers[name] = DivLayer(name, self, value)
        elif self._meta[name].layer_type == "element":
            if not isinstance(value, list):
                raise Exception("Value of layer " + name + " must be a list.")
            self.layers[name] = ElementLayer(name, self, value)
        else:
            raise Exception("Unknown layer type " + self._meta[name].layer_type + 
            " for layer " + name + ".")
        if self.corpus and self.id:
            data_fields = {name: layer.raw
                           for (name,layer) in self.layers.items()}
            self.corpus.update_doc(self.id, data_fields)
                     
        return self.layers[name]

    def add_layers(self, layers:dict):
        """Add multiple layers in one go.

        Parameters:
        -----------

        layers: dict
            A dictionary of layer names and values.

        Examples:
        ---------

        >>> from teanga import Corpus
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> corpus.add_layer_meta("words", layer_type="span", base="text")
        >>> corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
        >>> doc = corpus.add_doc("This is a document.")
        >>> doc.add_layers({"words": [(0,4), (5,7), (8,9), (10,18), (18,19)], \
"pos": ["DT", "VBZ", "DT", "NN", "."]})
        """
        added = set(self.layers.keys())
        n = len(added)

        for layer in self._meta:
            if layer not in layers and self._meta[layer].default is not None:
                layers[layer] = self._meta[layer].default

        while len(added) < len(layers) + n:
            for name, data in layers.items():
                if self._meta[name].base is None or self._meta[name].base in added:
                    self.add_layer(name, data)
                    added.add(name)
                elif (self._meta[name].base is not None 
                      and self._meta[name].base not in layers 
                      and self._meta[name].base not in added):
                    raise Exception("Cannot add layer " + name + " because sublayer " +
                    self._meta[name].base + " does not exist.")

    def __getitem__(self, name:str):
        """Return the value of a layer.

        Parameters:
        -----------

        name: str
            The name of the layer.
        """
        if name not in self._meta:
            raise Exception("Layer with name " + name + " does not exist.")
        return self.layers[name]

    @deprecated(reason="Access layers using __getitem__ instead, e.g., doc['text']")
    def get_layer(self, name:str):
        return self[name]

    @property
    def meta(self):
        return self._meta

    def text_for_layer(self, layer_name:str) -> Generator[None,None,str]:
        """Return the text for a layer.

        Parameters:
        -----------

        layer_name: str
            The name of the layer.
        
        Returns:
        --------
        A generator that yields the text for the layer.

        Examples:
        ---------
        >>> from teanga import Corpus
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> corpus.add_layer_meta("words", layer_type="span", base="text")
        >>> corpus.add_layer_meta("pos", layer_type="seq", base="words")
        >>> doc = corpus.add_doc("This is a document.")
        >>> layer = doc.add_layer("words", [[0,4], [5,7], [8,9], [10,18], [18,19]])
        >>> layer = doc.add_layer("pos", ["DT", "VBZ", "DT", "NN", "."])
        >>> list(doc.text_for_layer("text"))
        ['T', 'h', 'i', 's', ' ', 'i', 's', ' ', 'a', ' ', 'd', 'o', \
'c', 'u', 'm', 'e', 'n', 't', '.']
        >>> list(doc.text_for_layer("words"))
        ['This', 'is', 'a', 'document', '.']
        >>> list(doc.text_for_layer("pos"))
        ['This', 'is', 'a', 'document', '.']
        """
        if layer_name not in self._meta:
            raise Exception("Layer with name " + layer_name + " does not exist.")
        if self._meta[layer_name].layer_type == "characters":
            return self.layers[layer_name].text[0]
        else:
            text_layer = layer_name
            while self._meta[text_layer].layer_type != "characters":
                text_layer = self._meta[text_layer].base
            indexes = self.layers[layer_name].indexes(text_layer)
            return (self.layers[text_layer].text[0][start:end]
                    for start, end in indexes)
    
    def to_json(self) -> str:
        """Return the JSON representation of the document."""
        return {layer_id: self.layers[layer_id].raw
                for layer_id in self.layers.keys()}

    @staticmethod
    def from_json(json:dict, meta:dict, corpus=None, id=None) -> 'Document':
        """Return a document from its JSON representation."""
        doc = Document(meta, corpus, id)
        doc.add_layers(json)
        return doc

    def __repr__(self):
        return "Document(" + repr(self.id) + ", " + repr(self.layers) + ")"

class Layer(ABC):
    """A layer of annotation"""
    
    def __init__(self, name:str, doc:Document):
        self._name = name
        self._meta = doc.meta[name]
        self._doc = doc

    @abstractmethod
    def data(self) -> list[Union[str,int,Tuple[int,str]]]:
        """Return the data values of the layer."""
        pass

    @abstractmethod
    def raw(self) -> list:
        """Return the raw data values of the layer."""
        pass

    @abstractmethod
    def text(self) -> list[str]:
        """Return the underlying text grouped by the annotations of this layer."""
        pass

    def text_data(self) -> Generator[None,None,
                                     tuple[str,Union[str,int,Tuple[int,str]]]]:
        """Return a list of pairs of the underlying text grouped by the 
        annotations of this layer and the data values of the layer."""
        return zip(self.text, self.data)

    @abstractmethod
    def indexes(self, layer:str) -> list[Tuple[int,int]]:
        """Return the indexes of the annotations of this layer."""
        pass

    def indexes_data(self, layer:str) -> Generator[None,None,
            tuple[Tuple[int,int],Union[str,int,Tuple[int,str]]]]:
        """Return a list of pairs of the data values of the layer and 
        the indexes of the annotations of this layer.

        Examples:
        ---------
        >>> from .layer_desc import LayerDesc
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "words": LayerDesc(layer_type="seq", base="text")})
        >>> doc["text"] = "This"
        >>> list(doc["text"].indexes_data("text"))
        [((0, 1), None), ((1, 2), None), ((2, 3), None), ((3, 4), None)]
        >>> doc["words"] = ["A", "B", "C", "D"]
        >>> list(doc["words"].indexes_data("words"))
        [((0, 1), 'A'), ((1, 2), 'B'), ((2, 3), 'C'), ((3, 4), 'D')]
        """
        return zip(self.indexes(layer), self.data)

    @abstractmethod
    def __len__(self):
        """Return the number of annotations in the layer."""
        pass

    def __getitem__(self, key):
        """Return the annotation with the given index."""
        return self.raw[key]

class CharacterLayer(Layer):
    """A layer of characters"""
    
    def __init__(self, name:str, doc: Document, text:str):
        super().__init__(name, doc)
        self._text = text

    @property
    def data(self):
        """
        Return the data values of the layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters")})
        >>> doc["text"] = "This"
        >>> doc["text"].data
        [None, None, None, None]
        """
        return [None] * len(self._text)

    @property
    def raw(self):
        return self._text

    @property
    def text(self):
        """
        Return the underlying text grouped by the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters")})
        >>> doc["text"] = "This is a document."
        >>> doc["text"].text
        ['This is a document.']
        """
        return [self._text]

    def indexes(self, layer:str):
        """
        Return the indexes of the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters")})
        >>> doc["text"] = "This"
        >>> doc["text"].indexes("text")
        [(0, 1), (1, 2), (2, 3), (3, 4)]
        """
        if layer != self._name:
            raise Exception("Indexing on layer that is not a sublayer.")
        return list(zip(range(len(self._text)), range(1, len(self._text) + 1)))

    def __repr__(self):
        return "CharacterLayer(" + repr(self._text) + ")"

    def __len__(self):
        return len(self._text)

class SeqLayer(Layer):
    """A layer that is in one-to-one correspondence with its sublayer.
    Typical examples are POS tags, lemmas, etc."""
    def __init__(self, name:str, doc:Document, seq:list):
        super().__init__(name, doc)
        self.seq = seq

    @property
    def data(self):
        """
        Return the data values of the layer.

        Examples:
        ---------
        >>> d = Document({"text": LayerDesc(layer_type="characters"),
        ... "is_num": LayerDesc(layer_type="seq", base="text")})
        >>> d["text"] = "A0B"
        >>> d["is_num"] = [0, 1, 0]
        >>> d["is_num"].data
        [0, 1, 0]
        """
        return self.seq

    @property
    def raw(self):
        return self.seq

    @property
    def text(self):
        """
        Return the underlying text grouped by the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "is_num": LayerDesc(layer_type="seq", base="text")},
        ... text="A0B")
        >>> doc["is_num"] = [0,1,0]
        >>> doc["is_num"].text
        ['A', '0', 'B']
        """
        return list(self._doc.text_for_layer(self._name))

    def indexes(self, layer:str):
        """
        Return the indexes of the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "is_num": LayerDesc(layer_type="seq", base="text")},
        ... text="AOB")
        >>> doc["is_num"] = [0,1,0]
        >>> doc["is_num"].indexes("text")
        [(0, 1), (1, 2), (2, 3)]
        """
        if layer == self._name:
            return [(i, i+1) for i in range(len(self.seq))]
        else:
            return self._doc.layers[self._meta.base].indexes(layer)

    def __repr__(self):
        return "SeqLayer(" + repr(self.seq) + ")"

    def __len__(self):
        return len(self.seq)

class StandoffLayer(Layer):
    """Common superclass of span, div and element layers. Cannot be used
    directly"""
    @property
    def raw(self):
        return self._data

    @property
    def text(self):
        """
        Return the underlying text grouped by the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "words": LayerDesc(layer_type="span", base="text", data="string")}, 
        ... text="This is an example.")
        >>> doc["words"] = [[0,4,"A"], [5,7,"B"], [8,10,"C"], 
        ... [11,18,"D"]]
        >>> doc["words"].text
        ['This', 'is', 'an', 'example']
        """
        return list(self._doc.text_for_layer(self._name))

    def __len__(self):
        return len(self._data)

class SpanLayer(StandoffLayer):
    """A layer that defines spans of the sublayer which are annotated.
    Typical examples are tokens, named entities, chunks, etc."""
    def __init__(self, name:str, doc: Document, spans:list):
        super().__init__(name, doc)
        self._data = spans
        for span in self._data: 
            if not isinstance(span[0], numbers.Integral):
                raise Exception("Bad span data: " + repr(span))
            if not isinstance(span[1], numbers.Integral):
                raise Exception("Bad span data: " + repr(span))

    @property
    def data(self):
        """
        Return the data values of the layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "words": LayerDesc(layer_type="span", base="text", data="string")},
        ... text="This is an example.")
        >>> doc["words"] = [[0,4,"A"], [5,7,"B"], [8,10,"C"], 
        ... [11,18,"D"]]
        >>> doc["words"].data
        ['A', 'B', 'C', 'D']
        """
        if self._meta.data is None:
            return [None] * len(self._data)
        elif self._meta.data == "link" and self._meta.link_types:
            return [(s[2], s[3]) for s in self._data]
        else:
            return [s[2] for s in self._data]


    def indexes(self, layer:str):
        """
        Return the indexes of the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "words": LayerDesc(layer_type="span", base="text", data="string")},
        ... text="This is an example.")
        >>> doc["words"] = [[0,4,"A"], [5,7,"B"], [8,10,"C"], 
        ... [11,18,"D"]]
        >>> doc["words"].indexes("words")
        [(0, 1), (1, 2), (2, 3), (3, 4)]
        >>> doc["words"].indexes("text")
        [(0, 4), (5, 7), (8, 10), (11, 18)]
        """
        if layer == self._name:
            return list(zip(range(len(self._data)), range(1, len(self._data) + 1)))
        elif layer == self._meta.base:
            return [(s[0], s[1]) for s in self._data]
        else:
            subindexes = list(self._doc.layers[self._meta.base]. indexes(layer))
            return [(subindexes[s[0]], subindexes[s[1]]) for s in self._data]

    def __repr__(self):
        return "SpanLayer(" + repr(self._data) + ")"

class DivLayer(StandoffLayer):
    """A layer where the sublayer is divided into non-overlapping parts.
    As such these layers have only a start index for each annotation, and that
    annotation spans until the next annotation"""

    def __init__(self, name:str, doc:Document, spans:list):
        super().__init__(name, doc)
        self._data = spans
        for span in self._data: 
            if not isinstance(span[0], numbers.Integral):
                raise Exception("Bad span data: " + repr(span))

    @property
    def data(self):
        """
        Return the data values of the layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "sentences": LayerDesc(layer_type="div", base="text")},
        ... text="This is an example. This is another example.")
        >>> doc["sentences"] = [[0], [19]]
        >>> doc["sentences"].data
        [None, None]
        """
        if self._meta.data is None:
            return [None] * len(self._data)
        elif self._meta.data == "link" and self._meta.link_types:
            return [(s[1], s[2]) for s in self._data]
        else:
            return [s[1] for s in self._data]

    def indexes(self, layer:str):
        """
        Return the indexes of the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "sentences": LayerDesc(layer_type="div", base="text")},
        ... text="This is an example. This is another example.")
        >>> doc["sentences"] = [[0], [19]]
        >>> doc["sentences"].indexes("sentences")
        [(0, 1), (1, 2)]
        >>> doc["sentences"].indexes("text")
        [(0, 19), (19, 44)]
        """
        if layer == self._name:
            return list(zip(range(len(self._data)), range(1, len(self._data) + 1)))
        elif layer == self._meta.base:
            return list(pairwise(chain((s[0] for s in self._data), 
                                  [len(self._doc.layers[self._meta.base])])))
        else:
            subindexes = list(self._doc.layers[self._meta.base].indexes(layer))
            return list(pairwise(chain((subindexes[s[0]] for s in self._data), 
                                  [len(self._doc.layers[self._meta.base])])))

    def __repr__(self):
        return "DivLayer(" + repr(self._data) + ")"

class ElementLayer(StandoffLayer):
    """A layer where each annotation is an element of the sublayer. This allows
    for multiple annotations of a single element. Typical examples are
    metadata elements such a titles"""

    def __init__(self, name:str, doc: Document, spans:list):
        super().__init__(name, doc)
        self._data = spans
        for span in self._data: 
            if not isinstance(span[0], numbers.Integral):
                raise Exception("Bad span data: " + repr(span))

    @property
    def data(self):
        """
        Return the data values of the layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "alts": LayerDesc(layer_type="element", base="text", data="string" )},
        ... text="Tá sé seo mar shampla.")
        >>> doc["alts"] = [[1, 'á'], [4, 'é']]
        >>> doc["alts"].data
        ['á', 'é']
        """
        if self._meta.data is None:
            return [None] * len(self._data)
        elif self._meta.data == "link" and self._meta.link_types:
            return [(s[1], s[2]) for s in self._data]
        else:
            return [s[1] for s in self._data]


    def indexes(self, layer:str):
        """
        Return the indexes of the annotations of this layer.

        Examples:
        ---------
        >>> doc = Document({"text": LayerDesc(layer_type="characters"),
        ... "alts": LayerDesc(layer_type="element", base="text", data="string" )},
        ... text="Tá sé seo mar shampla.")
        >>> doc["alts"] = [[1, "́a"], [4, "́e"]]
        >>> doc["alts"].indexes("alts")
        [(0, 1), (1, 2)]
        >>> doc["alts"].indexes("text")
        [(1, 2), (4, 5)]
        """
        if layer == self._name:
            return list(zip(range(len(self._data)), range(1, len(self._data) + 1)))
        elif layer == self._meta.base:
            return [(s[0], s[0] + 1) for s in self._data]
        else:
            subindexes = list(self._doc.layers[self._meta.base].indexes(layer, 
                                                                        self._doc))
            return [(subindexes[s[0]], subindexes[s[0]] + 1) for s in self._data]

    def __repr__(self):
        return "ElementLayer(" + repr(self._data) + ")"

