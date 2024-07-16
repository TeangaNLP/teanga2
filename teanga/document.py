# Description: This file contains the model class for the corpus.
from abc import ABC, abstractmethod
from typing import Generator
import numbers
from itertools import chain, pairwise
from deprecated import deprecated
from typing import Union, Tuple, Iterator
from .layer_desc import LayerDesc
import regex as re

class Document:
    """Document class for storing and processing text data."""
    def __init__(self, meta:dict[str,Union[LayerDesc,dict]], 
                 corpus=None, id=None, **kwargs):
        self._meta = meta
        self.layers = {}
        self.id = id
        self.corpus = None
        self.add_layers({key: value 
                         for key, value in kwargs.items() 
                         if not key.startswith("_")})
        self.corpus = corpus

    def copy(self):
        """Return a copy of the document."""
        return Document(self._meta, self.corpus, self.id, 
                        **{key: value for key, value in self.layers.items()})

    @deprecated(reason="Use __setitem__ instead, e.g., doc['text'] = \
'This is a document.'")
    def add_layer(self, name:str, value : Union[str,list,'Layer']) -> 'Layer':
        self[name] = value

    def __setitem__(self, name:str, value : Union[str,list,'Layer']) -> 'Layer':
        """Add or set a layer to the document.
        
        Parameters:
        -----------
        name: str
            Name of the layer.
        value: str
            Value of the layer, a single string or 
            a list of values that are suitable for the 
            Teanga layer type or a Layer object.

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
        if isinstance(value, Layer):
            self.layers[name] = value
            return value
        if self._meta[name].layer_type == "characters":
            self.layers[name] = CharacterLayer(name, self, str(value))
        elif (self._meta[name].base not in self.layers and
                self._meta[self._meta[name].base].default is None):
            raise Exception("Cannot add layer " + name + " because sublayer " +
            self._meta[name].base + " does not exist.")
        elif self._meta[name].layer_type == "seq":
            if not isinstance(value, list):
                raise Exception("Value of layer " + name + " must be a list.")
            if self._meta[name].base in self.layers:
                base_layer_len = len(self.layers[self._meta[name].base])
            elif self._meta[self._meta[name].base].default is not None:
                base_layer_len = len(self._meta[self._meta[name].base].default)
            else:
                raise Exception("Cannot add layer " + name + " because sublayer " +
                    self._meta[name].base + " does not exist.")
            if len(value) != base_layer_len:
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

    def __getattr__(self, name:str) -> 'Layer':
        """Return the layer with the given name."""
        if name in self.layers:
            return self.layers[name]
        else:
            raise AttributeError("No such layer: " + name)

    def __setattr__(self, name:str, value) -> None:
        """Set the value of a layer."""
        if name != "layers" and name != "_meta" and name != "corpus" and name != "id":
            self.__setitem__(name, value)
        else:
            super().__setattr__(name, value)

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
        to_add = set(layers.keys())

        for layer in self._meta:
            if layer not in layers and self._meta[layer].default is not None:
                added.add(layer)

        while len(to_add) > 0:
            for name in to_add.copy():
                data = layers[name]
                if self._meta[name].base is None or self._meta[name].base in added:
                    self[name] = data
                    added.add(name)
                    to_add.remove(name)
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

    def __iter__(self):
        """Return an iterator over the layers."""
        return iter(self.layers)

    def __contains__(self, name:str) -> bool:
        """Return whether a layer with the given name exists."""
        return name in self.layers

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
        >>> doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19]]
        >>> doc.pos = ["DT", "VBZ", "DT", "NN", "."]
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

    def view(self, *args, start:int=0, end:int=None, root_layer:str=None):
        """Return a view of the document. A view is a grouping of the basic
          text data according to the annotations in the document

        Parameters:
        -----------

        args: list
            The layers to view the text data by.
        start: int
            The start index of the view.
        end: int
            The end index of the view.
        root_layer: str
            The root layer of the view (this should not normally be specified).

        Returns:
        --------

        The text data grouped by the annotations in the document.

        Examples:
        ---------

        >>> from teanga import Corpus
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> corpus.add_layer_meta("words", layer_type="span", base="text")
        >>> corpus.add_layer_meta("sentences", layer_type="div", base="text")
        >>> doc = corpus.add_doc("This is a sentence. This is another sentence.")
        >>> doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19], [20,24], \
[25,27], [28,35], [36,44], [44,45]]
        >>> doc.sentences = [0, 20]
        >>> doc.view("words")
        ['This', 'is', 'a', 'sentence', '.', 'This', 'is', 'another', 'sentence', '.']
        >>> doc.view("sentences")
        ['This is a sentence. ', 'This is another sentence.']
        >>> doc.view("words", "sentences")
        [['This', 'is', 'a', 'sentence', '.'], ['This', 'is', 'another', \
'sentence', '.']]
          """
        if root_layer is None:
            for layer in args:
                rl = self.layers[layer].root_layer()
                if root_layer is not None and rl != root_layer:
                    raise Exception("view was called with layers that have " +
                    "different root layers")
                root_layer = rl
        if root_layer is None:
            for layer in self.layers:
                if self._meta[layer].base is None:
                    if root_layer is not None:
                        raise Exception("view was called without specifying any" +
                        "layers or root layer but there are multiple root " +
                        "layers in the document")
                    root_layer = layer
                    break
        if end is None:
            end = len(self.layers["text"])
        if len(args) == 0:
            return self.text_for_layer(root_layer)[start:end]
        else:
            indexes = self.layers[args[-1]].indexes(root_layer)
            indexes = [(s, e) for s,e in indexes
                       if s >= start and e <= end]
            return [self.view(*args[:-1], start=s, end=e, root_layer=root_layer)
                    for s, e in indexes]

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

def _key_match(data, text, key, match) -> bool:
    if key == "$text":
        return text == match
    elif key == "$text_ne":
        return text != match
    elif key == "$eq":
        return data == match
    elif key == "$ne":
        return data != match
    elif key == "$gt":
        return data > match
    elif key == "$lt":
        return data < match
    elif key == "$gte":
        return data >= match
    elif key == "$lte":
        return data <= match
    elif key == "$in":
        return data in match
    elif key == "$nin":
        return data not in match
    elif key == "$text_in":
        return text in match
    elif key == "$text_nin":
        return text not in match
    elif key == "$regex":
        return re.match(match, data)
    elif key == "$text_regex":
        return re.match(match, text)
    elif key in ["$exists", "$and", "$or", "$not"]:
        raise Exception("Operator " + key + " occurs in wrong context")
    else:
        raise Exception("Unknown key: " + key)

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

    def root_layer(self) -> str:
        """Return the name of the root layer of the layer."""
        if self._meta.base is None:
            return self._name
        else:
            return self._doc.layers[self._meta.base].root_layer()

    def matches(self, value: Union[str,list,dict]) -> Iterator[int]:
        """Return the indexes of the annotations that match the given value.

        Parameters:
        -----------
        value: Union[str,list,dict]
            The value to match as described in the `view` method of 
            the `Corpus` class.
        """
        if isinstance(value, str):
            if self._meta.data is None:
                return (i for i, x in enumerate(self.text) if x == value)
            else:
                return (i for i, x in enumerate(self.data) if x == value)
        elif isinstance(value, list):
            if self._meta.data is None:
                return (i for i, x in enumerate(self.text) if x in value)
            else:
                return (i for i, x in enumerate(self.data) if x in value)
        elif isinstance(value, dict):
            if any(k.startswith("$text") for k in value):
                return (i for i, (d, t) in enumerate(zip(self.data, self.text))
                        if all(_key_match(d, t, k, v) for k, v in value.items()))
            else:
                return (i for i, d in enumerate(self.data)
                        if all(_key_match(d, None, k, v) for k, v in value.items()))
        else:
            raise Exception("Bad value: " + repr(value))

    @abstractmethod
    def __len__(self):
        """Return the number of annotations in the layer."""
        pass

    def __getitem__(self, key):
        """Return the annotation with the given index."""
        return self.raw[key]

    @abstractmethod
    def transform(self, transform_func):# -> Self:
        """Transform the layer using a transformation function."""
        pass

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

    def transform(self, transform_func):# -> Self:
        return CharacterLayer(self._name, self._doc, transform_func(self._text))

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

    def transform(self, transform_func):# -> Self:
        return SeqLayer(self._name, self._doc, [transform_func(x) for x in self.seq])

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
            subindexes = list(self._doc.layers[self._meta.base].indexes(layer))
            return [(subindexes[s[0]], subindexes[s[1]]) for s in self._data]

    def __repr__(self):
        return "SpanLayer(" + repr(self._data) + ")"

    def transform(self, transform_func):# -> Self:
        return SpanLayer(self._name, self._doc, [transform_func(x) for x in self._data])

def _1st_idx(d):
    if isinstance(d, numbers.Integral):
        return d
    else:
        return d[0]
 
class DivLayer(StandoffLayer):
    """A layer where the sublayer is divided into non-overlapping parts.
    As such these layers have only a start index for each annotation, and that
    annotation spans until the next annotation"""

    def __init__(self, name:str, doc:Document, spans:list):
        super().__init__(name, doc)
        self._data = spans
        for span in self._data: 
            if (not isinstance(span, numbers.Integral) and
                not isinstance(span[0], numbers.Integral)):
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
        >>> doc["sentences"] = [0, 19]
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
        >>> doc["sentences"] = [0, 19]
        >>> doc["sentences"].indexes("sentences")
        [(0, 1), (1, 2)]
        >>> doc["sentences"].indexes("text")
        [(0, 19), (19, 44)]
        """
        if layer == self._name:
            return list(zip(range(len(self._data)), range(1, len(self._data) + 1)))
        elif layer == self._meta.base:
            return list(pairwise(chain((s for s in self._data), 
                                  [len(self._doc.layers[self._meta.base])])))
        else:
            subindexes = list(self._doc.layers[self._meta.base].indexes(layer))
            return list(pairwise(
                chain(
                    (subindexes[s][0] for s in self._data), 
                    [len(self._doc.layers[layer])])))

    def __repr__(self):
        return "DivLayer(" + repr(self._data) + ")"

    def transform(self, transform_func):# -> Self:
        return DivLayer(self._name, self._doc, [transform_func(x) for x in self._data])

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
            return [(_1st_idx(s), _1st_idx(s) + 1) for s in self._data]
        else:
            subindexes = list(self._doc.layers[self._meta.base].indexes(layer))
            return [subindexes[_1st_idx(s)] for s in self._data]

    def __repr__(self):
        return "ElementLayer(" + repr(self._data) + ")"

    def transform(self, transform_func):# -> Self:
        return ElementLayer(self._name, self._doc, 
                            [transform_func(x) for x in self._data])

