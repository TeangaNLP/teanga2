from .document import Document
from .service import Service
from .utils import teanga_id_for_doc
from .layer_desc import LayerDesc, _layer_desc_from_kwargs, _from_layer_desc
from .groups import GroupedCorpus
from .transforms import TransformedCorpus
from .stream import CorpusStream, CorpusWriter

try:
    import teanga_pyo3.teanga as teangadb
    TEANGA_DB = True
except ImportError:
    TEANGA_DB = False
import shutil
import os
import json
import yaml
import gzip
import tempfile
from io import StringIO
from itertools import chain
from typing import Iterator, Union, Callable, Iterable  
from collections import Counter, defaultdict
from urllib.request import urlopen
import re

class Corpus:
    """Corpus class for storing and processing text data.

    Examples:
        >>> corpus = Corpus()
        >>> corpus.add_layer_meta("text")
        >>> doc = corpus.add_doc("This is a document.")
    """
    def __init__(self, db=None, new=False, db_corpus=None):
        if db_corpus:
            self.corpus = db_corpus
            self.meta = self.corpus.meta
        elif db:
            if not TEANGA_DB:
                teanga_db_fail()
            if new and os.path.exists(db):
                shutil.rmtree(db)
            self.corpus = teangadb.Corpus(db)
            self.meta = self.corpus.meta
        else:
            self.corpus = None
            self.meta = {}
            self._docs = {}


    def add_meta_from_service(self, service : Service):
        """Add the meta data of a service to the corpus. This is normally
        required to call apply to a service

        Args:
            service: The service to add.

        Examples:
            >>> corpus = Corpus()
            >>> class ExampleService:
            ...     def requires(self):
            ...         return {"text": {"type": "characters"}}
            ...     def produces(self):
            ...         return {"first_char": {"type": "characters"}}
            >>> corpus.add_meta_from_service(ExampleService())

        Returns:
            A number representing the arithmetic sum of `a` and `b`.
        """
        for name, layer in chain(service.requires().items(),
                                 service.produces().items()):
            if "type" not in layer:
                raise Exception("Layer type not specified." + str(layer))
            desc = _layer_desc_from_kwargs(layer)
            if name in self.meta and self.meta[name] != desc:
                raise Exception("Layer with name " + name +
                                " already exists with different meta.")
            elif name not in self.meta:
                self.add_layer_meta(name, **layer)


    def add_layer_meta(self, name:str=None,
                       layer_type:str="characters", base:str=None,
                       data=None, link_types:list[str]=None,
                       target:str=None, default=None,
                       meta:dict={}):
        """Add a layer to the corpus.

        Args:
            name: str
                Name of the layer.
            layer_type: str
                The type of the layer, can be "characters", "span", "seq",
                "element" or "div".
            base: str
                The name of the layer on which the new layer is based.
            data: list
                The data of the layer, this can be the value "string", "link" or
                a list of strings, for an enumeration of values
            link_types: list
                The types of the links, if the data is links.
            target: str
                The name of the target layer, if the data is links.
            default:
                A default value if none is given
            meta: dict
                Metadata properties of the layer.
        """
        if self.corpus:
            self.corpus.add_layer_meta(
                    name, layer_type, {}, base, data, link_types, target, default)
            return
        if name is None:
            raise Exception("Name of the layer is not specified.")
        if name in self.meta:
            raise Exception("Layer with name " + name + " already exists.")
        if layer_type not in ["characters", "span", "seq", "div", "element"]:
            raise Exception("Type of the layer is not valid.")
        if layer_type == "characters" and base is not None and base != "":
            raise Exception("Layer of type characters cannot be based on" +
            " another layer.")
        if layer_type == "characters":
            self.meta[name] = LayerDesc("characters")
            return
        if base is None:
            raise Exception("Layer of type " + layer_type + " must be based on " +
            "another layer.")
        self.meta[name] = LayerDesc(layer_type, base, data, link_types,
                                    target, default, meta)

    def add_doc(self, *args, **kwargs) -> Document:
        """Add a document to the corpus.

        Args:

            If the corpus has only a single layer, the document can be added as a
            string. If the corpus has multiple layers, the document must be added
            by specifying the names of the layers and the data for each layer as
            keyword arguments.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")

            >>> corpus = parallel_corpus(["en", "nl"])
            >>> doc = corpus.add_doc(en="This is a document.", nl="Dit is een document.")
        """
        if len(args) == 1 and isinstance(args[0], Document):
            doc = args[0]
            if self.corpus:
                self.corpus.add_doc({layer_id: doc[layer_id].raw
                                     for layer_id in doc.layers})
            else:
                self._docs[doc.id] = doc
            return doc
        char_layers = [name for (name, layer) in self.meta.items()
                       if layer.layer_type == "characters"]
        if len(char_layers) == 0:
            raise Exception("No character layer found. " +
            "Please add at least one character layer.")
        elif len(char_layers) == 1:
            if len(args) == 1:
                doc_id = teanga_id_for_doc(self.doc_ids,
                        **{char_layers[0]: args[0]})
                doc = Document(self.meta, id=doc_id, **{char_layers[0]: args[0]})
                if self.corpus:
                    self.corpus.add_doc({ char_layers[0]: args[0] })
                    doc.corpus = self.corpus
                else:
                    self._docs[doc_id] = doc
                return doc
            elif len(kwargs) == 1 and list(kwargs.keys())[0] == char_layers[0]:
                doc_id = teanga_id_for_doc(self.doc_ids,
                                           **kwargs)
                doc = Document(self.meta, id=doc_id, **kwargs)
                if self.corpus:
                    self.corpus.add_doc(**kwargs)
                    doc.corpus = self.corpus
                else:
                    self._docs[doc_id] = doc
                return doc
            else:
                raise Exception("Invalid arguments, please specify the text " +
                                "or use correct layer names.")
        else:
            if len(kwargs.keys()) == 0:
                raise Exception("More than one character layer found " +
                                f"{' '.join(char_layers)} " +
                                "Please specify the layer names to add")
            if set(kwargs.keys()).issubset(set(char_layers)):
                doc_id = teanga_id_for_doc(self.doc_ids,
                                           **kwargs)
                doc = Document(self.meta, id=doc_id, **kwargs)
                if self.corpus:
                    self.corpus.add_doc(kwargs)
                    doc.corpus = self.corpus
                else:
                    self._docs[doc_id] = doc
                return doc
            else:
                raise Exception("Invalid arguments, please specify the text " +
                                "or use correct layer names.")

    @property
    def doc_ids(self) -> Iterable[str]:
        """Return the document ids of the corpus.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> list(corpus.doc_ids)
            ['Kjco']

        """
        if self.corpus:
            return self.corpus.order
        else:
            return self._docs.keys()

    @property
    def docs(self) -> Iterator[Document]:
        """Get all the documents in the corpus

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> list(corpus.docs)
            [Document('Kjco', {'text': CharacterLayer('This is a document.')})]
        """
        if self.corpus:
            for doc_id in self.corpus.order:
                yield Document(self.meta, id=doc_id, corpus=self.corpus,
                                        **self.corpus.get_doc_by_id(doc_id))
        else:
            for doc in self._docs.items():
                yield doc[1]

    def doc_by_id(self, doc_id:str) -> Document:
        """
        Get a document by its id.

        Args:
            doc_id: str
                The id of the document.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus.doc_by_id("Kjco")
            Document('Kjco', {'text': CharacterLayer('This is a document.')})

            >>> if TEANGA_DB:
            ...   corpus = Corpus("tmp",new=True)
            ...   corpus.add_layer_meta("text")
            ...   doc = corpus.add_doc("This is a document.")
        """
        if self.corpus:
            return Document(self.meta, id=doc_id, corpus=self.corpus,
                            **self.corpus.get_doc_by_id(doc_id))
        else:
            if doc_id in self._docs:
                return self._docs[doc_id]
            else:
                raise Exception("Document with id " + doc_id + " not found.")

    def __getitem__(self, key:str) -> Document:
        """Get a document by its id.

        Args:
            key: Union[str, int, slice]
                The id of the document. Strings use document identifiers, while integers use the order of the documents in the corpus.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus["Kjco"]
            Document('Kjco', {'text': CharacterLayer('This is a document.')})
            >>> corpus[0]
            Document('Kjco', {'text': CharacterLayer('This is a document.')})
            >>> corpus[:1]
            [Document('Kjco', {'text': CharacterLayer('This is a document.')})]
        """
        if isinstance(key, int):
            if self.corpus:
                return self.doc_by_id[self.corpus.order[key]]
            else:
                return self.doc_by_id(list(self.doc_ids)[key])
        elif isinstance(key, slice):
            if self.corpus:
                return [self.doc_by_id(doc_id) for doc_id in self.corpus.order[key]]
            else:
                return [self.doc_by_id(doc_id) for doc_id in list(self.doc_ids)[key]]
        elif isinstance(key, str):
            return self.doc_by_id(key)
        else:
            raise Exception("Invalid key type.")

    @property
    def meta(self) -> dict[str, LayerDesc]:
        """Return the meta data of the corpus.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> corpus.meta
            {'text': LayerDesc(layer_type='characters', base=None, data=None, link_types=None, target=None, default=None, meta={})}
        """
        if self.corpus:
            return {
                    key: LayerDesc(layer_type=layer.layer_type, base=layer.base,
                                  data=layer.data, link_types=layer.link_types,
                                  target=layer.target, default=layer.default,
                                  meta=layer.meta)
                    for key, layer in self.corpus.meta.items() }
        else:
            return self._meta

    @meta.setter
    def meta(self, meta: dict[str, LayerDesc]):
        if self.corpus:
            self.corpus.meta = meta
        else:
            self._meta = meta

    def text_freq(self, layer:str,
                  condition : Union[str,
            Callable[[str], bool], list] = None) -> dict[str, int]:
        """Get the frequence of a text string in the corpus.

        Args:
            layer
                The layer to get the frequency of (e.g. "text")
            condition
                A filter to match to. If a single string is given, the frequency
                of this single word is returned. If a list of strings is given, the
                frequency of each string is returned. If a function is given, all
                strings are returned for which the function returns True.

        Returns:
            A dictionary with the frequency of each string.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> doc.tokens = [(0, 4), (5, 7), (8, 9), (10, 18)]
            >>> corpus.text_freq("tokens")
            Counter({'This': 1, 'is': 1, 'a': 1, 'document': 1})
            >>> corpus.text_freq("tokens", lambda x: "i" in x)
            Counter({'This': 1, 'is': 1})
        """
        if condition is None:
            return Counter(word
                for doc in self.docs
                for word in doc[layer].text)
        elif isinstance(condition, str):
            return Counter(word
                for doc in self.docs
                           for word in doc[layer].text
                           if word == condition)
        elif callable(condition):
            return Counter(word
                for doc in self.docs
                for word in doc[layer].text
                           if condition(word))
        else:
            return Counter(word
                for doc in self.docs
                for word in doc[layer].text
                           if word in condition)

    def val_freq(self, layer:str,
                 condition = None) -> Counter:
        """Get the frequency of a value in a layer.

        Args:
            layer
                The layer to get the frequency of (e.g. "pos")
            condition
                The value to get the frequency of. If a single value is given, the
                frequency of this single value is returned. If a list of values is
                given, the frequency of each value is returned. If a function is
                given, all values are returned for which the function returns True.

        Returns:
            A dictionary with the frequency of each value.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> corpus.add_layer_meta("words", layer_type="span", base="text")
            >>> corpus.add_layer_meta("pos", layer_type="seq", base="words",
            ...                        data=["NOUN", "VERB", "ADJ"])
            >>> doc = corpus.add_doc("Colorless green ideas sleep furiously.")
            >>> doc.words = [(0, 9), (10, 15), (16, 21), (22, 28), (29, 37)]
            >>> doc.pos = ["ADJ", "ADJ", "NOUN", "VERB", "ADV"]
            >>> corpus.val_freq("pos")
            Counter({'ADJ': 2, 'NOUN': 1, 'VERB': 1, 'ADV': 1})
            >>> corpus.val_freq("pos", ["NOUN", "VERB"])
            Counter({'NOUN': 1, 'VERB': 1})
            >>> corpus.val_freq("pos", lambda x: x[0] == "A")
            Counter({'ADJ': 2, 'ADV': 1})
        """
        if condition is None:
            return Counter(val
                for doc in self.docs
                for val in doc[layer].data)
        elif isinstance(condition, str):
            return Counter(val
                for doc in self.docs
                for val in doc[layer].data
                           if val == condition)
        elif callable(condition):
            return Counter(val
                for doc in self.docs
                for val in doc[layer].data
                           if condition(val))
        else:
            return Counter(val
                for doc in self.docs
                for val in doc[layer].data
                           if val in condition)

    def by_doc(self) -> GroupedCorpus:
        """Group the corpus by document to enable analysis such as frequency
        analysis on a per document basis.
        """
        return GroupedCorpus(self,
                             {doc_id: [doc_id] for doc_id in self.doc_ids})


    def by(self, layer:str) -> GroupedCorpus:
        """Group the corpus according to which documents have specific values
        of a layer. Mostly used for metadata layers (e.g., "author", "genre")
        """
        grouping = defaultdict(list)
        for doc in self.docs:
            if layer in doc:
                for value in doc[layer].data:
                    grouping[value].append(doc.id)
        return GroupedCorpus(self, grouping)

    def search(self, query=None, **kwargs) -> Iterator[str]:
        """Search for documents in the corpus.

        Args:
            kwargs, query:
                The search criteria. The keys are the layer names and the values
                can be either a string, a list of strings or a dictionary with values
                describing the search criteria.

                If the value is a string the search is interpreted as an exact
                match. If the layer has no data this is applied to the text
                otherwise it is applied to the data.

                If the value is a list of strings, the search is interpreted as a
                search for any of the strings in the list.

                For dictionaries, the following keys are supported:
                `$text`: The value on the base character layer equal this value.
                `$text_ne`: The value on the base character layer must not equal this value.
                `$eq`: The value must be equal to this value.
                `$ne`: The value must not be equal to this value.
                `$gt`: The value must be greater than this value.
                `$lt`: The value must be less than this value.
                `$gte`: The value must be greater than or equal to this value.
                `$lte`: The value must be less than or equal to this value.
                `$in`: The value must be in this list.
                `$nin`: The value must not be in this list.
                `$text_in`: The value on the base character layer must be in this list.
                `$text_nin`: The value on the base character layer must not be in this list.
                `$regex`: The value must match this regular expression.
                `$text_regex`: The value on the base character layer must match
                    this regular expression.
                `$and`: All the conditions in this list must be true.
                `$or`: At least one of the conditions in this list must be true.
                `$not`: The condition in this list must not be true.
                `$exists`: A particular layer must exist.

        Returns:
            An iterator over the document ids that match the search criteria.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> corpus.add_layer_meta("words", layer_type="span", base="text")
            >>> corpus.add_layer_meta("pos", layer_type="seq", base="words",
            ...                        data=["NOUN", "VERB", "ADJ"])
            >>> corpus.add_layer_meta("lemma", layer_type="seq", base="words",
            ...                        data="string")
            >>> doc = corpus.add_doc("Colorless green ideas sleep furiously.")
            >>> doc.words = [(0, 9), (10, 15), (16, 21), (22, 27), (28, 37)]
            >>> doc.pos = ["ADJ", "ADJ", "NOUN", "VERB", "ADV"]
            >>> doc.lemma = ["colorless", "green", "idea", "sleep", "furiously"]
            >>> list(corpus.search(pos="NOUN"))
            ['9wpe']
            >>> list(corpus.search(pos=["NOUN", "VERB"]))
            ['9wpe']
            >>> list(corpus.search(pos={"$in": ["NOUN", "VERB"]}))
            ['9wpe']
            >>> list(corpus.search(pos={"$regex": "N.*"}))
            ['9wpe']
            >>> list(corpus.search(pos="VERB", lemma="sleep"))
            ['9wpe']
            >>> list(corpus.search(pos="VERB", words="idea"))
            []
            >>> list(corpus.search(pos="VERB", words="ideas"))
            ['9wpe']
            >>> list(corpus.search({"pos": "VERB", "lemma": "sleep"}))
            ['9wpe']
            >>> list(corpus.search({"$and": {"pos": "VERB", "lemma": "sleep"}}))
            ['9wpe']
        """
        if kwargs and query:
            raise Exception("Cannot specify both query and kwargs.")
        if self.corpus:
            if kwargs:
                query = self.normalise_query(kwargs)
            else:
                query = self.normalise_query(query)
            for result in self.corpus.search(query):
                yield result
        else:
            if kwargs:
                for doc in self.docs:
                    if all(next(doc[layer].matches(value), None)
                           for layer, value in kwargs.items()):
                        yield doc.id
            else:
                for doc in self.docs:
                    for key, value in query.items():
                        if not self._doc_matches(doc, key, value):
                            break
                    else:
                        yield doc.id

    def normalise_query(self, query):
        """Normalise a query by replacing all field values with either `$eq` or
        `$text`
        """
        q2 = {}
        for key, value in query.items():
            if isinstance(value, list):
                if all(isinstance(v, str) for v in value):
                    if key in self.meta and self.meta[key].data is None:
                        q2[key] = {"$text_in": value}
                    else:
                        q2[key] = {"$in": value}
            elif isinstance(value, dict):
                q2[key] = value
            elif key in self.meta and self.meta[key].data is None:
                q2[key] = {"$text": value}
            else:
                q2[key] = {"$eq": value}
        return q2


    def _doc_matches(self, doc, key, value):
        """
        """
        if key == "$exists":
            return value in doc.layers
        elif key == "$and":
            return all(self._doc_matches(doc, k, v) for k, v in value.items())
        elif key == "$or":
            return any(self._doc_matches(doc, k, v) for k, v in value.items())
        elif key == "$not":
            if isinstance(value, dict):
                return all(self._doc_matches(doc, k, v) for k, v in value.items())
            else:
                raise Exception("Invalid $not query.")
        elif key in self.meta:
            return doc[key].matches(value)
        else:
            raise Exception("Invalid key: " + key)

    def to_yaml(self, path_or_buf : str):
        """Write the corpus to a yaml file.

        Args:
            path_or_buf: str
                The path to the yaml file or a buffer.

        """
        if self.corpus:
            teangadb.write_corpus_to_yaml_file(path_or_buf, self.corpus)
        else:
            if isinstance(path_or_buf, str):
                with open(path_or_buf, "w") as f:
                    self._to_pretty_yaml(f)
            else:
                self._to_pretty_yaml(path_or_buf)

    def to_yaml_str(self) -> str:
        """
        Write the corpus to a yaml string.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus.to_yaml_str()
            '_meta:\\n    text:\\n        type: characters\\n\
Kjco:\\n    text: This is a document.\\n'
        """
        if self.corpus:
            return teangadb.write_corpus_to_yaml_string(self.corpus)
        else:
            s = StringIO()
            self._to_pretty_yaml(s)
            return s.getvalue()

    def _to_pretty_yaml(self, writer):
        """
        """
        writer.write("_meta:\n")
        for name in sorted(self.meta.keys()):
            meta = self.meta[name]
            writer.write("    " + name + ":\n")
            writer.write("        type: " + meta.layer_type + "\n")
            if meta.base:
                writer.write("        base: " + _yaml_str(meta.base))
            if meta.data:
                writer.write("        data: " +
                             self._dump_yaml_json(meta.data))
            if meta.link_types:
                writer.write("        link_types: " +
                             self._dump_yaml_json(meta.link_types))
            if meta.target:
                writer.write("        target: " +
                             self._dump_yaml_json(meta.target))
            if meta.default:
                writer.write("        default: " +
                             self._dump_yaml_json(meta.default))
        for id, doc in self._docs.items():
            if re.match(r"^[0-9]+$", id):
                writer.write("\"" + id + "\":\n")
            else:
                writer.write(id + ":\n")
            for layer_id in sorted(doc.layers):
                writer.write("    ")
                if isinstance(doc[layer_id].raw, str):
                    writer.write(layer_id)
                    writer.write(": ")
                    writer.write(_yaml_str(doc[layer_id].raw))
                else:
                    writer.write(layer_id + ": ")
                    writer.write(json.dumps(doc[layer_id].raw) + "\n")

    def _dump_yaml_json(self, obj):
        """
        """
        if obj is None:
            return "null"
        elif isinstance(obj, str):
            return _yaml_str(obj)
        else:
            return json.dumps(obj) + "\n"

    def to_json(self, path_or_buf):
        """Write the corpus to a JSON file.

        Args:
            path_or_buf: str
                The path to the json file or a buffer.

        """
        if self.corpus:
            teangadb.write_corpus_to_json_file(path_or_buf, self.corpus)
        else:
            if isinstance(path_or_buf, str):
                with open(path_or_buf, "w") as f:
                    self._to_json(f)
            else:
                self._to_json(path_or_buf)

    def to_json_str(self) -> str:
        """
        Write the corpus to a JSON string.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus.to_json_str()
            '{"_meta": {"text": {"type": "characters"}}, "_order": ["Kjco"], "Kjco": {"text": "This is a document."}}'
         """
        if self.corpus:
            return teangadb.write_corpus_to_json_string(self.corpus)
        else:
            s = StringIO()
            self._to_json(s)
            return s.getvalue()

    def _to_json(self, writer):
        dct = {}
        dct["_meta"] = {name: _from_layer_desc(data)
                        for name, data in self.meta.items()
                        if not name.startswith("_")}
        dct["_order"] = list(self.doc_ids)
        for doc_id, doc in self._docs.items():
            dct[doc_id] = {layer_id: doc[layer_id].raw
                           for layer_id in doc.layers}
        json.dump(dct, writer)

    def to_tcf(self, path:str):
        """Write the corpus to a TCF file.

        Args:
            path: str
                The path to the TCF file.
        """
        if self.corpus:
            teangadb.write_corpus_to_tcf(self.corpus, path)
        else:
            if not TEANGA_DB:
                teanga_db_fail()
            tmpfile = tempfile.mkstemp()[1]
            self.to_json(tmpfile)
            tmppath = tempfile.mkdtemp()
            corpus = teangadb.read_corpus_from_json_file(tmpfile, tmppath)
            teangadb.write_corpus_to_tcf(corpus, path)

    def apply(self, service : Service):
        """Apply a service to each document in the corpus.

        Args:
            service: The service to apply.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> corpus.add_layer_meta("first_char")
            >>> doc = corpus.add_doc(text="This is a document.")
            >>> from teanga.service import Service
            >>> class FirstCharService(Service):
            ...     def requires(self):
            ...         return {"text": { "type": "characters"}}
            ...     def produces(self):
            ...         return {"first_char": {"type": "characters"}}
            ...     def execute(self, input):
            ...         input["first_char"] = input["text"][0]
            ...         return input
            >>> corpus.apply(FirstCharService())
        """
        self.add_meta_from_service(service)
        for doc in self.docs:
            service.execute(doc)


    def lower(self) -> TransformedCorpus:
        """Lowercase all the text in the corpus.

        Examples:
            >>> corpus = Corpus()
            >>> corpus.add_layer_meta("text")
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus = corpus.lower()
            >>> list(corpus.docs)
            [Document('Kjco', {'text': CharacterLayer('this is a document.')})]
        """
        text_layers = [layer for layer in self.meta
                       if self.meta[layer].layer_type == "characters"]
        return TransformedCorpus(self, {layer: lambda x: x.lower()
                                        for layer in text_layers})

    def upper(self) -> TransformedCorpus:
        """Uppercase all the text in the corpus.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus = corpus.upper()
            >>> list(corpus.docs)
            [Document('Kjco', {'text': CharacterLayer('THIS IS A DOCUMENT.')})]
        """
        text_layers = [layer for layer in self.meta
                       if self.meta[layer].layer_type == "characters"]
        return TransformedCorpus(self, {layer: lambda x: x.upper()
                                        for layer in text_layers})

    def transform(self, layer: str, transform:
                  Callable[[str], str]) -> TransformedCorpus:
        """Transform a layer in the corpus.

        Args:
            layer: str
                The name of the layer to transform.
            transform: Callable[[str], str]
                The transformation function.

        Examples:
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus = corpus.transform("text", lambda x: x[:10])
            >>> list(corpus.docs)
            [Document('Kjco', {'text': CharacterLayer('This is a ')})]
        """
        return TransformedCorpus(self, {layer: transform})

    def writer(self, buf) -> CorpusWriter:
        """Create a writer object that can serialize documents in 
        a streaming fashion.

        Args:
            buf: str
                The buffer to write to.

        Examples:
            >>> import io
            >>> corpus = text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> string = io.StringIO()
            >>> with corpus.writer(string) as writer:
            ...     for doc in corpus.docs:
            ...         writer.write(doc)
        """
        return CorpusWriter(buf, self.meta)

def _yaml_str(s):
    """
    """
    s = yaml.safe_dump(s)
    if s.endswith("\n...\n"):
        s = s[:-4]
    if not s.startswith("'"):
        s = s.replace("\n", "\n    ")
        if s.endswith("\n    "):
            s = s[:-4]
    return s

def _corpus_hook(dct : dict) -> Corpus:
    """
    """
    c = Corpus()
    if "_meta" not in dct:
        return dct
    c.meta = {key: _layer_desc_from_kwargs(value)
              for key, value in dct["_meta"].items()
              if not key.startswith("_")}
    if "_order" in dct:
        for doc_id in dct["_order"]:
            c._docs[doc_id] = Document(c.meta, id=doc_id, **dct[doc_id])
    else:
        for doc_id, value in dct.items():
            if not doc_id.startswith("_"):
                doc = Document(c.meta, id=doc_id, **value)
                text_fields = {
                        field: value for field, value in value.items()
                        if isinstance(value, str)
                }
                tid = teanga_id_for_doc(c.doc_ids, **text_fields)
                if tid != doc_id:
                    raise Exception("Invalid document id: " + doc_id +
                                    " should be " + tid)
                c._docs[doc_id] = doc
    return c

def read_json_str(json_str:str, db_file:str=None) -> Corpus:
    """Read a corpus from a json string.

    Args:
        json_str: str
            The json string.
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.

    Examples:
        >>> corpus = read_json_str('{"_meta": {"text": {"type": \
    "characters"}},"Kjco": {"text": "This is a document."}}')
    """
    if db_file:
        if not TEANGA_DB:
            teanga_db_fail()
        return Corpus(db_corpus=teangadb.read_corpus_from_json_string(
            json_str, db_file))
    else:
        return json.loads(json_str, object_hook=_corpus_hook)

def read_json(path_or_buf, db_file:str=None) -> Corpus:
    """Read a corpus from a json file.

    Args:
        path_or_buf: str
            The path to the json file or a buffer.
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.
    """
    if db_file:
        if not TEANGA_DB:
            teanga_db_fail()
        return Corpus(db_corpus=teangadb.read_corpus_from_json_file(
            path_or_buf, db_file))
    else:
        return json.load(path_or_buf, object_hook=_corpus_hook)

def read_yaml(path_or_buf, db_file:str=None) -> Corpus:
    """Read a corpus from a yaml file.

    Args:
        path_or_buf: str
            The path to the yaml file or a buffer.
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.
    """
    if db_file:
        if not TEANGA_DB:
            teanga_db_fail()
        return Corpus(db_corpus=teangadb.read_corpus_from_yaml_file(
            path_or_buf, db_file))

    else:
        return _corpus_hook(yaml.load(open(path_or_buf), Loader=yaml.FullLoader))

def read_yaml_str(yaml_str, db_file:str=None) -> Corpus:
    """Read a corpus from a yaml string.

    Args:
        yaml_str: str
            The yaml string.
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.

    Examples:
        >>> yaml_str = '''_meta:
        ...   text:
        ...     type: characters
        ... Kjco:
        ...   text: This is a document.'''
        >>> corpus = read_yaml_str(yaml_str)
    """
    if db_file:
        if not TEANGA_DB:
            teanga_db_fail()
        return Corpus(db_corpus=teangadb.read_corpus_from_yaml_string(
            yaml_str, db_file))
    else:
        return _corpus_hook(yaml.load(yaml_str, Loader=yaml.FullLoader))
    
def parse(path_or_buf:str) -> CorpusStream:
    """Parse a corpus incrementally from a file or buffer. Note that you will need
    to load this into a Corpus object directly

    Args:
        path_or_buf: str
            The path to the file or a buffer.

    Examples:
        >>> import io
        >>> yaml_str = '''_meta:
        ...   text:
        ...     type: characters
        ... Kjco:
        ...   text: This is a document.'''
        >>> stream = parse(io.StringIO(yaml_str))
        >>> corpus = Corpus()
        >>> corpus._meta = stream.meta
        >>> for doc in stream:
        ...     _ = corpus.add_doc(doc)
    """
    return CorpusStream(path_or_buf)

def from_url(url:str, db_file:str=None) -> Corpus:
    """Read a corpus from a URL.

    Args:
        url: str
            The URL to read the corpus from.
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.
    """
    if db_file:
        if not TEANGA_DB:
            teanga_db_fail()
        return Corpus(db_corpus=teangadb.read_corpus_from_yaml_url(
            url, db_file))
    else:
        if url.endswith(".gz"):
            with gzip.open(urlopen(url), "rt") as f:
                return _corpus_hook(yaml.load(f, Loader=yaml.FullLoader))
        else:
            return _corpus_hook(yaml.load(urlopen(url), Loader=yaml.FullLoader))

def text_corpus(db_file:str = None) -> Corpus:
    """
    Create a corpus with a `text` and `tokens` layer

    Args:
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.
    
    Returns:
        A corpus with a `text` and `tokens` layer
    """
    corpus = Corpus(db=db_file)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("tokens", layer_type="span", base="text")
    return corpus

def parallel_corpus(languages : list[str], db_file:str = None) -> Corpus:
    """
    Create a corpus with a character layer and token layer for each language

    Args:
        languages: list[str]
            The languages to create the corpus for
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.

    Returns:
        A corpus with a character layer and token layer for each language

    Examples:
        >>> corpus = parallel_corpus(["en", "nl"])
    """
    corpus = Corpus(db=db_file)
    for lang in languages:
        corpus.add_layer_meta(lang, layer_type="characters")
        corpus.add_layer_meta(lang + "_tokens", layer_type="span", base=lang)
    return corpus

def read_tcf(file:str, db_file:str=None) -> Corpus:
    """Read a corpus from a TCF file. Requires TeangaDB module.

    Args:
        file: str
            The path to the TCF file.
        db_file: str
            The path to the database file, if the corpus should be stored in a
            database.
    """
    if not TEANGA_DB:
        teanga_db_fail()
    if not db_file:
        db_file = tempfile.mkdtemp()
    return Corpus(db_corpus=teangadb.read_corpus_from_tcf_file(file, db_file))

def teanga_db_fail():
    """
    """
    raise Exception("Teanga database not available. Please install the Teanga "
                    + "Rust package from https://github.com/teangaNLP/teanga.rs")
