from typing import Callable, TYPE_CHECKING, Iterator
if TYPE_CHECKING:
    from corpus import Corpus
    from service import Service
    from document import Document

class TransformedCorpus:
    """A corpus that lazily applies a transformation to its documents."""
    def __init__(self, corpus : 'Corpus', transform : dict[str, Callable[[str], str]]):
        """Create a new TransformedCorpus.

        Args:
            corpus: The corpus to transform.
            transform: A dictionary mapping layer names to functions 
            that transform the layer.
        """
        self.corpus = corpus
        self._transform = transform

    def add_meta_from_service(self, service: 'Service'):
        """Add metadata from a service to the corpus.
        See Corpus.add_meta_from_service for more information.
        """
        self.corpus.add_meta_from_service(service)

    def add_layer_meta(self, name:str=None,
                  layer_type:str="characters", base:str=None, 
                  data=None, link_types:list[str]=None,
                  target:str=None, default=None):
        """Add a layer to the corpus.
        See Corpus.add_layer_meta for more information.
        """
        self.corpus.add_layer_meta(name, layer_type, base, 
                                   data, link_types, target, default)

    def add_doc(self, *args, **kwargs) -> 'Document':
        """Add a document to the corpus.
        See Corpus.add_doc for more information.
        """
        return self.corpus.add_doc(*args, **kwargs)

    def doc_ids(self) -> list[str]:
        """Return a list of document ids in the corpus.
        See Corpus.doc_ids for more information.
        """
        return self.corpus.doc_ids()

    @property
    def docs(self) -> Iterator[tuple[str, 'Document']]:
        """Return an iterator over the documents in the corpus.
        See Corpus.docs for more information.
        """
        for doc_id, doc in self.corpus.docs:
           yield (doc_id, self.transform_doc(doc)) 

    def transform_doc(self, doc: 'Document') -> 'Document':
        """Transform a document using the transformation functions.

        Args:
            doc: The document to transform.

        Returns:
            A new document with the transformed layers.
        """
        new_doc = doc.copy()
        for layer_name, transform in self._transform.items():
            new_doc[layer_name] = doc.layers[layer_name].transform(transform)
        return new_doc

    def doc_by_id(self, doc_id: str) -> 'Document':
        """Return a document by its id.
        See Corpus.doc_by_id for more information.
        """
        return self.transform_doc(self.corpus.doc_by_id(doc_id))

    @property
    def meta(self) -> dict:
        """Return the metadata of the corpus.
        See Corpus.meta for more information.
        """
        return self.corpus.meta

    @meta.setter
    def meta(self, value: dict):
        """Set the metadata of the corpus.
        See Corpus.meta for more information.
        """
        self.corpus.meta = value

    def apply(self, service: 'Service'):
        """Apply a service to the corpus.
        See Corpus.apply for more information.
        """
        self.corpus.apply(service)

    def lower(self):# -> Self:
        """Lowercase all the text in the corpus. """
        text_layers = [layer for layer in self.meta 
                       if self.meta[layer].layer_type == "characters"]
        new_transform = self._transform.copy()
        for layer in text_layers:
            if layer in self._transform:
                new_transform[layer] = lambda x: self._transform[layer](x).lower()
            else:
                new_transform[layer] = lambda x: x.lower()
        return TransformedCorpus(self, new_transform)

    def upper(self):# -> Self:
        """Uppercase all the text in the corpus. """
        text_layers = [layer for layer in self.meta 
                       if self.meta[layer].layer_type == "characters"]
        new_transform = self._transform.copy()
        for layer in text_layers:
            if layer in self._transform:
                new_transform[layer] = lambda x: self._transform[layer](x).upper()
            else:
                new_transform[layer] = lambda x: x.upper()
        return TransformedCorpus(self, new_transform)

    def transform(self, layer: str, transform: 
                  Callable[[str], str]):# -> Self:
        """Transform a layer in the corpus.

        Parameters:
            layer: str
                The name of the layer to transform.
            transform: Callable[[str], str]
                The transformation function.

        Examples:
            >>> import teanga
            >>> corpus = teanga.text_corpus()
            >>> doc = corpus.add_doc("This is a document.")
            >>> corpus = corpus.upper().transform("text", lambda x: x[:10])
            >>> list(corpus.docs)
            [('Kjco', Document('Kjco', {'text': CharacterLayer('THIS IS A ')}))]
        """
        new_transform = self._transform.copy()
        if layer in self._transform:
            new_transform[layer] = lambda x: transform(self._transform[layer](x))
        else:
            new_transform[layer] = transform
        return TransformedCorpus(self, new_transform)




