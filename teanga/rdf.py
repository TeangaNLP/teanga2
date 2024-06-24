import rdflib
from rdflib import RDF
import teanga

TEANGA_BUILT_INS = set(["text", "words", "sentences", "paragraphs"])

def teanga_corpus_to_rdf(graph, corpus, url: str):
    """
    Convert a Teanga Corpus to RDF using the Teanga Namespace. The corpus
    is added to the current graph

    Parameters
    ----------

    graph : rdflib.Graph
    url : str
        The URL of the Teanga Corpus
    """
    teanga = rdflib.Namespace("http://teanga.io/teanga#")
    graph.add((rdflib.URIRef(url), RDF.type, teanga.Corpus))
    for document in corpus.docs:
        document_id, document = document
        doc_url = url + "#" + document_id
        graph.add((rdflib.URIRef(doc_url), RDF.type, teanga.Document))
        graph.add((rdflib.URIRef(url), teanga.document, rdflib.URIRef(doc_url)))
        for layer in document.layers:
            layer_desc = corpus.meta[layer]
            if "uri" in layer_desc.meta:
                layer_url = rdflib.URIRef(layer_desc.meta["uri"])
            elif layer in TEANGA_BUILT_INS:
                layer_url = teanga[layer]
            else:
                layer_url = url + "#" + layer
            layer_url = rdflib.URIRef(layer_url)
            if layer_desc.layer_type == "characters":
                graph.add((rdflib.URIRef(doc_url), 
                           layer_url,
                           rdflib.Literal(document[layer].text[0])))
            else:
                for idx, data in enumerate(document[layer].raw):
                    base_layer = layer_desc.base
                    base_layer_desc = corpus.meta[base_layer]
                    node_url = _node_url(url, document_id, layer,
                                         layer_desc.layer_type, idx)
                    node = rdflib.URIRef(node_url)
                    graph.add((rdflib.URIRef(doc_url), layer_url, node))
                    data_value = None
                    graph.add((node, teanga.idx, rdflib.Literal(idx)))
                    if layer_desc.layer_type == "element":
                        if isinstance(data, list) or isinstance(data, tuple):
                            target_url = _node_url(url, document_id, base_layer,
                                               base_layer_desc.layer_type, data[0])
                            data_value = data[1:]    
                        else:
                            target_url = _node_url(url, document_id, base_layer,
                                               base_layer_desc.layer_type, data)
                    elif layer_desc.layer_type == "seq":
                        target_url = _node_url(url, document_id, base_layer,
                                               base_layer_desc.layer_type, idx)
                        data_value = data
                    elif layer_desc.layer_type == "span":
                        target_url = _node_url(url, document_id, base_layer,
                                               base_layer_desc.layer_type, data[0], 
                                               data[1])
                        if len(data) == 3:
                            data_value = data[2:]
                    elif layer_desc.layer_type == "div":
                        if isinstance(data, list) or isinstance(data, tuple):
                            target_url = _node_url(url, document_id, base_layer,
                                               base_layer_desc.layer_type, data[0])
                            data_value = data[1:]
                        else:
                            target_url = _node_url(url, document_id, base_layer,
                                               base_layer_desc.layer_type, data)
                    else:
                         raise ValueError("Unknown layer type: " + 
                                          layer_desc.layer_type)
                    if ((isinstance(data, list) or isinstance(data, tuple)) 
                        and len(data) == 1):
                        data = data[0]
                    graph.add((node, teanga.ref, rdflib.URIRef(target_url)))
                    write_teanga_data(graph, node, data_value, teanga, 
                                      layer_desc, url, document_id, document)

    return graph

def _node_url(url: str, doc_id : str, layer: str, 
              layer_type: str, idx: int, end_idx: int = None):
    if layer_type == "characters":
        if end_idx:
            return (url + "#" + doc_id + "&layer=" + layer + 
                    "&char=" + str(idx) + "," + str(end_idx))
        else:
            return url + "#" + doc_id + "&layer=" + layer + "&char=" + str(idx)
    else:
        return url + "#" + doc_id + "&layer=" + layer + "&idx=" + str(idx)
    
def write_teanga_data(graph : rdflib.Graph, 
                      node : rdflib.URIRef, 
                      data, 
                      teanga : rdflib.Namespace, 
                      layer_desc : teanga.LayerDesc,
                      url : str,
                      document_id : str,
                      document: teanga.Document) -> None:
    data_type = layer_desc.data
    if data_type is None:
        return
    elif data_type == "string":
        graph.add((node, teanga.data, rdflib.Literal(data)))
    elif data_type == "link":
        if not layer_desc.target:
            target = layer_desc.base
        else:
            target = layer_desc.target
        if layer_desc.link_types:
            target_url = _node_url(url, document_id, target,
                               document.meta[target].layer_type,
                               data[0])
            link_type = rdflib.URIRef(url + "#" + data[1])
            graph.add((node, link_type, rdflib.URIRef(target_url)))
        else:
            target_url = _node_url(url, document_id, target,
                               document.meta[target].layer_type,
                               data)
            graph.add((node, teanga.link, rdflib.URIRef(target_url)))
    elif isinstance(data_type, list):
        graph.add((node, teanga.data, rdflib.Literal(data)))
    else:
        raise ValueError("Unknown data type: " + data_type)

def teanga_corpus_to_nif(graph, corpus, url :str) -> None:
    """
    Convert a Teanga Corpus to RDF using the NIF Namespace. The corpus
    is added to the current graph

    Parameters
    ----------

    graph : rdflib.Graph
        A graph to add the corpus to (normally an empty graph)
    corpus : teanga.Corpus
        The corpus to convert
    url : str
        The URL of the Teanga Corpus
    """
    nif = rdflib.Namespace("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#")
    teanga = rdflib.Namespace("http://teanga.io/teanga#")
    for document in corpus.docs:
        document_id, document = document
        doc_url = url + "#" + document_id
        graph.add((rdflib.URIRef(doc_url), RDF.type, teanga.Document))
        graph.add((rdflib.URIRef(doc_url), RDF.type, nif.Context))
        for layer in document.layers:
            layer_desc = corpus.meta[layer]
            if "uri" in layer_desc.meta:
                layer_url = rdflib.URIRef(layer_desc.meta["uri"])
            elif layer in TEANGA_BUILT_INS:
                layer_url = teanga[layer]
            else:
                layer_url = url + "#" + layer
            layer_url = rdflib.URIRef(layer_url)
            if layer_desc.layer_type == "characters":
                node_url = url + "#" + document_id + "&layer=" + layer
                graph.add((rdflib.URIRef(doc_url), 
                           teanga.document, 
                           rdflib.URIRef(node_url)))
                graph.add((rdflib.URIRef(node_url), 
                           nif.referenceContext, 
                           rdflib.URIRef(doc_url)))
                graph.add((rdflib.URIRef(node_url), 
                           RDF.type, nif.String))
                graph.add((rdflib.URIRef(node_url),
                           nif.isString, 
                           rdflib.Literal(document[layer].text[0])))
            else:
                root_layer = document[layer].root_layer()
                for idx, ((start_idx, end_idx), data) in enumerate(document[layer].
                        indexes_data(root_layer)):
                    node_url = _node_url(url, document_id, layer,
                                         layer_desc.layer_type, idx)
                    node = rdflib.URIRef(node_url)

                    rel_start_idx, rel_end_idx = (document[layer].
                                                  indexes(layer_desc.base)[idx])
                    if layer_desc.layer_type == "span":
                        base_url = _node_url(url, document_id, layer_desc.base,
                                         corpus.meta[layer_desc.base].layer_type, 
                                                rel_start_idx, rel_end_idx)
                    else:
                        base_url = _node_url(url, document_id, layer_desc.base,
                                         corpus.meta[layer_desc.base].layer_type, 
                                                rel_start_idx)
                    graph.add((node, nif.superStringTrans, rdflib.URIRef(base_url)))

                    root_url = url + "#" + document_id + "&layer=" + root_layer

                    graph.add((node, nif.superString, rdflib.URIRef(root_url)))

                    graph.add((node, RDF.type, nif.OffsetbasedString))
                    graph.add((node, RDF.type, nif.String))
                    graph.add((node, nif.beginIndex, rdflib.Literal(start_idx, datatype=rdflib.XSD.nonNegativeInteger)))
                    graph.add((node, nif.endIndex, rdflib.Literal(end_idx, datatype=rdflib.XSD.nonNegativeInteger)))
                    if isinstance(data, str):
                        graph.add((node, layer_url, rdflib.Literal(data)))
                    elif isinstance(data, int):
                        pass
                    elif isinstance(data, tuple):
                        pass
                    else:
                        graph.add((node, RDF.value, layer_url))
                    write_teanga_data(graph, node, data, teanga, layer_desc,
                                      url, document_id, document)

 
def teanga_corpus_to_webanno(corpus : teanga.Corpus, url : str) -> list[dict]:
    """Convert a Teanga Corpus to a list of WebAnno JSON objects

    Parameters
    ----------

    corpus : teanga.Corpus
        The corpus to convert
    url : str
        The URL of the document
    """
    webannos = []
    teanga_ns = "http://teanga.io/teanga#"
    for document in corpus.docs:
        document_id, document = document
        for layer in document.layers:
            layer_desc = corpus.meta[layer]
            if "uri" in layer_desc.meta:
                layer_url = layer_desc.meta["uri"]
            elif layer in TEANGA_BUILT_INS:
                layer_url = teanga_ns + layer
            else:
                layer_url = url + "#" + layer
            if layer_desc.layer_type != "characters":
                root_layer = document[layer].root_layer()
                for idx, ((start_idx, end_idx), data) in enumerate(document[layer].
                        indexes_data(root_layer)):
                    webanno = {}
                    webanno["id"] = ("#" + document_id + "&layer=" + layer + "&idx=" 
                                     + str(idx))
                    webanno["type"] = "Annotation"
                    webanno["target"] = {
                            "source": url + "#" + document_id + "&layer=" + root_layer,
                            "selector": {
                                "type": "TextPositionSelector",
                                "start": start_idx,
                                "end": end_idx
                                }
                            }
                    if isinstance(data, str):
                        webanno["body"] = {
                                "type": "TextualBody",
                                "value": data
                                }
                    elif isinstance(data, int):
                        if not layer_desc.target:
                            target = layer_desc.base
                        else:
                            target = layer_desc.target
                        target_url = _node_url(url, document_id, target,
                               document.meta[target].layer_type,
                               data)
                        webanno["body"] = {
                                "id": target_url }
                    elif isinstance(data, tuple):
                        if not layer_desc.target:
                            target = layer_desc.base
                        else:
                            target = layer_desc.target
                        target_url = _node_url(url, document_id, target,
                               document.meta[target].layer_type,
                               data[0])
                        webanno["body"] = {
                                "id": target_url }
                    else:
                        webanno["body"] = {
                                "value": {
                                    "@id": layer_url
                                    }
                                }

                    webannos.append(webanno)

    return webannos
