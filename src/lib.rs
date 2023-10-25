// Purpose: Rust implementation of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use sled;
use speedy::{Writable,Readable};
use sha2::{Digest, Sha256};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use itertools::Itertools;
use serde::{Serialize,Deserialize};
use thiserror::Error;

mod serialization;

const DOCUMENT_PREFIX : u8 = 0x00;
const ID2STR_PREFIX : u8 = 0x01;
const STR2ID_PREFIX : u8 = 0x02;
const META_PREFIX : u8 = 0x03;
const ORDER_BYTES : [u8;1] = [0x04];
const STRIDX_SIZE : [u8;1] = [0x05];

#[pyclass]
#[derive(Debug,Clone)]
/// A corpus object
pub struct Corpus {
    #[pyo3(get)]
    meta: HashMap<String, LayerDesc>,
    #[pyo3(get)]
    order: Vec<String>,
    path: String
}

#[pyclass]
#[derive(Debug,Clone,Readable,Writable,Serialize,Deserialize)]
/// A layer description
struct LayerDesc {
    #[pyo3(get)]
    #[serde(rename = "type")]
    layer_type: LayerType,
    #[pyo3(get)]
    #[serde(default = "String::new")]
    on: String,
    #[pyo3(get)]
    data: Option<DataType>,
    #[pyo3(get)]
    values: Option<Vec<String>>,
    #[pyo3(get)]
    target: Option<String>,
    #[pyo3(get)]
    default: Option<Vec<String>>
}


#[pymethods]
impl Corpus {
    #[new]
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new(path : &str) -> TeangaResult<Corpus> {
        let db = open_db(path)?;
        let mut meta = HashMap::new();
        for m in db.scan_prefix(&[META_PREFIX]) {
            let (name, v) = m.map_err(|e| TeangaError::DBError(e))?;
            let layer_desc = LayerDesc::read_from_buffer(v.as_ref()).
                map_err(|e| TeangaError::DataError(e))?;
            let name = std::str::from_utf8(name.as_ref())
                .map_err(|_| TeangaError::UTFDataError)?.to_string();
            meta.insert(name, layer_desc);
        }
        let order = match db.get(ORDER_BYTES.to_vec())
            .map_err(|e| TeangaError::DBError(e))? {
            Some(bytes) => Vec::read_from_buffer(bytes.as_ref()).
                map_err(|e| TeangaError::DataError(e))?,
            None => Vec::new()
        };
        Ok(Corpus {
            meta,
            order,
            path: path.to_string()
        })
    }

    /// Add a layer to the corpus
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    /// * `layer_type` - The type of the layer
    /// * `on` - The layer that this layer is on
    /// * `data` - The data file for this layer
    /// * `values` - The values for this layer
    /// * `target` - The target layer for this layer
    /// * `default` - The default values for this layer
    #[pyo3(name="add_layer_meta")]
    pub fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        on: String, data: Option<DataType>, values: Option<Vec<String>>, 
        target: Option<String>, default: Option<Vec<String>>) -> TeangaResult<()> {
        CorpusTransaction::new(self)?.add_layer_meta(name, layer_type, on, data, values, target, default)
    }

    /// Add or update a document in the corpus
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The ID of the document
    pub fn add_doc(&mut self, content : HashMap<String, PyLayer>) -> TeangaResult<String> {
        CorpusTransaction::new(self)?.add_doc(content)
    }

    /// Update a document
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The new ID of the document (if no text layers are changed this will be the same as input)
    pub fn update_doc(&mut self, id : &str, content : HashMap<String, PyLayer>) -> TeangaResult<String> {
        CorpusTransaction::new(self)?.update_doc(id, content)
    }

 
    /// Remove a document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    pub fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        CorpusTransaction::new(self)?.remove_doc(id)
    }

    /// Get a document by its ID
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    ///
    /// # Returns
    ///
    /// The document as a map from layers names to layers
    pub fn get_doc_by_id(&self, id : &str) -> TeangaResult<HashMap<String, PyLayer>> {
        let db = open_db(&self.path)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        let data = db.get(id_bytes)
            .map_err(|e| TeangaError::DBError(e))?
            .ok_or_else(|| TeangaError::ModelError(
                format!("Document not found")))?;
        let doc = Document::read_from_buffer(data.as_ref()).
            map_err(|e| TeangaError::DataError(e))?;
        let mut result = HashMap::new();
        for (key, layer) in doc.content {
            let layer_desc : &LayerDesc = self.meta.get(&key).
                    ok_or_else(|| TeangaError::ModelError(
                        format!("Serialized document contains undeclared layer {}", key)))?;
            result.insert(key, layer.into_py(
                    layer_desc,
                    &|u| {
                        let mut id_bytes = Vec::new();
                        id_bytes.push(ID2STR_PREFIX);
                        id_bytes.extend(u.to_be_bytes());
                        String::from_utf8(
                            db.get(id_bytes)
                            .expect("Error reading string index")
                            .unwrap_or_else(|| panic!("String index not found"))
                            .as_ref().to_vec())
                            .expect("Unicode error in string index")
                    })?);

        }
        Ok(result)
    }

    /// Get the documents in the corpus
    ///
    /// # Returns
    ///
    /// The documents IDs in order
    pub fn get_docs(&self) -> Vec<String> {
        self.order.clone()
    }
}


/// A corpus with an open database connection to implement multiple changes
/// without closing the database
struct CorpusTransaction<'a> {
    corpus : &'a mut Corpus, 
    db : sled::Db
}

impl<'a> CorpusTransaction<'a> {
    /// Connect to the database
    fn new(corpus : &'a mut Corpus) -> TeangaResult<CorpusTransaction> {
        let db = open_db(&corpus.path)?;
        Ok(CorpusTransaction {
            corpus,
            db
        })
    }

    /// Add a layer to the corpus
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    /// * `layer_type` - The type of the layer
    /// * `on` - The layer that this layer is on
    /// * `data` - The data file for this layer
    /// * `values` - The values for this layer
    /// * `target` - The target layer for this layer
    /// * `default` - The default values for this layer
    pub fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        on: String, data: Option<DataType>, values: Option<Vec<String>>, 
        target: Option<String>, default: Option<Vec<String>>) -> TeangaResult<()> {
        if layer_type == LayerType::characters && on != "" {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type characters cannot be based on another layer", name)))
        }

        if layer_type != LayerType::characters && on == "" {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type {} must be based on another layer", name, layer_type)))
        }

        let data = match data {
            None => None,
            Some(DataType::String) => match values {
                Some(ref values) => Some(DataType::Enum(values.clone())),
                None => data
            },
            Some(DataType::Link) => match values {
                Some(ref values) => Some(DataType::TypedLink(values.clone())),
                None => data
            },
            _ => data
        };

        let layer_desc = LayerDesc {
            layer_type,
            on,
            data,
            values,
            target,
            default
         };

        let mut id_bytes = Vec::new();
        id_bytes.push(META_PREFIX);
        id_bytes.extend(name.clone().as_bytes());
        self.db.insert(id_bytes, layer_desc.write_to_vec()
            .map_err(|e| TeangaError::DataError(e))?).map_err(|e| TeangaError::DBError(e))?;

        self.corpus.meta.insert(name.clone(), layer_desc);
         Ok(())
    }

    /// Add or update a document in the corpus
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The ID of the document
    pub fn add_doc(&mut self, content : HashMap<String, PyLayer>) -> TeangaResult<String> {
        for key in content.keys() {
            if !self.corpus.meta.contains_key(key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }
        let mut doc_content = HashMap::new();
        for (k, v) in content {
            let layer_meta = self.corpus.meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                format!("No meta information for layer {}", k)))?;
            doc_content.insert(k, 
                Layer::from_py(v, layer_meta, &|u : &str| {
                        let mut id_bytes = Vec::new();
                        id_bytes.push(STR2ID_PREFIX);
                        id_bytes.extend(u.as_bytes());
                        let b = self.db.get(id_bytes)
                            .expect("Error reading string index")
                            .unwrap_or_else(|| panic!("String index not found"));
                        if b.len() != 4 {
                            panic!("String index is not 4 bytes");
                        }
                        u32::from_be_bytes(b.as_ref().try_into().unwrap())
                    })?);
        }
        let doc = Document::new(doc_content);
        let id = teanga_id(&self.corpus.order, &doc);
    
        self.corpus.order.push(id.clone());

        self.db.insert(ORDER_BYTES.to_vec(), self.corpus.order.write_to_vec().
            map_err(|e| TeangaError::DataError(e))?).map_err(|e| TeangaError::DBError(e))?;

        let data = doc.write_to_vec().map_err(|e| TeangaError::DataError(e))?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        self.db.insert(id_bytes, data).map_err(|e| TeangaError::DBError(e))?;
        Ok(id)
    }

    /// Update a document
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The new ID of the document (if no text layers are changed this will be the same as input)
    pub fn update_doc(&mut self, id : &str, content : HashMap<String, PyLayer>) -> TeangaResult<String> {
        for key in content.keys() {
            if !self.corpus.meta.contains_key(key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }

        let mut doc_content = HashMap::new();
        for (k, v) in content {
            let layer_meta = self.corpus.meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                format!("No meta information for layer {}", k)))?;
            doc_content.insert(k, 
                Layer::from_py(v, layer_meta, &|u : &str| {
                        let mut id_bytes = Vec::new();
                        id_bytes.push(STR2ID_PREFIX);
                        id_bytes.extend(u.as_bytes());
                        match self.db.get(id_bytes).expect("Error reading string index") {
                            Some(b) => {
                                if b.len() != 4 {
                                    panic!("String index is not 4 bytes");
                                }
                                u32::from_be_bytes(b.as_ref().try_into().unwrap())
                            },
                            None => {
                                gen_next_id(&self.db, u)
                            }
                        }
                    })?);
        }

        let doc = Document::new(doc_content);
        let new_id = teanga_id(&self.corpus.order, &doc);
        if id != new_id {
        
            let n = self.corpus.order.iter().position(|x| x == id).ok_or_else(|| TeangaError::ModelError(
                format!("Cannot find document in order vector: {}", id)))?;
            self.corpus.order.remove(n);
            self.corpus.order.insert(n, new_id.clone());

            self.db.insert(ORDER_BYTES.to_vec(), self.corpus.order.write_to_vec().
                map_err(|e| TeangaError::DataError(e))?).map_err(|e| TeangaError::DBError(e))?;

            let mut old_id_bytes = Vec::new();
            old_id_bytes.push(DOCUMENT_PREFIX);
            old_id_bytes.extend(id.as_bytes());
            self.db.remove(old_id_bytes).map_err(|e| TeangaError::ModelError(
                format!("Cannot remove document {}", e)))?;
        }

        let data = doc.write_to_vec().map_err(|e| TeangaError::DataError(e))?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(new_id.as_bytes());
        self.db.insert(id_bytes, data).map_err(|e| TeangaError::DBError(e))?;
        Ok(new_id)
    }

 
    /// Remove a document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    pub fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        self.db.remove(id_bytes).map_err(|e| TeangaError::ModelError(
            format!("Cannot remove document {}", e)))?;
        self.corpus.order.retain(|x| x != id);
        self.db.insert(ORDER_BYTES.to_vec(), self.corpus.order.write_to_vec().
            map_err(|e| TeangaError::DataError(e))?).map_err(|e| TeangaError::DBError(e))?;
        Ok(())
    }

    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.corpus.meta = meta;
        for (name, layer_desc) in self.corpus.meta.iter() {
            let mut id_bytes = Vec::new();
            id_bytes.push(META_PREFIX);
            id_bytes.extend(name.clone().as_bytes());
            self.db.insert(id_bytes, layer_desc.write_to_vec()
                .map_err(|e| TeangaError::DataError(e)).unwrap()).unwrap();
        }
        Ok(())
    }

    fn set_order(&mut self, order : Vec<String>) -> TeangaResult<()> {
        self.corpus.order = order;
        self.db.insert(ORDER_BYTES.to_vec(), self.corpus.order.write_to_vec().
            map_err(|e| TeangaError::DataError(e)).unwrap()).unwrap();
        Ok(())
    }
}

fn open_db(path : &str) -> TeangaResult<sled::Db> {
    sled::open(path).map_err(|e| TeangaError::DBError(e))
}

fn gen_next_id(db : &sled::Db, u : &str) -> u32 {
    let mut n = db.get(STRIDX_SIZE.to_vec())
        .expect("Error reading string index size")
        .map(|b| {
            if b.len() != 4 {
                panic!("String index size is not 4 bytes");
            }
            u32::from_be_bytes(b.as_ref().try_into().unwrap())
        }).unwrap_or(0);

    while let Err(_) = db.compare_and_swap(STRIDX_SIZE.to_vec(), 
        if n == 0 { None } else { Some(n.to_be_bytes().to_vec()) }, 
        Some((n+1).to_be_bytes().to_vec()))
        .expect("Error reading DB") {
        n = db.get(STRIDX_SIZE.to_vec())
            .expect("Error reading string index size")
            .map(|b| {
                if b.len() != 4 {
                    panic!("String index size is not 4 bytes");
                }
                u32::from_be_bytes(b.as_ref().try_into().unwrap())
            }).unwrap_or(0);
    }
    n = n + 1;
    let mut id_bytes = Vec::new();
    id_bytes.push(STR2ID_PREFIX);
    id_bytes.extend(u.as_bytes());
    db.insert(id_bytes, n.to_be_bytes().to_vec()).expect("Error reading DB");
    let mut id_bytes = Vec::new();
    id_bytes.push(ID2STR_PREFIX);
    id_bytes.extend(n.to_be_bytes().to_vec());
    db.insert(id_bytes, u.as_bytes().to_vec()).expect("Error reading DB");
    n
}

#[derive(Debug,Clone,Readable,Writable)]
/// A document object
struct Document {
    content: HashMap<String, Layer>
}

impl Document {
    fn new(content : HashMap<String, Layer>) -> Document {
        Document {
            content
        }
    }
}

#[derive(Debug,Clone,Readable,Writable)]
enum Layer {
    Characters(String),
    Seq(Vec<u32>),
    Div(Vec<(u32,u32)>),
    DivNoData(Vec<u32>),
    Element(Vec<(u32,u32)>),
    ElementNoData(Vec<u32>),
    Span(Vec<(u32,u32,u32)>),
    SpanNoData(Vec<(u32,u32)>)
}

impl Layer {
    fn into_py<F>(&self, meta : &LayerDesc, idx2str : &F) -> TeangaResult<PyLayer> 
        where F : Fn(u32) -> String {
        match self {
            Layer::Characters(val) => Ok(PyLayer::CharacterLayer(val.clone())),
            Layer::Seq(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but not data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for id in val {
                            result.push(u32_into_py_str(*id, &DataType::String, idx2str)?);
                        }
                        Ok(PyLayer::LS(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for id in val {
                            result.push(vals[*id as usize].clone());
                        }
                        Ok(PyLayer::LS(result))
                    },
                    Some(DataType::Link) => {
                        Ok(PyLayer::L1(val.clone()))
                    },
                    Some(DataType::TypedLink(ref vals)) => {
                        let mut result = Vec::new();
                        for id in val {
                            result.push(u32_into_py_u32_str(*id, &DataType::TypedLink(vals.clone()))?);
                        }
                        Ok(PyLayer::L1S(result))
                    }
                }
            },
            Layer::Div(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but no data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, u32_into_py_str(*data, &DataType::String, idx2str)?));
                        }
                        Ok(PyLayer::L1S(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, vals[*data as usize].clone()));
                        }
                        Ok(PyLayer::L1S(result))
                    },
                    Some(DataType::Link) => {
                        Ok(PyLayer::L2(val.clone()))
                    },
                    Some(DataType::TypedLink(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            let tl = u32_into_py_u32_str(*data, &DataType::TypedLink(vals.clone()))?;
                            result.push((*start, tl.0, tl.1));
                        }
                        Ok(PyLayer::L2S(result))
                    }
                }
            },
            Layer::Element(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but no data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, u32_into_py_str(*data, &DataType::String, idx2str)?));
                        }
                        Ok(PyLayer::L1S(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, vals[*data as usize].clone()));
                        }
                        Ok(PyLayer::L1S(result))
                    },
                    Some(DataType::Link) => {
                        Ok(PyLayer::L2(val.clone()))
                    },
                    Some(DataType::TypedLink(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            let tl = u32_into_py_u32_str(*data, &DataType::TypedLink(vals.clone()))?;
                            result.push((*start, tl.0, tl.1));
                        }
                        Ok(PyLayer::L2S(result))
                    }
                }
            },
            Layer::Span(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but no data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            result.push((*start, *end, u32_into_py_str(*data, &DataType::String, idx2str)?));
                        }
                        Ok(PyLayer::L2S(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            result.push((*start, *end, vals[*data as usize].clone()));
                        }
                        Ok(PyLayer::L2S(result))
                    },
                    Some(DataType::Link) => {
                        Ok(PyLayer::L3(val.clone()))
                    },
                    Some(DataType::TypedLink(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            let tl = u32_into_py_u32_str(*data, &DataType::TypedLink(vals.clone()))?;
                            result.push((*start, *end, tl.0, tl.1));
                        }
                        Ok(PyLayer::L3S(result))
                    }
                }
            },
            Layer::DivNoData(val) => {
                let mut result = Vec::new();
                for start in val {
                    result.push(*start);
                }
                Ok(PyLayer::L1(result))
            },
            Layer::ElementNoData(val) => {
                let mut result = Vec::new();
                for start in val {
                    result.push(*start);
                }
                Ok(PyLayer::L1(result))
            },
            Layer::SpanNoData(val) => {
                let mut result = Vec::new();
                for (start, end) in val {
                    result.push((*start, *end));
                }
                Ok(PyLayer::L2(result))
            }
        }
    }

    fn from_py<F>(obj : PyLayer, meta : &LayerDesc, str2idx : &F) -> TeangaResult<Layer> 
        where F : Fn(&str) -> u32 {
        match obj {
            PyLayer::CharacterLayer(val) => Ok(Layer::Characters(val)),
            PyLayer::L1(val) => {
                match meta.data {
                    Some(_) => {
                        Ok(Layer::Seq(val))
                    },
                    None => {
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::DivNoData(val)),
                            LayerType::element => Ok(Layer::ElementNoData(val)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    }
                }
            },
            PyLayer::L2(val) => {   
                match meta.data {
                    Some(_) => {
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::Div(val)),
                            LayerType::element => Ok(Layer::Element(val)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    },
                    None => {
                        Ok(Layer::SpanNoData(val))
                    }
                }
            },
            PyLayer::L3(val) => {
                Ok(Layer::Span(val))
            },
            PyLayer::LS(val) => {
                let mut result = Vec::new();
                for data in val {
                    result.push(py_str_into_u32(&data, &DataType::String, str2idx)?);
                }
                Ok(Layer::Seq(result))
            },
            PyLayer::L1S(val) => {
                match meta.data {
                    Some(ref metadata @ DataType::TypedLink(_)) => {
                        let mut result = Vec::new();
                        for (idx, link) in val {
                            result.push(py_u32_str_into_u32(idx, link, &metadata)?);
                        }
                        Ok(Layer::Seq(result))
                    },
                    Some(ref metadata) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((start, py_str_into_u32(&data, metadata, str2idx)?));
                        }
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::Div(result)),
                            LayerType::element => Ok(Layer::Element(result)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    },
                    None => Err(TeangaError::ModelError(
                        format!("String in data, but data type is none")))
                }
            },
            PyLayer::L2S(val) => {
                let metadata = meta.data.as_ref().ok_or_else(|| TeangaError::ModelError(
                    format!("Cannot convert data layer to {}", meta.layer_type)))?;
                match meta.data {
                    Some(ref metadata @ DataType::TypedLink(_)) => {
                        let mut result = Vec::new();
                        for (start, idx, link) in val {
                            result.push((start, py_u32_str_into_u32(idx, link, &metadata)?));
                        }
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::Div(result)),
                            LayerType::element => Ok(Layer::Element(result)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    },
                    _ => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            result.push((start, end, py_str_into_u32(&data, metadata, str2idx)?));
                        }
                        Ok(Layer::Span(result))
                    }
                }
            },
            PyLayer::L3S(val) => {
                let metadata = meta.data.as_ref().ok_or_else(|| TeangaError::ModelError(
                    format!("Cannot convert data layer to {}", meta.layer_type)))?;
                let mut result = Vec::new();
                for (start, end, idx, link) in val {
                    result.push((start, end, py_u32_str_into_u32(idx, link, metadata)?));
                }
                Ok(Layer::Span(result))
            }
        }
    }
}

#[derive(Debug,Clone,PartialEq, Serialize, Deserialize)]
#[derive(FromPyObject)]
#[serde(untagged)]
pub enum PyLayer {
    CharacterLayer(String),
    L1(Vec<u32>),
    L2(Vec<(u32,u32)>),
    L3(Vec<(u32,u32,u32)>),
    LS(Vec<String>),
    L1S(Vec<(u32,String)>),
    L2S(Vec<(u32,u32,String)>),
    L3S(Vec<(u32,u32,u32,String)>)
}

impl IntoPy<PyObject> for PyLayer {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            PyLayer::CharacterLayer(val) => val.into_py(py),
            PyLayer::L1(val) => val.into_py(py),
            PyLayer::L2(val) => val.into_py(py),
            PyLayer::L3(val) => val.into_py(py),
            PyLayer::LS(val) => val.into_py(py),
            PyLayer::L1S(val) => val.into_py(py),
            PyLayer::L2S(val) => val.into_py(py),
            PyLayer::L3S(val) => val.into_py(py)
        }
    }
}

fn u32_into_py_str<F>(val : u32, layer_type : &DataType, f : &F) -> TeangaResult<String> 
    where F : Fn(u32) -> String {
    match layer_type {
        DataType::String => Ok(f(val)),
        DataType::Enum(vals) => {
            if val < vals.len() as u32 {
                Ok(vals[val as usize].clone())
            } else {
                Err(TeangaError::ModelError(
                        format!("Enum data is out of range of enum")))
            }
        }
        _ => Err(TeangaError::ModelError(
                format!("Cannot convert {} to string", layer_type)))
    }
}

fn u32_into_py_u32_str(val : u32, layer_type : &DataType) -> TeangaResult<(u32,String)> {
    match layer_type {
        DataType::TypedLink(vals) => {
            let n = (vals.len() as f64).log2().ceil() as u32;
            let link_targ = val >> n;
            let link_type = val & ((1 << n) - 1);
            if link_type < vals.len() as u32 {
                Ok((link_targ, vals[link_type as usize].clone()))
            } else {
                Err(TeangaError::ModelError(
                        format!("Link type is out of range of enum")))
            }
        }
        _ => Err(TeangaError::ModelError(
                format!("Cannot convert {} to string", layer_type)))
    }
}

fn py_str_into_u32<F>(val : &str, layer_type : &DataType, f : &F) -> TeangaResult<u32> 
    where F : Fn(&str) -> u32 {
    match layer_type {
        DataType::String => Ok(f(val)),
        DataType::Enum(vals) => {
            match vals.iter().position(|x| x == val) {
                Some(idx) => Ok(idx as u32),
                None => Err(TeangaError::ModelError(
                        format!("Cannot convert enum {} to {}", val, vals.iter().join(","))))
            }
        },
        _ => Err(TeangaError::ModelError( 
                format!("Cannot convert string to {}", layer_type)))
    }
}

fn py_u32_str_into_u32(link_targ : u32, link_type : String, layer_type : &DataType) -> TeangaResult<u32> {
    match layer_type {
        DataType::TypedLink(vals) => {
            match vals.iter().position(|x| *x == link_type) {
                Some(idx) => Ok((idx as u32) << ((vals.len() as f64).log2().ceil() as u32) | link_targ),
                None => Err(TeangaError::ModelError(
                        format!("Cannot convert link type {} to {}", link_type, vals.iter().join(","))))
            }
        },
        _ => Err(TeangaError::ModelError(
            format!("Cannot convert string and int to {}", layer_type)))
    }
}

fn teanga_id(existing_keys : &Vec<String>, doc : &Document) -> String {
    let mut hasher = Sha256::new();
    for key in doc.content.keys().sorted() {
        match doc.content.get(key).unwrap() {
            Layer::Characters(val) => {
                hasher.update(key.as_bytes());
                hasher.update(vec![0u8]);
                hasher.update(val.as_bytes());
                hasher.update(vec![0u8]);
            }
            _ => ()
        }
    }
    let code = STANDARD.encode(hasher.finalize().as_slice());
    let mut n = 4;
    while existing_keys.contains(&code[..n].to_string()) && n < code.len() {
        n += 1;
    }
    return code[..n].to_string();
}

#[allow(non_camel_case_types)]
#[derive(Debug,Clone,PartialEq,Readable,Writable,Serialize,Deserialize)]
pub enum LayerType {
    characters,
    seq,
    div,
    element,
    span
}

impl FromPyObject<'_> for LayerType {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        match ob.extract::<String>()?.to_lowercase().as_str() {
            "characters" => Ok(LayerType::characters),
            "seq" => Ok(LayerType::seq),
            "div" => Ok(LayerType::div),
            "element" => Ok(LayerType::element),
            "span" => Ok(LayerType::span),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown layer type {}", ob.extract::<String>()?)))
        }
    }
}

impl IntoPy<PyObject> for LayerType {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            LayerType::characters => "characters".into_py(py),
            LayerType::seq => "seq".into_py(py),
            LayerType::div => "div".into_py(py),
            LayerType::element => "element".into_py(py),
            LayerType::span => "span".into_py(py)
        }
    }
}

impl Display for LayerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LayerType::characters => write!(f, "characters"),
            LayerType::seq => write!(f, "seq"),
            LayerType::div => write!(f, "div"),
            LayerType::element => write!(f, "element"),
            LayerType::span => write!(f, "span")
        }
    }
}

#[derive(Debug,Clone,PartialEq,Readable,Writable,Serialize,Deserialize)]
pub enum DataType {
    String,
    Enum(Vec<String>),
    Link,
    TypedLink(Vec<String>)
}

impl FromPyObject<'_> for DataType {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        match ob.extract::<Vec<String>>() {
            Ok(vals) => return Ok(DataType::Enum(vals)),
            Err(_) => ()
        };
        match ob.extract::<String>()?.to_lowercase().as_str() {
            "string" => Ok(DataType::String),
            "link" => Ok(DataType::Link),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown data type {}", ob.extract::<String>()?)))
        }
    }
}

impl IntoPy<PyObject> for DataType {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            DataType::String => "string".into_py(py),
            DataType::Enum(_) => "string".into_py(py),
            DataType::Link => "link".into_py(py),
            DataType::TypedLink(_) => "link".into_py(py)
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Enum(vals) => write!(f, "enum({})", vals.iter().join(",")),
            DataType::Link => write!(f, "link"),
            DataType::TypedLink(vals) => write!(f, "link({})", vals.iter().join(","))
        }
    }
}

#[pyfunction]
fn read_corpus_from_json_string(s : &str, path : &str) -> PyResult<Corpus> {
    serialization::read_corpus_from_json_string(s, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn read_corpus_from_yaml_string(s : &str, path: &str) -> PyResult<Corpus> {
    serialization::read_corpus_from_yaml_string(s, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn write_corpus_to_yaml(corpus : &Corpus, path : &str) -> PyResult<()> {
    serialization::write_corpus_to_yaml(corpus, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name="teangadb")]
fn teangadb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Corpus>()?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml, m)?)?;
    Ok(())
}

#[derive(Error, Debug)]
pub enum TeangaError {
    #[error("DB read error: {0}")]
    DBError(#[from] sled::Error),
    #[error("Data read error: {0}")]
    DataError(#[from] speedy::Error),
    #[error("Data read error: UTF-8 String could not be decoded")]
    UTFDataError,
    #[error("Teanga model error: {0}")]
    ModelError(String),
}

type TeangaResult<T> = Result<T, TeangaError>;

impl From<TeangaError> for PyErr {
    fn from(err: TeangaError) -> PyErr {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", err))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_teanga_id_1() {
        let existing_keys = Vec::new();
        let doc = Document {
            content: vec![("text".to_string(), 
                         Layer::Characters("This is a document.".to_string()))].into_iter().collect()
        };
        let expected = "Kjco";
        assert_eq!(teanga_id(&existing_keys, &doc), expected);
    }

    #[test]
    fn test_gen_next_id() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        assert_eq!(1, gen_next_id(&db, "A"));
        assert_eq!(2, gen_next_id(&db, "B"));
    }
}
