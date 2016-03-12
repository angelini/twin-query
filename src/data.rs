use bincode;
use bincode::rustc_serialize as serialize;
use bincode::SizeLimit;
use flate2::write::ZlibEncoder;
use flate2::read::ZlibDecoder;
use flate2::Compression;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io;
use std::path;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Bool(bool),
    Int(usize),
    String(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Bool(v) => write!(f, "{:?}", v),
            Value::Int(v) => write!(f, "{:?}", v),
            Value::String(ref v) => write!(f, "{:?}", v),
        }
    }
}

#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
pub struct Datum<T> {
    pub id: usize,
    pub value: T,
    pub time: usize,
}

impl<T> Datum<T> {
    fn new(id: usize, value: T, time: usize) -> Datum<T> {
        Datum {
            id: id,
            value: value,
            time: time,
        }
    }
}

impl<T: fmt::Display> fmt::Display for Datum<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.value, self.time)
    }
}

#[derive(Debug)]
pub struct GenericDatum {
    id: usize,
    value: Value,
    time: usize,
}

impl GenericDatum {
    pub fn new(id: usize, value: Value, time: usize) -> Self {
        GenericDatum {
            id: id,
            value: value,
            time: time,
        }
    }
}

impl fmt::Display for GenericDatum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.value, self.time)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, RustcEncodable, RustcDecodable)]
pub struct ColumnName {
    pub table: String,
    pub column: String,
}

impl ColumnName {
    pub fn new<S: Into<String>>(table: S, column: S) -> ColumnName {
        ColumnName {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn id(&self) -> ColumnName {
        ColumnName::new(self.table.to_owned(), "id".to_owned())
    }
}

impl fmt::Display for ColumnName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.column)
    }
}

#[derive(Debug)]
pub enum ColumnType {
    Bool,
    Int,
    String,
}

#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
pub enum Data {
    Bool(Vec<Datum<bool>>),
    Int(Vec<Datum<usize>>),
    String(Vec<Datum<String>>),
}

impl Data {
    pub fn get(&self, index: usize) -> Option<GenericDatum> {
        match *self {
            Data::Bool(ref data) => {
                data.get(index)
                    .and_then(|datum| {
                        Some(GenericDatum::new(datum.id, Value::Bool(datum.value), datum.time))
                    })
            }
            Data::Int(ref data) => {
                data.get(index)
                    .and_then(|datum| {
                        Some(GenericDatum::new(datum.id, Value::Int(datum.value), datum.time))
                    })
            }
            Data::String(ref data) => {
                data.get(index)
                    .and_then(|datum| {
                        Some(GenericDatum::new(datum.id,
                                               Value::String(datum.value.clone()),
                                               datum.time))
                    })
            }
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            Data::Bool(ref data) => data.len(),
            Data::Int(ref data) => data.len(),
            Data::String(ref data) => data.len(),
        }
    }

    fn sort(&mut self) {
        fn sort_by_time<T>(a: &Datum<T>, b: &Datum<T>) -> cmp::Ordering {
            a.time.cmp(&b.time)
        };

        match *self {
            Data::Bool(ref mut data) => data.sort_by(sort_by_time),
            Data::Int(ref mut data) => data.sort_by(sort_by_time),
            Data::String(ref mut data) => data.sort_by(sort_by_time),
        };
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Encoding(serialize::EncodingError),
    Decoding(serialize::DecodingError),
    NameAlreadyTake(ColumnName),
    NameNotFound(ColumnName),
    ParseError(ColumnName, ColumnType),
}

pub type Ids = HashSet<usize>;

#[derive(Debug, RustcEncodable, RustcDecodable)]
pub struct Column {
    pub name: ColumnName,
    pub data: Data,
    time_index: Option<[usize; 5]>,
}

impl Column {
    fn new(name: ColumnName, t: ColumnType) -> Self {
        let data = match t {
            ColumnType::Bool => Data::Bool(vec![]),
            ColumnType::Int => Data::Int(vec![]),
            ColumnType::String => Data::String(vec![]),
        };
        Column {
            name: name,
            data: data,
            time_index: None,
        }
    }

    fn sort(&mut self) {
        self.data.sort()
    }

    #[allow(needless_range_loop)]
    fn index_by_time(&mut self) {
        let len = self.data.len();
        let mut index = [0, 0, 0, 0, 0];

        if len < 5 {
            return;
        }

        let increment = len / 5;
        for i in 0..5 {
            index[i] = self.data.get(increment * i).unwrap().time;
        }

        self.time_index = Some(index);
    }

    fn add_datum(&mut self, id: usize, value: String, time: usize) -> Result<(), Error> {
        match self.data {
            Data::Bool(ref mut data) => {
                match value.parse::<bool>() {
                    Ok(v) => data.push(Datum::new(id, v, time)),
                    Err(_) => return Err(Error::ParseError(self.name.clone(), ColumnType::Bool)),
                }
            }
            Data::Int(ref mut data) => {
                match value.parse::<usize>() {
                    Ok(v) => data.push(Datum::new(id, v, time)),
                    _ => return Err(Error::ParseError(self.name.clone(), ColumnType::Int)),
                }
            }
            Data::String(ref mut data) => data.push(Datum::new(id, value, time)),
        };
        Ok(())
    }
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
pub struct Db {
    pub cols: HashMap<ColumnName, Column>,
    pub ids: HashMap<String, Ids>,
    entity_count: usize,
}

impl Db {
    fn new() -> Db {
        Db {
            cols: HashMap::new(),
            ids: HashMap::new(),
            entity_count: 0,
        }
    }

    pub fn from_file(file_path: &str) -> Result<Db, Error> {
        if !path::Path::new(file_path).exists() {
            try!(File::create(file_path));
            return Ok(Db::new());
        }

        let file = try!(File::open(file_path));
        let reader = io::BufReader::new(file);
        let mut decoder = ZlibDecoder::new(reader);
        let decoded = try!(serialize::decode_from(&mut decoder, SizeLimit::Infinite));

        Ok(decoded)
    }

    pub fn write(&self, filename: &str) -> Result<(), Error> {
        let path = path::Path::new(filename);
        let writer = io::BufWriter::new(try!(File::create(path)));
        let mut encoder = ZlibEncoder::new(writer, Compression::Fast);

        try!(bincode::rustc_serialize::encode_into(self, &mut encoder, SizeLimit::Infinite));
        Ok(())
    }

    pub fn next_id(&mut self, table: &str) -> usize {
        let ids = self.ids.get_mut(table).expect(&format!("Cannot find table {}", table));
        let next = self.entity_count;

        self.entity_count += 1;
        ids.insert(next);

        next
    }

    pub fn add_column(&mut self, name: ColumnName, t: ColumnType) -> Result<(), Error> {
        match self.cols.get(&name) {
            Some(_) => Err(Error::NameAlreadyTake(name)),
            None => {
                self.cols.insert(name.clone(), Column::new(name.clone(), t));
                self.ids.insert(name.table, Ids::new());
                Ok(())
            }
        }
    }

    pub fn add_datum(&mut self, name: &ColumnName, id: usize, value: String, time: usize)
                     -> Result<(), Error> {
        let mut col = match self.cols.get_mut(name) {
            Some(c) => c,
            None => return Err(Error::NameNotFound(name.to_owned())),
        };
        col.add_datum(id, value, time)
    }

    #[allow(for_kv_map)]
    pub fn optimize_columns(&mut self) {
        for (_, col) in &mut self.cols {
            col.sort();
            col.index_by_time()
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<serialize::EncodingError> for Error {
    fn from(err: serialize::EncodingError) -> Error {
        Error::Encoding(err)
    }
}

impl From<serialize::DecodingError> for Error {
    fn from(err: serialize::DecodingError) -> Error {
        Error::Decoding(err)
    }
}
