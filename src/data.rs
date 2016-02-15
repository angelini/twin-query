use bincode;
use bincode::SizeLimit;
use bincode::rustc_serialize as serialize;
use flate2::write::ZlibEncoder;
use flate2::read::ZlibDecoder;
use flate2::Compression;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::path;

#[derive(Debug, Clone)]
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

#[derive(Debug, RustcEncodable, RustcDecodable)]
struct Entry<T> {
    eid: usize,
    value: T,
    time: usize,
}

impl<T> Entry<T> {
    fn new(eid: usize, value: T, time: usize) -> Entry<T> {
        Entry {
            eid: eid,
            value: value,
            time: time,
        }
    }
}

impl<T: fmt::Display> fmt::Display for Entry<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.eid, self.value, self.time)
    }
}

#[derive(Debug)]
pub struct EntryValue {
    eid: usize,
    value: Value,
    time: usize,
}

impl EntryValue {
    pub fn new(eid: usize, value: Value, time: usize) -> EntryValue {
        EntryValue {
            eid: eid,
            value: value,
            time: time,
        }
    }
}

impl fmt::Display for EntryValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.eid, self.value, self.time)
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

#[derive(Debug, RustcEncodable, RustcDecodable)]
enum Entries {
    Bool(Vec<Entry<bool>>),
    Int(Vec<Entry<usize>>),
    String(Vec<Entry<String>>),
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
struct Column {
    name: ColumnName,
    entries: Entries,
}

impl Column {
    fn new(name: ColumnName, t: ColumnType) -> Column {
        let entries = match t {
            ColumnType::Bool => Entries::Bool(vec![]),
            ColumnType::Int => Entries::Int(vec![]),
            ColumnType::String => Entries::String(vec![]),
        };
        Column {
            name: name,
            entries: entries,
        }
    }

    fn len(&self) -> usize {
        match self.entries {
            Entries::Bool(ref v) => v.len(),
            Entries::Int(ref v) => v.len(),
            Entries::String(ref v) => v.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, index: usize) -> Option<EntryValue> {
        match self.entries {
            Entries::Bool(ref entries) => {
                entries.get(index)
                       .and_then(|entry| {
                           Some(EntryValue::new(entry.eid, Value::Bool(entry.value), entry.time))
                       })
            }
            Entries::Int(ref entries) => {
                entries.get(index)
                       .and_then(|entry| {
                           Some(EntryValue::new(entry.eid, Value::Int(entry.value), entry.time))
                       })
            }
            Entries::String(ref entries) => {
                entries.get(index)
                       .and_then(|entry| {
                           Some(EntryValue::new(entry.eid,
                                                Value::String(entry.value.clone()),
                                                entry.time))
                       })
            }
        }
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

#[derive(Debug, RustcEncodable, RustcDecodable)]
pub struct Db {
    cols: HashMap<ColumnName, Column>,
    entity_count: usize,
}

impl Db {
    fn new() -> Db {
        Db {
            cols: HashMap::new(),
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

    pub fn next_eid(&mut self) -> usize {
        let next = self.entity_count;
        self.entity_count += 1;
        next
    }

    pub fn add_column(&mut self, name: ColumnName, t: ColumnType) -> Result<(), Error> {
        match self.cols.get(&name) {
            Some(_) => Err(Error::NameAlreadyTake(name)),
            None => {
                self.cols.insert(name.clone(), Column::new(name, t));
                Ok(())
            }
        }
    }

    pub fn add_entry(&mut self, name: &ColumnName, eid: usize, value: String, time: usize)
                     -> Result<(), Error> {
        let mut col = match self.cols.get_mut(name) {
            Some(c) => c,
            None => return Err(Error::NameNotFound(name.to_owned())),
        };

        match col.entries {
            Entries::Bool(ref mut entries) => {
                match value.parse::<bool>() {
                    Ok(v) => entries.push(Entry::new(eid, v, time)),
                    Err(_) => return Err(Error::ParseError(name.to_owned(), ColumnType::Bool)),
                }
            }
            Entries::Int(ref mut entries) => {
                match value.parse::<usize>() {
                    Ok(v) => entries.push(Entry::new(eid, v, time)),
                    _ => return Err(Error::ParseError(name.to_owned(), ColumnType::Int)),
                }
            }
            Entries::String(ref mut entries) => entries.push(Entry::new(eid, value, time)),
        };
        Ok(())
    }
}

impl fmt::Display for Db {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "\n"));

        for (name, _) in &self.cols {
            try!(write!(f, "{} ", name));
        }
        try!(write!(f, "\n-----------------------\n"));

        for i in 0..10 {
            let mut wrote = false;
            for (_, col) in &self.cols {
                if col.len() > i {
                    try!(write!(f, "{} ", col.get(i).unwrap()));
                    wrote = true;
                }
            }
            if wrote {
                try!(write!(f, "\n"))
            }
        }
        Ok(())
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
