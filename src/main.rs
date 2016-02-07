#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro

extern crate clap;
extern crate linenoise;

use clap::{App, SubCommand};
use std::fmt;
use std::collections::HashMap;
use std::fs::File;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ColumnName {
    table: String,
    column: String,
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
pub enum Comparator {
    Equal,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<ColumnName>),
    Where(ColumnName, Comparator, ColumnName),
}

peg_file! grammar("grammar.rustpeg");

#[derive(Debug)]
enum Value {
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

#[derive(Debug)]
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
struct EntryValue {
    eid: usize,
    value: Value,
    time: usize,
}

impl EntryValue {
    fn new(eid: usize, value: Value, time: usize) -> EntryValue {
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

enum ColumnType {
    Bool,
    Int,
    String,
}

#[derive(Debug)]
enum Entries {
    Bool(Vec<Entry<bool>>),
    Int(Vec<Entry<usize>>),
    String(Vec<Entry<String>>),
}

#[derive(Debug)]
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

    fn get(&self, index: usize) -> Option<EntryValue> {
        match self.entries {
            Entries::Bool(ref entries) => {
                match entries.get(index) {
                    Some(entry) => Some(EntryValue::new(entry.eid, Value::Bool(entry.value), entry.time)),
                    None => None
                }
            }
            Entries::Int(ref entries) => {
                match entries.get(index) {
                    Some(entry) => Some(EntryValue::new(entry.eid, Value::Int(entry.value), entry.time)),
                    None => None
                }
            }
            Entries::String(ref entries) => {
                match entries.get(index) {
                    Some(entry) => Some(EntryValue::new(entry.eid, Value::String(entry.value.clone()), entry.time)),
                    None => None
                }
            }
        }
    }
}

#[derive(Debug)]
struct Db {
    cols: HashMap<ColumnName, Column>,
}

impl Db {
    fn new() -> Db {
        Db {
            cols: HashMap::new(),
        }
    }

    fn add_column(&mut self, name: ColumnName, t: ColumnType) {
        match self.cols.get(&name) {
            Some(_) => panic!(format!("Column already exists: {}", name)),
            None => self.cols.insert(name.clone(), Column::new(name, t))
        };
    }

    fn add_entry(&mut self, name: &ColumnName, entry: EntryValue) {
        let mut col = self.cols.get_mut(name).expect(&format!("Cannot find column: {}", name));

        match col.entries {
            Entries::Bool(ref mut entries) => {
                match entry.value {
                    Value::Bool(v) => entries.push(Entry::new(entry.eid, v, entry.time)),
                    _ => panic!("Wrong type for column: {}, expected Bool", name)
                }
            }
            Entries::Int(ref mut entries) => {
                match entry.value {
                    Value::Int(v) => entries.push(Entry::new(entry.eid, v, entry.time)),
                    _ => panic!("Wrong type for column: {}, expected Int", name)
                }
            }
            Entries::String(ref mut entries) => {
                match entry.value {
                    Value::String(v) => entries.push(Entry::new(entry.eid, v, entry.time)),
                    _ => panic!("Wrong type for column: {}, expected String", name)
                }
            }
        };
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

fn sample_db() -> Db {
    let mut db = Db::new();

    let a = ColumnName::new("table", "a");
    let b = ColumnName::new("table", "b");
    let c = ColumnName::new("table", "c");

    db.add_column(a.clone(), ColumnType::Bool);
    db.add_column(b.clone(), ColumnType::Int);
    db.add_column(c.clone(), ColumnType::String);

    db.add_entry(&a, EntryValue::new(1, Value::Bool(true), 0));
    db.add_entry(&a, EntryValue::new(2, Value::Bool(true), 0));
    db.add_entry(&a, EntryValue::new(3, Value::Bool(false), 0));

    db.add_entry(&b, EntryValue::new(1, Value::Int(1), 0));
    db.add_entry(&b, EntryValue::new(2, Value::Int(2), 0));
    db.add_entry(&b, EntryValue::new(3, Value::Int(3), 0));

    db.add_entry(&c, EntryValue::new(1, Value::String("first".to_owned()), 0));
    db.add_entry(&c, EntryValue::new(2, Value::String("second".to_owned()), 0));
    db.add_entry(&c, EntryValue::new(3, Value::String("third".to_owned()), 0));

    db
}

fn read_query() -> String {
    let mut query = "".to_owned();

    loop {
        let line = linenoise::input("").expect("Cannot read line from console");
        if line == "" {
            let len = query.len();
            if len > 0 {
                query.truncate(len - 1);
            }
            return query;
        }
        query = query + &line + "\n";
    }
}

fn start_repl(path: &str) {
    linenoise::history_set_max_len(1000);
    linenoise::history_load(".history");

    let db = sample_db();

    loop {
        println!("---");
        let query_raw = read_query();
        let query = grammar::query(&query_raw);

        linenoise::history_save(".history");
        linenoise::history_add(&query_raw);

        match query {
            Ok(q) => {
                println!("query: {:?}", q);
                println!("{}", db);
            }
            Err(e) => println!("{}", e),
        }
    }
}

fn create_db(path: &str) {
    File::create(path);
}

fn main() {
    let matches = App::new("twin-query")
                      .version("0.1")
                      .subcommand(SubCommand::with_name("query")
                                      .arg_from_usage("<FILE> 'Path to DB file'"))
                      .subcommand(SubCommand::with_name("create")
                                      .arg_from_usage("<FILE> 'Path to new DB file'"))
                      .subcommand(SubCommand::with_name("add")
                                      .arg_from_usage("<FILE> 'Path to DB file'")
                                      .arg_from_usage("<SCHEMA> 'Path to schema file'")
                                      .arg_from_usage("<DATA> 'Path to data, stored in CSV'"))
                      .get_matches();

    if let Some(matches) = matches.subcommand_matches("query") {
        start_repl(matches.value_of("FILE").unwrap());
    }

    if let Some(matches) = matches.subcommand_matches("create") {
        create_db(matches.value_of("FILE").unwrap());
    }

    if let Some(matches) = matches.subcommand_matches("add") {
        println!("in add {:?} {:?} {:?}",
                 matches.value_of("FILE"),
                 matches.value_of("SCHEMA"),
                 matches.value_of("DATA"));
    }
}
