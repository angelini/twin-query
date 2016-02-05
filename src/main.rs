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
struct Entry {
    eid: usize,
    value: Value,
    time: usize,
}

impl Entry {
    fn new(eid: usize, value: Value, time: usize) -> Entry {
        Entry {
            eid: eid,
            value: value,
            time: time,
        }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.eid, self.value, self.time)
    }
}

#[derive(Debug)]
struct Column {
    name: ColumnName,
    values: Vec<Entry>,
}

impl Column {
    fn new(name: ColumnName) -> Column {
        Column {
            name: name,
            values: vec![],
        }
    }
}

#[derive(Debug)]
struct Db {
    map: HashMap<ColumnName, usize>,
    cols: Vec<Column>,
}

impl Db {
    fn new() -> Db {
        Db {
            map: HashMap::new(),
            cols: vec![],
        }
    }

    fn add_column(&mut self, name: ColumnName) {
        let index = self.cols.len();
        self.map.insert(name.clone(), index);
        self.cols.push(Column::new(name));
    }

    fn add_entry(&mut self, name: &ColumnName, entry: Entry) {
        let index = self.map.get(name).unwrap();
        let mut col = self.cols.get_mut(*index).unwrap();
        let mut vals = &mut col.values;
        vals.push(entry);
    }
}

impl fmt::Display for Db {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "\n"));

        for col in &self.cols {
            try!(write!(f, "{} ", col.name));
        }
        try!(write!(f, "\n-----------------------\n"));

        for i in 0..10 {
            let mut wrote = false;
            for col in &self.cols {
                if col.values.len() > i {
                    try!(write!(f, "{} ", col.values[i]));
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

    db.add_column(a.clone());
    db.add_column(b.clone());

    db.add_entry(&a, Entry::new(1, Value::Int(1), 0));
    db.add_entry(&a, Entry::new(2, Value::Int(2), 0));
    db.add_entry(&a, Entry::new(3, Value::Int(3), 0));

    db.add_entry(&b, Entry::new(1, Value::String("first".to_owned()), 0));
    db.add_entry(&b, Entry::new(2, Value::String("second".to_owned()), 0));
    db.add_entry(&b, Entry::new(3, Value::String("third".to_owned()), 0));

    db
}

fn sample_entries() -> Vec<Entry> {
    return vec![Entry::new(1, Value::Int(0), 0),
                Entry::new(2, Value::String("foo".to_owned()), 0),
                Entry::new(3, Value::Bool(true), 0)];
}

fn read_query() -> String {
    let mut query = "".to_owned();

    loop {
        let line = linenoise::input("").unwrap();
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
