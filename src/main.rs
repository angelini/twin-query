#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro

extern crate clap;
extern crate linenoise;

use clap::{App, SubCommand};
use std::fmt;
use std::fs::File;
use std::collections::HashMap;

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
struct Column<T> {
    name: ColumnName,
    values: Vec<Entry<T>>,
}

impl<T> Column<T> {
    fn new(name: ColumnName) -> Column<T> {
        Column {
            name: name,
            values: vec![],
        }
    }
}

#[derive(Debug, PartialEq)]
enum ColumnType {
    Bool,
    Int,
    String,
}

#[derive(Debug)]
struct Db {
    map: HashMap<ColumnName, (usize, ColumnType)>,

    bool_cols: Vec<Column<bool>>,
    int_cols: Vec<Column<usize>>,
    string_cols: Vec<Column<String>>,
}

impl Db {
    fn new() -> Db {
        Db {
            map: HashMap::new(),
            bool_cols: vec![],
            int_cols: vec![],
            string_cols: vec![],
        }
    }

    fn add_column(&mut self, name: ColumnName, t: ColumnType) {
        match t {
            ColumnType::Bool => {
                let index = self.bool_cols.len();
                self.map.insert(name.clone(), (index, t));
                self.bool_cols.push(Column::new(name));
            }
            ColumnType::Int => {
                let index = self.int_cols.len();
                self.map.insert(name.clone(), (index, t));
                self.int_cols.push(Column::new(name));
            }
            ColumnType::String => {
                let index = self.string_cols.len();
                self.map.insert(name.clone(), (index, t));
                self.string_cols.push(Column::new(name));
            }
        }
    }

    fn add_bool_entry(&mut self, name: &ColumnName, entry: Entry<bool>) {
        let &(index, ref t) = self.map.get(name).unwrap();
        assert!(*t == ColumnType::Bool);
        self.bool_cols[index].values.push(entry)
    }

    fn add_int_entry(&mut self, name: &ColumnName, entry: Entry<usize>) {
        let &(index, ref t) = self.map.get(name).unwrap();
        assert!(*t == ColumnType::Int);
        self.int_cols[index].values.push(entry)
    }

    fn add_string_entry(&mut self, name: &ColumnName, entry: Entry<String>) {
        let &(index, ref t) = self.map.get(name).unwrap();
        assert!(*t == ColumnType::String);
        self.string_cols[index].values.push(entry)
    }
}

impl fmt::Display for Db {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "\n"));

        for col in &self.bool_cols {
            try!(write!(f, "{} ", col.name));
        };
        for col in &self.int_cols {
            try!(write!(f, "{} ", col.name));
        };
        for col in &self.string_cols {
            try!(write!(f, "{} ", col.name));
        };
        try!(write!(f, "\n-----------------------\n"));

        for i in 0..10 {
            let mut wrote = false;

            for col in &self.bool_cols {
                if col.values.len() > i {
                    try!(write!(f, "{} ", col.values[i]));
                    wrote = true;
                }
            };
            for col in &self.int_cols {
                if col.values.len() > i {
                    try!(write!(f, "{} ", col.values[i]));
                    wrote = true;
                }
            };
            for col in &self.string_cols {
                if col.values.len() > i {
                    try!(write!(f, "{} ", col.values[i]));
                    wrote = true;
                }
            };

            if wrote { try!(write!(f, "\n")) }
        }
        Ok(())
    }
}

fn sample_db() -> Db {
    let mut db = Db::new();

    let a = ColumnName::new("table", "a");
    let b = ColumnName::new("table", "b");

    db.add_column(a.clone(), ColumnType::Int);
    db.add_column(b.clone(), ColumnType::String);

    db.add_int_entry(&a, Entry::new(1, 1, 0));
    db.add_int_entry(&a, Entry::new(2, 2, 0));
    db.add_int_entry(&a, Entry::new(3, 3, 0));

    db.add_string_entry(&b, Entry::<String>::new(1, "first".to_owned(), 0));
    db.add_string_entry(&b, Entry::new(2, "second".to_owned(), 0));
    db.add_string_entry(&b, Entry::new(3, "third".to_owned(), 0));

    db
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
            Err(e) => println!("{}", e)
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
