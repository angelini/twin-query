#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro

extern crate bincode;
extern crate clap;
extern crate flate2;
extern crate linenoise;
extern crate rustc_serialize;

mod data;

use clap::{App, SubCommand};
use std::fs::File;

use data::{ColumnName, ColumnType, Db, EntryValue, Error, Value};

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

fn sample_db() -> Result<Db, Error> {
    let mut db = Db::new();

    let a = ColumnName::new("table", "a");
    let b = ColumnName::new("table", "b");
    let c = ColumnName::new("table", "c");

    try!(db.add_column(a.clone(), ColumnType::Bool));
    try!(db.add_column(b.clone(), ColumnType::Int));
    try!(db.add_column(c.clone(), ColumnType::String));

    try!(db.add_entry(&a, EntryValue::new(1, Value::Bool(true), 0)));
    try!(db.add_entry(&a, EntryValue::new(2, Value::Bool(true), 0)));
    try!(db.add_entry(&a, EntryValue::new(3, Value::Bool(false), 0)));

    try!(db.add_entry(&b, EntryValue::new(1, Value::Int(1), 0)));
    try!(db.add_entry(&b, EntryValue::new(2, Value::Int(2), 0)));
    try!(db.add_entry(&b, EntryValue::new(3, Value::Int(3), 0)));

    try!(db.add_entry(&c, EntryValue::new(1, Value::String("first".to_owned()), 0)));
    try!(db.add_entry(&c,
                      EntryValue::new(2, Value::String("second".to_owned()), 0)));
    try!(db.add_entry(&c, EntryValue::new(3, Value::String("third".to_owned()), 0)));

    Ok(db)
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

    let db = Db::from_file(path).unwrap();

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

fn create_db(file_path: &str) {
    File::create(file_path).unwrap();
}

fn add_to_db(file_path: &str, schema_path: &str, csv_path: &str) {
    match sample_db() {
        Ok(db) => db.write(file_path).unwrap(),
        Err(e) => println!("e: {:?}", e),
    }
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
        add_to_db(matches.value_of("FILE").unwrap(),
                  matches.value_of("SCHEMA").unwrap(),
                  matches.value_of("DATA").unwrap());
    }
}
