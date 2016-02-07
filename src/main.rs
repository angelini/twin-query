#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro

extern crate clap;
extern crate linenoise;

mod data;

use clap::{App, SubCommand};
use std::fs::File;

use data::{ColumnName, ColumnType, Db, EntryValue, Value};

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
