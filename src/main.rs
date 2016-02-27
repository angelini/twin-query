#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro
#![allow(len_without_is_empty)]

extern crate bincode;
extern crate clap;
extern crate csv;
extern crate flate2;
extern crate petgraph;
extern crate prettytable;
extern crate rl_sys;
extern crate rustc_serialize;
extern crate toml;

mod data;
mod exec;
mod query;
mod repl;

use clap::{App, SubCommand};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use data::{ColumnName, ColumnType, Db};
use repl::start_repl;

#[derive(Debug, RustcEncodable, RustcDecodable)]
struct Schema {
    table: String,
    columns: HashMap<String, String>,
    time_column: String,
    csv_ordering: Vec<String>,
}

fn read_schema(schema_path: &str) -> Schema {
    let mut contents = String::new();
    File::open(schema_path)
        .and_then(|mut f| f.read_to_string(&mut contents))
        .unwrap();

    toml::decode_str(&contents).unwrap()
}

fn add_to_db(file_path: &str, schema_path: &str, csv_path: &str) {
    let mut db = Db::from_file(file_path).expect("Cannot load db from file");

    let schema = read_schema(schema_path);
    println!("schema: {:?}", schema);

    let time_index = match schema.csv_ordering.iter().position(|c| c == &schema.time_column) {
        Some(i) => i,
        None => panic!("Time index not found"),
    };

    for (column_name, column_type) in schema.columns {
        let t = match column_type.as_str() {
            "Bool" => ColumnType::Bool,
            "Int" => ColumnType::Int,
            "String" => ColumnType::String,
            _ => panic!("Invalid column type"),
        };
        db.add_column(ColumnName::new(schema.table.clone(), column_name), t)
          .expect("Could not add column");
    }

    let mut rdr = csv::Reader::from_file(csv_path)
                      .and_then(|r| Ok(r.has_headers(false)))
                      .unwrap();

    for row in rdr.records().map(|r| r.unwrap()) {
        let eid = db.next_eid();
        let time = row.get(time_index).unwrap().parse::<usize>().unwrap();

        for (column_name, value) in schema.csv_ordering.iter().zip(row.iter()) {
            let name = ColumnName::new(schema.table.clone(), column_name.to_owned());
            db.add_entry(&name, eid, value.to_owned(), time).expect("Could not add to db");
        }
    }

    db.write(file_path).expect("Could not write db to disk");
}

fn main() {
    let matches = App::new("twin-query")
                      .version("0.1")
                      .subcommand(SubCommand::with_name("query")
                                      .arg_from_usage("<FILE> 'Path to DB file'"))
                      .subcommand(SubCommand::with_name("add")
                                      .arg_from_usage("<FILE> 'Path to DB file'")
                                      .arg_from_usage("<SCHEMA> 'Path to schema file'")
                                      .arg_from_usage("<DATA> 'Path to data, stored in CSV'"))
                      .get_matches();

    if let Some(matches) = matches.subcommand_matches("query") {
        start_repl(matches.value_of("FILE").unwrap());
    }

    if let Some(matches) = matches.subcommand_matches("add") {
        add_to_db(matches.value_of("FILE").unwrap(),
                  matches.value_of("SCHEMA").unwrap(),
                  matches.value_of("DATA").unwrap());
    }
}
