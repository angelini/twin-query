#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro
#![allow(len_without_is_empty)]

extern crate bincode;
extern crate clap;
extern crate csv;
extern crate flate2;
extern crate linenoise;
extern crate petgraph;
extern crate prettytable;
extern crate rustc_serialize;
extern crate toml;

mod data;
mod query;
mod exec;

use clap::{App, SubCommand};
use prettytable::Table;
use prettytable::row::Row;
use prettytable::cell::Cell;
use prettytable::format;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use data::{ColumnName, ColumnType, Db, Entries};
use query::Plan;

peg_file! grammar("grammar.rustpeg");

fn read_query_raw() -> String {
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

fn print_table(cols: Vec<(&ColumnName, &Entries)>, limit: usize) {
    let mut cols = cols;
    cols.sort_by(|a, b| format!("{}", a.0).cmp(&format!("{}", b.0)));

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    let col_names = cols.iter()
                        .map(|&(ref name, _)| Cell::new(&format!("{}", name)))
                        .collect::<Vec<Cell>>();
    table.set_titles(Row::new(col_names));

    let max_col_len = cols.iter().fold(0, |acc, &(_, ref entries)| cmp::max(acc, entries.len()));

    for i in 0..cmp::min(limit, max_col_len) {
        let mut row = vec![];
        for &(_, ref entries) in &cols {
            let entry = entries.get(i).unwrap();
            row.push(Cell::new(&format!("{}", entry)));
        }
        table.add_row(Row::new(row));
    }

    table.printstd();
}


fn start_repl(path: &str) {
    linenoise::history_set_max_len(1000);
    linenoise::history_load(".history");

    let db = Db::from_file(path).expect("Cannot load db from file");

    loop {
        println!("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
        let query_raw = read_query_raw();

        linenoise::history_save(".history");
        linenoise::history_add(&query_raw);

        let query_lines = grammar::query(&query_raw);

        let plan = match query_lines {
            Ok(lines) => {
                let p = Plan::new(lines);
                let valid = p.is_valid();

                if valid.is_err() {
                    println!("{:?}", valid);
                    continue;
                }
                p
            }
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        println!("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
        println!("{}", plan);

        let names_and_entries = db.cols
                                  .iter()
                                  .map(|(name, col)| (name, &col.entries))
                                  .collect::<Vec<(&ColumnName, &Entries)>>();
        print_table(names_and_entries, 20);
        println!("");

        match exec::exec(&db, &plan) {
            Ok(entries) => {
                print_table(entries.iter()
                                   .map(|&(ref n, ref e)| (n, e))
                                   .collect(),
                            20)
            }
            Err(e) => {
                println!("{:?}", e);
                continue;
            }
        };
    }
}

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
