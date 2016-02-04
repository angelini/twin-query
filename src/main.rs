#![feature(plugin)]
#![plugin(peg_syntax_ext)]

extern crate clap;
extern crate linenoise;

use std::fs::File;
use clap::{App, SubCommand};

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<String>),
    Where(String, String),
}

peg_file! grammar("grammar.rustpeg");

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

    loop {
        println!("---");
        let query_raw = read_query();
        println!("query_raw: {:?}", query_raw);
        let query = grammar::query(&query_raw);
        println!("query: {:?}", query);
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
        println!("in create {:?} {:?} {:?}",
                 matches.value_of("FILE"),
                 matches.value_of("SCHEMA"),
                 matches.value_of("DATA"));
    }
}
