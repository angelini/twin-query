#![feature(plugin)]
#![plugin(peg_syntax_ext)]
#![plugin(clippy)]
#![allow(len_zero)] // for pegile macro
#![allow(len_without_is_empty)]

extern crate bincode;
extern crate clap;
extern crate csv;
extern crate crossbeam;
extern crate flate2;
extern crate petgraph;
extern crate prettytable;
extern crate rl_sys;
extern crate rustc_serialize;
extern crate time;
extern crate toml;

mod data;
mod exec;
mod insert;
mod query;
mod repl;

use clap::{App, SubCommand};
use std::str::FromStr;

use data::Db;
use query::Plan;

fn exec_query(file_path: &str, query_raw: &str) {
    let query = query_raw.replace("\\n", "\n");

    let db = Db::from_file(file_path).expect("Failed to load db from file");
    let plan = Plan::from_str(&query).expect("Failed to parse query");
    let result = exec::exec(&db, &plan).expect("Failed to exec query");

    repl::print_table(result.iter()
                            .map(|&(ref n, ref e)| (n, e))
                            .collect(),
                      2000);
}

fn main() {
    let matches = App::new("twin-query")
                      .version("0.1")
                      .subcommand(SubCommand::with_name("repl")
                                      .arg_from_usage("<FILE> 'Path to DB file'"))
                      .subcommand(SubCommand::with_name("query")
                                      .arg_from_usage("<FILE> 'Path to DB file'")
                                      .arg_from_usage("<QUERY> 'Full query string'"))
                      .subcommand(SubCommand::with_name("add")
                                      .arg_from_usage("<FILE> 'Path to DB file'")
                                      .arg_from_usage("<SCHEMA> 'Path to schema file'")
                                      .arg_from_usage("<DATA> 'Path to data, stored in CSV'"))
                      .get_matches();

    if let Some(matches) = matches.subcommand_matches("repl") {
        repl::start_repl(matches.value_of("FILE").unwrap());
    }

    if let Some(matches) = matches.subcommand_matches("query") {
        let vals: Vec<&str> = matches.values_of("QUERY").unwrap().collect();
        exec_query(matches.value_of("FILE").unwrap(), &vals.join(","));
    }

    if let Some(matches) = matches.subcommand_matches("add") {
        insert::add_to_db(matches.value_of("FILE").unwrap(),
                          matches.value_of("SCHEMA").unwrap(),
                          matches.value_of("DATA").unwrap());
    }
}
