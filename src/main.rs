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
mod insert;
mod query;
mod repl;

use clap::{App, SubCommand};

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
        repl::start_repl(matches.value_of("FILE").unwrap());
    }

    if let Some(matches) = matches.subcommand_matches("add") {
        insert::add_to_db(matches.value_of("FILE").unwrap(),
                          matches.value_of("SCHEMA").unwrap(),
                          matches.value_of("DATA").unwrap());
    }
}
