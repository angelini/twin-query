use prettytable::format;
use prettytable::Table;
use prettytable::row::Row;
use prettytable::cell::Cell;
use rl_sys::readline;
use rl_sys::history::{listmgmt, mgmt, histfile};
use std::cmp;
use std::path::Path;
use std::process;
use std::str::FromStr;
use time;

use data::{ColumnName, Db, Data};
use exec;
use plan::Plan;

fn read_query_raw() -> String {
    let mut query = "".to_owned();

    loop {
        match readline::readline("") {
            Ok(Some(ref line)) => {
                if line == "" {
                    let len = query.len();
                    if len > 0 {
                        query.truncate(len - 1);
                    }
                    return query;
                } else if line == "exit" {
                    return line.to_owned();
                }
                query = query + &line + "\n";
            }
            _ => panic!("Cannot read line from console"),
        }
    }
}

pub fn print_table(cols: Vec<(&ColumnName, &Data)>, limit: usize) {
    let mut cols = cols;
    cols.sort_by(|a, b| format!("{}", a.0).cmp(&format!("{}", b.0)));

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    let col_names = cols.iter()
                        .map(|&(ref name, _)| Cell::new(&format!("{}", name)))
                        .collect::<Vec<Cell>>();
    table.set_titles(Row::new(col_names));

    let max_col_len = cols.iter().fold(0, |acc, &(_, ref data)| cmp::max(acc, data.len()));

    for i in 0..cmp::min(limit, max_col_len) {
        let mut row = vec![];
        for &(_, ref data) in &cols {
            match data.get(i) {
                Some(d) => row.push(Cell::new(&format!("{}", d))),
                None => row.push(Cell::new(" ")),
            }
        }
        table.add_row(Row::new(row));
    }

    table.printstd();
}

pub fn start_repl(path: &str) {
    let history_path = Path::new("./.history");
    let mut start = time::precise_time_s();
    let db = Db::from_file(path).expect("Failed to load db from file");
    println!("\nload time: {:.4}", time::precise_time_s() - start);

    mgmt::init();
    if history_path.exists() {
        histfile::read(Some(history_path)).expect("Failed to read history");
    }

    loop {
        println!("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

        let query_raw = read_query_raw();
        if query_raw == "exit" {
            mgmt::cleanup();
            process::exit(0);
        };

        listmgmt::add(&query_raw).expect("Failed to save history");
        histfile::write(Some(history_path)).expect("Failed to write history");

        let plan = match Plan::from_str(&query_raw) {
            Ok(plan) => plan,
            Err(e) => {
                println!("{:?}", e);
                continue;
            }
        };

        println!("{}", plan);

        start = time::precise_time_s();
        match exec::exec(&db, &plan) {
            Ok(data) => {
                println!("exec time: {:.4}\n", time::precise_time_s() - start);
                print_table(data.iter()
                                .map(|&(ref n, ref e)| (n, e))
                                .collect(),
                            2000)
            }
            Err(e) => {
                println!("{:?}", e);
                continue;
            }
        };
    }
}
