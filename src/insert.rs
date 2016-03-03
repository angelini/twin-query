use csv;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use toml;

use data::{ColumnName, ColumnType, Db};

#[derive(Debug, RustcEncodable, RustcDecodable)]
struct RawSchema {
    table: String,
    columns: HashMap<String, String>,
    time_column: String,
    csv_ordering: Vec<String>,
}

#[derive(Debug)]
struct Schema {
    table: String,
    columns: HashMap<ColumnName, ColumnType>,
    time_column: ColumnName,
    csv_ordering: Vec<ColumnName>,
}

impl Schema {
    fn new(raw: RawSchema) -> Schema {
        Schema {
            table: raw.table.to_owned(),
            columns: Self::column_names_and_types(&raw.table, raw.columns),
            time_column: ColumnName::new(raw.table.to_owned(), raw.time_column),
            csv_ordering: Self::ordering(&raw.table, raw.csv_ordering),
        }
    }

    fn time_index(&self) -> usize {
        match self.csv_ordering.iter().position(|c| c == &self.time_column) {
            Some(i) => i,
            None => panic!("Time index not found"),
        }
    }

    fn column_names_and_types(table: &str, raw: HashMap<String, String>)
                              -> HashMap<ColumnName, ColumnType> {
        raw.iter()
           .map(|(col_name, col_type)| {
               let t = match col_type.as_str() {
                   "Bool" => ColumnType::Bool,
                   "Int" => ColumnType::Int,
                   "String" => ColumnType::String,
                   _ => panic!("Invalid column type"),
               };
               let name = ColumnName::new(table.to_owned(), col_name.to_owned());
               (name, t)
           })
           .collect()
    }

    fn ordering(table: &str, raw: Vec<String>) -> Vec<ColumnName> {
        raw.iter().map(|col| ColumnName::new(table.to_owned(), col.to_owned())).collect()
    }
}

fn read_schema(schema_path: &str) -> Schema {
    let mut contents = String::new();
    File::open(schema_path)
        .and_then(|mut f| f.read_to_string(&mut contents))
        .unwrap();

    Schema::new(toml::decode_str(&contents).unwrap())
}

pub fn add_to_db(file_path: &str, schema_path: &str, csv_path: &str) {
    let mut db = Db::from_file(file_path).expect("Cannot load db from file");

    let schema = read_schema(schema_path);
    let time_index = schema.time_index();

    for (column_name, column_type) in schema.columns {
        db.add_column(column_name, column_type)
          .expect("Could not add column");
    }

    let mut rdr = csv::Reader::from_file(csv_path)
                      .and_then(|r| Ok(r.has_headers(false)))
                      .unwrap();

    let mut count = 0;
    for row in rdr.records().map(|r| r.unwrap()) {
        let eid = db.next_eid(&schema.table);
        let time = row.get(time_index).unwrap().parse::<usize>().unwrap();

        for (name, value) in schema.csv_ordering.iter().zip(row.iter()) {
            db.add_datum(&name, eid, value.to_owned(), time).expect("Could not add to db");
            count += 1;
        }
    }

    println!("added {:?} datums", count);
    db.sort_columns();
    db.write(file_path).expect("Could not write db to disk");
}
