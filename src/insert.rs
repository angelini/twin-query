use csv;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use toml;

use data::{ColumnName, ColumnType, Db};

#[derive(Debug)]
enum Error {
    MissingId,
    MissingTime,
    InvalidOrdering,
}

#[derive(Debug, RustcEncodable, RustcDecodable)]
struct RawSchema {
    table: String,
    columns: HashMap<String, String>,
    csv_ordering: Vec<String>,
}

impl RawSchema {
    fn add_column(&mut self, name: &str, t: &str) {
        self.columns.insert(name.to_owned(), t.to_owned());
    }
}

#[derive(Debug)]
struct Schema {
    table: String,
    columns: HashMap<ColumnName, ColumnType>,
    csv_ordering: Vec<ColumnName>,
}

impl Schema {
    fn from_raw(mut raw: RawSchema) -> Result<Schema, Error> {
        raw.add_column("id", "Int");
        raw.add_column("time", "Int");
        let ordering_set = raw.csv_ordering.iter().map(|s| s.as_str()).collect::<HashSet<&str>>();

        for col in &raw.csv_ordering {
            if !raw.columns.contains_key(col) {
                return Err(Error::InvalidOrdering);
            }
        }

        if raw.csv_ordering.len() != raw.columns.len() {
            return Err(Error::InvalidOrdering);
        }

        if raw.csv_ordering.len() != ordering_set.len() {
            return Err(Error::InvalidOrdering);
        }

        if !ordering_set.contains("id") {
            return Err(Error::MissingId);
        }

        if !ordering_set.contains("time") {
            return Err(Error::MissingTime);
        }

        Ok(Schema {
            table: raw.table.to_owned(),
            columns: Self::column_names_and_types(&raw.table, raw.columns),
            csv_ordering: Self::ordering(&raw.table, raw.csv_ordering.clone()),
        })
    }

    fn column_index(&self, col: &str) -> Option<usize> {
        self.csv_ordering.iter().position(|c| c.column == col)
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
        raw.into_iter().map(|col| ColumnName::new(table.to_owned(), col)).collect()
    }
}

fn read_schema(schema_path: &str) -> Schema {
    let mut contents = String::new();
    File::open(schema_path)
        .and_then(|mut f| f.read_to_string(&mut contents))
        .unwrap();

    Schema::from_raw(toml::decode_str(&contents).unwrap()).expect("Invalid schema")
}

pub fn add_to_db(file_path: &str, schema_path: &str, csv_path: &str) {
    let mut db = Db::from_file(file_path).expect("Failed to load db from file");

    let schema = read_schema(schema_path);
    let id_index = schema.column_index("id").expect("`id` column not found");
    let time_index = schema.column_index("time").expect("`time` column not found");

    for (column_name, column_type) in schema.columns {
        db.add_column(column_name, column_type)
          .expect("Failed to add column to db");
    }

    let mut rdr = csv::Reader::from_file(csv_path)
                      .and_then(|r| Ok(r.has_headers(false)))
                      .unwrap();

    let mut count = 0;
    for row in rdr.records().map(|r| r.unwrap()) {
        let id = row.get(id_index).unwrap().parse::<usize>().unwrap();
        let time = row.get(time_index).unwrap().parse::<usize>().unwrap();

        for (name, value) in schema.csv_ordering.iter().zip(row.iter()) {
            db.add_datum(&name, id, value.to_owned(), time).expect("Failed to add datum to db");
            count += 1;
        }
    }

    println!("added {:?} datums", count);
    db.optimize_columns();
    db.write(file_path).expect("Failed to write db to disk");
}
