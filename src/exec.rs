use crossbeam;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use data::{ColumnName, Db, Ids, Data, Datum, Value};
use plan::{Plan, Predicate, PlanNode, Stage};

struct Cache<'a> {
    db: &'a Db,
    map: HashMap<ColumnName, Ids>,
}

impl<'a> Cache<'a> {
    fn new(db: &'a Db) -> Self {
        Cache {
            db: db,
            map: HashMap::new(),
        }
    }

    fn get(&self, name: &ColumnName) -> Option<&Ids> {
        self.map.get(name).or_else(|| {
            match self.db.ids.get(&name.table) {
                Some(ids) => Some(ids),
                None => None,
            }
        })
    }

    fn insert_or_merge(&mut self, name: ColumnName, ids: Ids) {
        let merged = match self.map.get(&name) {
            Some(set) => ids.intersection(set).cloned().collect(),
            None => ids,
        };
        self.map.insert(name, merged);
    }
}

#[derive(Debug)]
enum Filtered {
    Data(Data),
    Ids(Ids),
}

#[derive(Debug)]
pub enum Error {
    MissingColumn(ColumnName),
    InvalidJoin(ColumnName),
}

fn match_by_predicate(data: &Data, predicate: &Predicate) -> Ids {
    let mut ids = Ids::new();

    match *data {
        Data::Bool(ref data) => {
            for datum in data {
                if predicate.test(&Value::Bool(datum.value)) {
                    ids.insert(datum.id);
                }
            }
        }
        Data::Int(ref data) => {
            for datum in data {
                if predicate.test(&Value::Int(datum.value)) {
                    ids.insert(datum.id);
                }
            }
        }
        Data::String(ref data) => {
            for datum in data {
                if predicate.test(&Value::String(datum.value.to_owned())) {
                    ids.insert(datum.id);
                }
            }
        }
    }

    ids
}

fn match_by_ids(data: &[Datum<usize>], ids: &Ids) -> Ids {
    data.iter()
        .fold(Ids::new(), |mut acc, datum| {
            if ids.contains(&datum.value) {
                acc.insert(datum.id);
            }
            acc
        })
}

fn clone_matching_data<T: Clone>(data: &[Datum<T>], ids: &Ids, limit: usize) -> Vec<Datum<T>> {
    data.iter()
        .filter(|datum| ids.contains(&datum.id))
        .take(limit)
        .cloned()
        .collect()
}

fn find_data_by_set(data: &Data, ids: &HashSet<usize>, limit: usize) -> Data {
    match *data {
        Data::Bool(ref data) => Data::Bool(clone_matching_data(data, ids, limit)),
        Data::Int(ref data) => Data::Int(clone_matching_data(data, ids, limit)),
        Data::String(ref data) => Data::String(clone_matching_data(data, ids, limit)),
    }
}

fn find_data(db: &Db, cache: &Cache, node: &PlanNode) -> Result<(ColumnName, Filtered), Error> {
    match *node {
        PlanNode::Select(ref name, limit) => {
            let name_id = name.id();
            let ids = try!(cache.get(&name_id).ok_or(Error::MissingColumn(name_id)));
            let column = try!(db.cols.get(name).ok_or(Error::MissingColumn(name.to_owned())));

            Ok((name.to_owned(),
                Filtered::Data(find_data_by_set(&column.data, &ids, limit))))
        }
        PlanNode::Join(ref left, ref right) => {
            let ids = try!(cache.get(left).ok_or(Error::MissingColumn(left.to_owned())));
            let column = try!(db.cols.get(right).ok_or(Error::MissingColumn(right.to_owned())));

            match column.data {
                Data::Int(ref data) => Ok((right.id(), Filtered::Ids(match_by_ids(data, ids)))),
                _ => Err(Error::InvalidJoin(right.to_owned())),
            }
        }
        PlanNode::Where(ref left, ref predicate) => {
            let left_id = left.id();
            let column = try!(db.cols.get(left).ok_or(Error::MissingColumn(left.to_owned())));

            Ok((left_id,
                Filtered::Ids(match_by_predicate(&column.data, predicate))))
        }
        PlanNode::WhereId(ref left, ref ids) => {
            let cache_ids = try!(cache.get(left).ok_or(Error::MissingColumn(left.to_owned())));
            let matched_ids = ids.iter()
                                 .filter(|id| cache_ids.contains(id))
                                 .cloned()
                                 .collect::<HashSet<usize>>();

            Ok((left.to_owned(), Filtered::Ids(matched_ids)))
        }
    }
}

fn exec_stage(db: &Db, cache: &Cache, stage: &Stage) -> Result<Vec<(ColumnName, Filtered)>, Error> {
    let (tx, rx) = mpsc::channel();

    crossbeam::scope(|scope| {
        for query_node in &stage.nodes {
            let t_tx = tx.clone();
            scope.spawn(move || {
                let (name, filtered) = find_data(&db, &cache, &query_node).unwrap();
                t_tx.send((name, filtered)).unwrap();
            });
        }
    });

    let mut results = vec![];
    for _ in 0..stage.len() {
        results.push(rx.recv().unwrap())
    }

    Ok(results)
}

pub fn exec(db: &Db, plan: &Plan) -> Result<Vec<(ColumnName, Data)>, Error> {
    let mut cache = Cache::new(db);
    let mut result = vec![];

    for stage in &plan.stages {
        for (name, filtered) in try!(exec_stage(db, &cache, stage)) {
            match filtered {
                Filtered::Ids(ids) => cache.insert_or_merge(name, ids),
                Filtered::Data(data) => result.push((name, data)),
            }
        }
    }

    Ok(result)
}
