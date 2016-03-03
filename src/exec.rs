use std::collections::{HashMap, HashSet};

use data::{ColumnName, Db, Eids, Entries, Entry, Value};
use query::{Plan, Predicates, QueryNode};

struct Cache<'a> {
    db: &'a Db,
    map: HashMap<ColumnName, Eids>,
}

impl<'a> Cache<'a> {
    fn new(db: &'a Db) -> Self {
        Cache {
            db: db,
            map: HashMap::new(),
        }
    }

    fn get(&self, name: &ColumnName) -> Option<&Eids> {
        self.map.get(name).or_else(|| {
            match self.db.eids.get(&name.table) {
                Some(eids) => Some(eids),
                None => None,
            }
        })
    }

    fn insert_or_merge(&mut self, name: ColumnName, eids: Eids) {
        if let Some(set) = self.map.get_mut(&name) {
            set.extend(eids);
            return;
        }
        self.map.insert(name, eids);
    }
}

#[derive(Debug)]
enum Filtered {
    Entries(Entries),
    Eids(Eids),
}

#[derive(Debug)]
pub enum Error {
    MissingColumn(ColumnName),
    InvalidJoin(ColumnName),
}

fn match_by_predicates(entries: &Entries, predicates: &Predicates) -> Eids {
    let mut eids = Eids::new();

    match *entries {
        Entries::Bool(ref entries) => {
            for entry in entries {
                if predicates.test(&Value::Bool(entry.value)) {
                    eids.insert(entry.eid);
                }
            }
        }
        Entries::Int(ref entries) => {
            for entry in entries {
                if predicates.test(&Value::Int(entry.value)) {
                    eids.insert(entry.eid);
                }
            }
        }
        Entries::String(ref entries) => {
            for entry in entries {
                if predicates.test(&Value::String(entry.value.to_owned())) {
                    eids.insert(entry.eid);
                }
            }
        }
    }

    eids
}

fn match_by_eids(entries: &[Entry<usize>], eids: &Eids) -> Eids {
    entries.iter()
           .fold(Eids::new(), |mut acc, entry| {
               if eids.contains(&entry.value) {
                   acc.insert(entry.eid);
               }
               acc
           })
}

fn clone_matching_entries<T: Clone>(entries: &[Entry<T>], eids: &Eids) -> Vec<Entry<T>> {
    entries.iter()
           .filter(|entry| eids.contains(&entry.eid))
           .cloned()
           .collect()
}

fn find_entries_by_set(entries: &Entries, eids: &HashSet<usize>) -> Entries {
    match *entries {
        Entries::Bool(ref entries) => Entries::Bool(clone_matching_entries(entries, eids)),
        Entries::Int(ref entries) => Entries::Int(clone_matching_entries(entries, eids)),
        Entries::String(ref entries) => Entries::String(clone_matching_entries(entries, eids)),
    }
}

fn find_entries(db: &Db, cache: &Cache, node: &QueryNode) -> Result<(ColumnName, Filtered), Error> {
    match *node {
        QueryNode::Select(ref name) => {
            let name_eid = name.eid();
            let eids = try!(cache.get(&name_eid).ok_or(Error::MissingColumn(name_eid)));
            let column = try!(db.cols.get(name).ok_or(Error::MissingColumn(name.to_owned())));

            Ok((name.to_owned(),
                Filtered::Entries(find_entries_by_set(&column.entries, &eids))))
        }
        QueryNode::Join(ref left, ref right) => {
            let eids = try!(cache.get(left).ok_or(Error::MissingColumn(left.to_owned())));
            let column = try!(db.cols.get(right).ok_or(Error::MissingColumn(right.to_owned())));

            match column.entries {
                Entries::Int(ref entries) => {
                    Ok((right.eid(), Filtered::Eids(match_by_eids(entries, eids))))
                }
                _ => Err(Error::InvalidJoin(right.to_owned())),
            }
        }
        QueryNode::Where(ref left, ref predicates) => {
            let left_eid = left.eid();
            let column = try!(db.cols.get(left).ok_or(Error::MissingColumn(left.to_owned())));

            Ok((left_eid,
                Filtered::Eids(match_by_predicates(&column.entries, predicates))))
        }
        QueryNode::Empty => panic!("Tried to execute empty node")
    }
}

pub fn exec(db: &Db, plan: &Plan) -> Result<Vec<(ColumnName, Entries)>, Error> {
    let mut cache = Cache::new(db);
    let mut result = vec![];

    let stage_query_nodes = plan.stage_query_nodes();

    for query_nodes in &stage_query_nodes {
        for query_node in query_nodes {
            let (name, filtered) = try!(find_entries(db, &cache, query_node));

            match filtered {
                Filtered::Eids(eids) => cache.insert_or_merge(name, eids),
                Filtered::Entries(entries) => result.push((name, entries)),
            }
        }
    }

    Ok(result)
}
