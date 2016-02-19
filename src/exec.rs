use data::{ColumnName, Db, Entries, Entry, Value};
use query::{Comparator, Plan, Lhs, QueryNode, Rhs};
use std::collections::{HashMap, HashSet};

type Eids = HashSet<usize>;

type Cache = HashMap<ColumnName, Eids>;

#[derive(Debug)]
enum Filtered {
    Entries(Entries),
    Eids(Eids),
}

#[derive(Debug)]
pub enum Error {
    MissingColumn(ColumnName),
}

fn compare(left: &Value, comp: &Comparator, right: &Value) -> bool {
    match *comp {
        Comparator::Equal => left == right,
        Comparator::Greater => left > right,
        Comparator::GreaterOrEqual => left >= right,
        Comparator::Less => left < right,
        Comparator::LessOrEqual => left <= right,
    }
}

fn find_eids_by_value(entries: &Entries, comp: &Comparator, value: &Value) -> Eids {
    let mut eids = Eids::new();

    match *entries {
        Entries::Bool(ref entries) => {
            for entry in entries {
                if compare(&Value::Bool(entry.value), comp, value) {
                    eids.insert(entry.eid);
                }
            }
        }
        Entries::Int(ref entries) => {
            for entry in entries {
                if compare(&Value::Int(entry.value), comp, value) {
                    eids.insert(entry.eid);
                }
            }
        }
        Entries::String(ref entries) => {
            for entry in entries {
                if compare(&Value::String(entry.value.to_owned()), comp, value) {
                    eids.insert(entry.eid);
                }
            }
        }
    }

    eids
}

fn match_by_eids<T>(entries: &[Entry<T>], eids: &Eids) -> Eids {
    entries.iter()
           .fold(Eids::new(), |mut acc, entry| {
               if eids.contains(&entry.eid) {
                   acc.insert(entry.eid);
               }
               acc
           })
}

fn find_eids_by_set(entries: &Entries, eids: &Eids) -> Eids {
    match *entries {
        Entries::Bool(ref entries) => match_by_eids(entries, eids),
        Entries::Int(ref entries) => match_by_eids(entries, eids),
        Entries::String(ref entries) => match_by_eids(entries, eids),
    }
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
        QueryNode::Where(Lhs::Column(ref left), ref comp, Rhs::Constant(ref v)) => {
            let left_eid = left.eid();
            let column = try!(db.cols.get(left).ok_or(Error::MissingColumn(left.to_owned())));

            Ok((left_eid,
                Filtered::Eids(find_eids_by_value(&column.entries, comp, v))))
        }
        QueryNode::Where(Lhs::Column(ref left), _, Rhs::Column(ref right)) => {
            let (left_eid, right_eid) = (left.eid(), right.eid());
            let eids = try!(cache.get(&right_eid).ok_or(Error::MissingColumn(right_eid)));
            let column = try!(db.cols.get(left).ok_or(Error::MissingColumn(left.to_owned())));

            Ok((left_eid,
                Filtered::Eids(find_eids_by_set(&column.entries, &eids))))
        }
    }
}

fn insert_or_merge(cache: &mut Cache, name: ColumnName, eids: Eids) {
    let set = match cache.get(&name) {
        Some(set) => eids.intersection(set).cloned().collect(),
        None => eids,
    };
    cache.insert(name, set);
}

pub fn exec(db: &Db, plan: &Plan) -> Result<Vec<(ColumnName, Entries)>, Error> {
    let mut cache = Cache::new();
    let mut result = vec![];

    let stage_nodes = plan.stage_nodes();

    for stage in &stage_nodes {
        for query_node in stage {
            let (name, filtered) = try!(find_entries(db, &cache, query_node));

            match filtered {
                Filtered::Eids(eids) => insert_or_merge(&mut cache, name, eids),
                Filtered::Entries(entries) => result.push((name, entries)),
            }
        }
    }

    Ok(result)
}
