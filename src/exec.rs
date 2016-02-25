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

fn compare(left: &Value, predicates: &[(Comparator, Rhs)], cache: &Cache) -> bool {
    predicates.iter().fold(true, |acc, predicate| {
        acc && match *predicate {
            (Comparator::Equal, Rhs::Constant(ref v)) => left == v,
            (Comparator::Greater, Rhs::Constant(ref v)) => left > v,
            (Comparator::GreaterOrEqual, Rhs::Constant(ref v)) => left >= v,
            (Comparator::Less, Rhs::Constant(ref v)) => left < v,
            (Comparator::LessOrEqual, Rhs::Constant(ref v)) => left <= v,
            (Comparator::Equal, Rhs::Column(ref col_name)) => {
                let col_eid_name = col_name.eid();
                let eids = cache.get(&col_eid_name).unwrap();
                match *left {
                    Value::Int(ref v) => eids.contains(v),
                    _ => panic!("Invalid query state, join not against int col")
                }
            }
            _ => panic!("Invalid query state, comp with column name")
        }
    })
}

fn match_by_predicates(entries: &Entries, predicates: &[(Comparator, Rhs)], cache: &Cache) -> Eids {
    let mut eids = Eids::new();

    match *entries {
        Entries::Bool(ref entries) => {
            for entry in entries {
                if compare(&Value::Bool(entry.value), predicates, cache) {
                    eids.insert(entry.eid);
                }
            }
        }
        Entries::Int(ref entries) => {
            for entry in entries {
                if compare(&Value::Int(entry.value), predicates, cache) {
                    eids.insert(entry.eid);
                }
            }
        }
        Entries::String(ref entries) => {
            for entry in entries {
                if compare(&Value::String(entry.value.to_owned()), predicates, cache) {
                    eids.insert(entry.eid);
                }
            }
        }
    }

    eids
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
        QueryNode::Where(Lhs::Column(ref left), ref predicates) => {
            let left_eid = left.eid();
            let column = try!(db.cols.get(left).ok_or(Error::MissingColumn(left.to_owned())));

            Ok((left_eid, Filtered::Eids(match_by_predicates(&column.entries, predicates, cache))))
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

    if stage_nodes.len() == 1 {
        for &query_node in &stage_nodes[0] {
            match *query_node {
                QueryNode::Select(ref name) => {
                    let col = try!(db.cols.get(&name).ok_or(Error::MissingColumn(name.to_owned())));;
                    result.push((name.to_owned(), col.entries.clone()))
                }
                _ => panic!("Invalid query state, should only be selects"),
            }
        }

        return Ok(result);
    }

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
