use data::{ColumnName, Db, Entries, Entry, Value};
use query::{Comparator, Plan, Lhs, QueryNode, Rhs};
use std::collections::HashMap;

type Cache = HashMap<ColumnName, Entries>;

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

fn filter_entries(entries: &Entries, comp: &Comparator, value: &Value) -> Entries {
    match *entries {
        Entries::Bool(ref rows) => {
            Entries::Bool(rows.iter().filter(|r| {
                compare(&Value::Bool(r.value), comp, value)
            }).cloned().collect::<Vec<Entry<bool>>>())
        }
        Entries::Int(ref rows) => {
            Entries::Int(rows.iter().filter(|r| {
                compare(&Value::Int(r.value), comp, value)
            }).cloned().collect::<Vec<Entry<usize>>>())
        }
        Entries::String(ref rows) => {
            Entries::String(rows.iter().filter(|r| {
                compare(&Value::String(r.value.to_owned()), comp, value)
            }).cloned().collect::<Vec<Entry<String>>>())
        }
    }
}

// FIXME: Remove needless result clonning
fn exec_node(db: &Db, cache: &Cache, node: &QueryNode) -> Result<(ColumnName, Entries), Error> {
    match *node {
        QueryNode::Select(ref name) => {
            let name_eid = name.eid();

            // FIXME: Returns the results of the last where search
            if let Some(entries) = cache.get(&name_eid) {
                Ok((name.to_owned(), entries.clone()))
            } else {
                Err(Error::MissingColumn(name_eid))
            }
        }
        QueryNode::Where(Lhs::Column(ref left), ref comp, Rhs::Constant(ref v)) => {
            let left_eid = left.eid();

            if let Some(entries) = cache.get(&left_eid) {
                Ok((left_eid, filter_entries(entries, comp, v)))
            } else if let Some(column) = db.cols.get(left) {
                Ok((left_eid, filter_entries(&column.entries, comp, v)))
            } else {
                Err(Error::MissingColumn(left_eid))
            }
        }
        QueryNode::Where(Lhs::Column(ref left), ref comp, Rhs::Column(ref right)) => panic!(),
    }
}

pub fn exec(db: &Db, plan: &Plan) -> Result<Vec<(ColumnName, Entries)>, Error> {
    let mut cache = Cache::new();
    let mut result = vec![];

    let stage_nodes = plan.stage_nodes();

    for (index, stage) in stage_nodes.iter().enumerate() {
        for query_node in stage {
            let (name, filtered) = try!(exec_node(db, &cache, query_node));

            if index == stage_nodes.len() - 1 {
                result.push((name, filtered));
            } else {
                cache.insert(name, filtered);
                println!("cache: {:?}", cache);
            }
        }
    }

    Ok(result)
}
