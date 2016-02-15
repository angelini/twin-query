use petgraph::{Dfs, EdgeDirection, Graph};
use petgraph::graph::NodeIndex;
use std::cmp;
use std::collections::HashSet;

use data::{ColumnName, Value};

#[derive(Debug, Clone)]
pub enum Comparator {
    Equal,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Debug, Clone)]
pub enum Lhs {
    Column(ColumnName),
}

#[derive(Debug, Clone)]
pub enum Rhs {
    Column(ColumnName),
    Constant(Value),
}

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<ColumnName>),
    Where(Lhs, Comparator, Rhs),
}

#[derive(Debug, Clone)]
enum QueryNode {
    Select(ColumnName),
    Where(Lhs, Comparator, Rhs),
}

#[derive(Debug, Clone)]
struct PlanNode {
    node: QueryNode,
    require: Option<ColumnName>,
    provide: Option<ColumnName>,
}

impl PlanNode {
    fn from_query_node(node: QueryNode) -> PlanNode {
        let require = match node {
            QueryNode::Select(ref name) => {
                Some(ColumnName::new(name.table.to_owned(), "eid".to_owned()))
            }
            QueryNode::Where(_, _, Rhs::Column(ref right)) => {
                Some(ColumnName::new(right.table.to_owned(), "eid".to_owned()))
            },
            _ => None,
        };

        let provide = match node {
            QueryNode::Where(Lhs::Column(ref left), _, _) => {
                Some(ColumnName::new(left.table.to_owned(), "eid".to_owned()))
            }
            _ => None,
        };

        PlanNode {
            node: node,
            require: require,
            provide: provide,
        }
    }
}

fn parse_line(line: QueryLine) -> Vec<PlanNode> {
    match line {
        QueryLine::Select(cols) => {
            cols.into_iter()
                .map(|col| PlanNode::from_query_node(QueryNode::Select(col)))
                .collect()
        }
        QueryLine::Where(left, comp, right) => {
            vec![PlanNode::from_query_node(QueryNode::Where(left, comp, right))]
        }
    }
}

fn find_stage_index(stages: &[HashSet<NodeIndex>], node: &NodeIndex) -> Option<usize> {
    for (idx, stage) in stages.iter().enumerate() {
        if stage.contains(node) {
            return Some(idx);
        }
    };
    None
}

#[derive(Debug)]
pub struct Plan {
    graph: Graph<PlanNode, ColumnName>,
    stages: Vec<HashSet<NodeIndex>>,
}

impl Plan {
    pub fn new(lines: Vec<QueryLine>) -> Plan {
        let mut graph = Graph::new();
        let mut stages = vec![];

        let node_indices = lines.into_iter()
            .flat_map(parse_line)
            .map(|node| {
                (graph.add_node(node.clone()), node)
            })
            .collect::<Vec<(NodeIndex, PlanNode)>>();

        for &(node_index, ref node) in &node_indices {
            for &(inner_index, ref inner) in &node_indices {
                if node.require.is_some() && node.require == inner.provide {
                    graph.add_edge(node_index, inner_index, node.require.clone().unwrap());
                }
            }
        }

        for external in graph.externals(EdgeDirection::Incoming) {
            let mut dfs = Dfs::new(&graph, external);
            while let Some(node) = dfs.next(&graph) {
                let mut max_depth = -1;
                let provides = graph.neighbors_directed(node, EdgeDirection::Incoming);

                for provide in provides {
                    match find_stage_index(&stages, &provide) {
                        Some(stage_index) => {
                            max_depth = cmp::max(max_depth, stage_index as isize)
                        }
                        _ => continue
                    }
                }

                let stage_index = (max_depth + 1) as usize;

                if stage_index >= stages.len() {
                    stages.push(HashSet::new())
                }
                stages[stage_index].insert(node);
            }
        }

        stages.reverse();
        Plan { graph: graph, stages: stages }
    }
}
