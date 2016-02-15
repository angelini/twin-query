use petgraph::Graph;
use petgraph::graph::NodeIndex;

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

#[derive(Debug)]
pub struct Plan {
    graph: Graph<PlanNode, ColumnName>,
}

impl Plan {
    pub fn new(lines: Vec<QueryLine>) -> Plan {
        let mut graph = Graph::new();

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

        Plan { graph: graph }
    }
}
