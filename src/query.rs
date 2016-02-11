use petgraph::Graph;
use petgraph::graph::NodeIndex;

use data::ColumnName;

#[derive(Debug, Clone)]
pub enum Comparator {
    Equal,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<ColumnName>),
    Where(ColumnName, Comparator, ColumnName), /* FIXME: Where needs to support constants in the rh column */
}

#[derive(Debug, Clone)]
enum QueryNode {
    Select(ColumnName),
    Where(ColumnName, Comparator, ColumnName),
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
            QueryNode::Where(_, _, ref right) => {
                Some(ColumnName::new(right.table.to_owned(), "eid".to_owned()))
            }
        };

        let provide = match node {
            QueryNode::Where(ref left, _, _) => {
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
                println!("looking for: {:?}", node.require);
                if node.require.is_some() && node.require == inner.provide {
                    graph.add_edge(node_index, inner_index, node.require.clone().unwrap());
                }
            }
        }

        Plan { graph: graph }
    }
}
