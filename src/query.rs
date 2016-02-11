use data::ColumnName;

#[derive(Debug)]
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

#[derive(Debug)]
enum QueryNode {
    Select(ColumnName),
    Where(ColumnName, Comparator, ColumnName),
}

#[derive(Debug)]
struct PlanNode {
    node: QueryNode,
    require: Option<ColumnName>,
    provide: Option<ColumnName>,
}

impl PlanNode {
    fn from_query_node(node: QueryNode) -> PlanNode {
        let require = match node {
            QueryNode::Select(ref name) => Some(name.to_owned()),
            QueryNode::Where(_, _, ref right) => Some(right.to_owned()),
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

#[derive(Debug)]
pub struct Plan {
    nodes: Vec<PlanNode>,
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

impl Plan {
    pub fn new(lines: Vec<QueryLine>) -> Plan {
        let nodes = lines.into_iter().flat_map(parse_line).collect();
        Plan { nodes: nodes }
    }
}
