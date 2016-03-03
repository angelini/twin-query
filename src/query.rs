use petgraph::{Dfs, EdgeDirection, Graph};
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use std::cmp;
use std::collections::HashSet;
use std::fmt;

use data::{ColumnName, Value};

#[derive(Debug, Clone)]
pub enum Comparator {
    Equal,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

impl Comparator {
    fn test(&self, left: &Value, right: &Value) -> bool {
        match *self {
            Comparator::Equal => left == right,
            Comparator::Greater => left > right,
            Comparator::GreaterOrEqual => left >= right,
            Comparator::Less => left < right,
            Comparator::LessOrEqual => left <= right,
        }
    }
}

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<ColumnName>),
    Join(String, ColumnName),
    Where(ColumnName, Comparator, Value),
    Limit(usize),
}

#[derive(Debug, Clone)]
pub struct Predicate {
    comparator: Comparator,
    value: Value,
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} {:?}", self.comparator, self.value)
    }
}

#[derive(Debug, Clone)]
pub struct Predicates {
    list: Vec<Predicate>,
}

impl Predicates {
    pub fn test(&self, left: &Value) -> bool {
        self.list.iter().fold(true, |acc, predicate| {
            acc && predicate.comparator.test(left, &predicate.value)
        })
    }

    fn new(comp: Comparator, val: Value) -> Predicates {
        Predicates {
            list: vec![Predicate {
                           comparator: comp,
                           value: val,
                       }],
        }
    }

    fn add(&mut self, predicate: Predicate) {
        self.list.push(predicate)
    }
}

impl fmt::Display for Predicates {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "["));
        let len = self.list.len();
        for (i, predicate) in self.list.iter().enumerate() {
            try!(write!(f, "{}", predicate));
            if i != len - 1 {
                try!(write!(f, ", "));
            }
        }
        write!(f, "]")
    }
}

impl Extend<Predicate> for Predicates {
    fn extend<T: IntoIterator<Item = Predicate>>(&mut self, iterable: T) {
        for elem in iterable {
            self.add(elem);
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryNode {
    Empty,
    Select(ColumnName, usize),
    Join(ColumnName, ColumnName),
    Where(ColumnName, Predicates),
}

impl fmt::Display for QueryNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            QueryNode::Select(ref col_name, limit) => write!(f, "Select({}, {})", col_name, limit),
            QueryNode::Join(ref left, ref right) => write!(f, "Join({}, {})", left, right),
            QueryNode::Where(ref col_name, ref preds) => {
                write!(f, "Where({}, {})", col_name, preds)
            }
            QueryNode::Empty => write!(f, "Empty()"),
        }
    }
}

#[derive(Debug, Clone)]
struct PlanNode {
    query: QueryNode,
    requires: Option<HashSet<ColumnName>>,
    provide: Option<ColumnName>,
}

impl PlanNode {
    fn from_query_node(node: QueryNode) -> PlanNode {
        let mut set = HashSet::new();
        let requires = match node {
            QueryNode::Select(ref name, _) => {
                set.insert(name.id());
                Some(set)
            }
            QueryNode::Join(ref left, _) => {
                set.insert(left.id());
                Some(set)
            }
            _ => None,
        };

        let provide = match node {
            QueryNode::Join(_, ref right) => Some(right.id()),
            QueryNode::Where(ref left, _) => Some(left.id()),
            _ => None,
        };

        PlanNode {
            query: node,
            requires: requires,
            provide: provide,
        }
    }
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.query)
    }
}

fn parse_line(line: QueryLine, limit: usize) -> Vec<PlanNode> {
    match line {
        QueryLine::Select(cols) => {
            cols.into_iter()
                .map(|col| PlanNode::from_query_node(QueryNode::Select(col, limit)))
                .collect()
        }
        QueryLine::Where(left, comp, right) => {
            vec![PlanNode::from_query_node(QueryNode::Where(left, Predicates::new(comp, right)))]
        }
        QueryLine::Join(left, right) => {
            let left_id = ColumnName::new(left, "id".to_owned());
            vec![PlanNode::from_query_node(QueryNode::Join(left_id, right))]
        }
        QueryLine::Limit => vec![],
    }
}

type NodeIndices = HashSet<NodeIndex>;

#[derive(Debug)]
pub enum Error {
    NoStages,
    EmptyStages,
    InvalidStageOrder,
    EmptyNodeInStages,
}

#[derive(Debug)]
pub struct Plan {
    graph: Graph<PlanNode, ColumnName>,
    stages: Vec<NodeIndices>,
}

impl Plan {
    pub fn new(lines: Vec<QueryLine>) -> Plan {
        let graph = Self::build_graph(lines);
        let stages = Self::build_stages(&graph);
        let mut plan = Plan {
            graph: graph,
            stages: stages,
        };

        plan.optimize();
        plan
    }

    pub fn is_valid(&self) -> Result<(), Error> {
        if self.stages.len() == 0 {
            return Err(Error::NoStages);
        }

        if self.stages.iter().any(|s| s.len() == 0) {
            return Err(Error::EmptyStages);
        }

        let stage_query_types = self.stage_query_types();
        let stages_len = stage_query_types.len();

        for (index, types) in stage_query_types.iter().enumerate() {
            if index == stages_len - 1 && (types.contains(&2) || types.contains(&3)) {
                return Err(Error::InvalidStageOrder);
            }

            if index < stages_len - 1 && types.contains(&1) {
                return Err(Error::InvalidStageOrder);
            }

            if types.contains(&4) {
                return Err(Error::EmptyNodeInStages);
            }
        }

        Ok(())
    }

    pub fn stage_query_nodes(&self) -> Vec<Vec<&QueryNode>> {
        self.stages
            .iter()
            .map(|stage| {
                stage.iter()
                     .map(|node_index| &self.graph[node_index.to_owned()].query)
                     .collect()
            })
            .collect()
    }

    fn optimize(&mut self) {
        for stage in &mut self.stages {
            let groups = Self::group_nodes(&self.graph, stage);

            for (node_index, col_name, predicates, to_remove) in groups {
                for rem in to_remove {
                    stage.remove(&rem);
                    self.graph[rem].query = QueryNode::Empty;
                }

                let mut node = &mut self.graph[node_index];
                node.query = QueryNode::Where(col_name, predicates);
            }
        }
    }

    fn group_nodes(graph: &Graph<PlanNode, ColumnName>, stage: &NodeIndices)
                   -> Vec<(NodeIndex, ColumnName, Predicates, NodeIndices)> {
        let mut groups = vec![];
        let mut already_matched: HashSet<NodeIndex> = HashSet::new();

        for &node_index in stage.iter() {
            if already_matched.contains(&node_index) {
                continue;
            };

            let (col_name, predicates) = match graph[node_index].query {
                QueryNode::Where(ref col_name, ref predicates) => (col_name, predicates),
                _ => continue,
            };

            let mut predicates = predicates.clone();
            let mut similar = HashSet::new();

            for &inner_index in stage.iter() {
                if node_index == inner_index {
                    continue;
                }

                let (inner_col, inner_preds) = match graph[inner_index].query {
                    QueryNode::Where(ref inner_col, ref inner_preds) => (inner_col, inner_preds),
                    _ => continue,
                };

                if col_name != inner_col {
                    continue;
                }

                already_matched.insert(inner_index);
                similar.insert(inner_index);
                predicates.extend(inner_preds.list.clone());
            }

            if similar.len() > 0 {
                groups.push((node_index, col_name.clone(), predicates, similar))
            }
        }

        groups
    }

    fn stage_query_types(&self) -> Vec<HashSet<usize>> {
        self.stages
            .iter()
            .map(|stage| {
                let mut stage_types = HashSet::new();
                for node_index in stage {
                    let plan_node = &self.graph[*node_index];
                    match plan_node.query {
                        QueryNode::Select(_, _) => stage_types.insert(1),
                        QueryNode::Join(_, _) => stage_types.insert(2),
                        QueryNode::Where(_, _) => stage_types.insert(3),
                        QueryNode::Empty => stage_types.insert(4),
                    };
                }
                stage_types
            })
            .collect()
    }

    fn build_graph(lines: Vec<QueryLine>) -> Graph<PlanNode, ColumnName> {
        let mut graph = Graph::new();

        let limit = lines.iter().fold(20, |acc, line| {
            match *line {
                QueryLine::Limit(size) => size,
                _ => acc,
            }
        });
        let node_indices = lines.into_iter()
                                .flat_map(|line| parse_line(line, limit))
                                .map(|node| (graph.add_node(node.clone()), node))
                                .collect::<Vec<(NodeIndex, PlanNode)>>();

        for &(node_index, ref node) in &node_indices {
            for &(inner_index, ref inner) in &node_indices {
                match (&node.requires, &inner.provide) {
                    (&Some(ref r), &Some(ref p)) => {
                        if r.contains(&p) {
                            graph.add_edge(node_index, inner_index, inner.provide.clone().unwrap());
                        }
                    }
                    _ => continue,
                }
            }
        }

        graph
    }

    fn build_stages(graph: &Graph<PlanNode, ColumnName>) -> Vec<NodeIndices> {
        let mut stages = vec![];

        for external in graph.externals(EdgeDirection::Incoming) {
            let mut dfs = Dfs::new(graph, external);
            while let Some(node) = dfs.next(graph) {
                let mut max_depth = -1;
                let provides = graph.neighbors_directed(node, EdgeDirection::Incoming);

                for provide in provides {
                    match Self::find_stage_index(&stages, &provide) {
                        Some(stage_index) => max_depth = cmp::max(max_depth, stage_index as isize),
                        _ => continue,
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
        stages
    }


    fn find_stage_index(stages: &[NodeIndices], node: &NodeIndex) -> Option<usize> {
        for (idx, stage) in stages.iter().enumerate() {
            if stage.contains(node) {
                return Some(idx);
            }
        }
        None
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "Plan: "));
        for (idx, stage) in self.stages.iter().enumerate() {
            let s = stage.iter()
                         .map(|i| format!("{}", self.graph[i.to_owned()]))
                         .collect::<Vec<String>>();
            try!(write!(f, "[{}]", s.join(", ")));

            if idx != self.stages.len() - 1 {
                try!(write!(f, ", "));
            }
        }
        write!(f, "\n{}", Dot::new(&self.graph))
    }
}
