use petgraph::{Dfs, EdgeDirection, Graph};
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use std::cmp;
use std::collections::HashSet;
use std::fmt;
use std::str;

use data::{ColumnName, Value};

peg_file! grammar("grammar.rustpeg");

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

#[derive(Debug, Clone)]
pub enum Predicate {
    Constant(Comparator, Value),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
}

impl Predicate {
    pub fn or_from_vec(mut predicates: Vec<Predicate>) -> Predicate {
        if predicates.len() == 1 {
            return predicates.pop().unwrap();
        }

        if predicates.len() == 2 {
            return Predicate::Or(Box::new(predicates.pop().unwrap()),
                                 Box::new(predicates.pop().unwrap()));
        }

        let first = predicates.pop();
        Predicate::Or(Box::new(first.unwrap()),
                      Box::new(Self::or_from_vec(predicates)))
    }

    pub fn test(&self, value: &Value) -> bool {
        #![allow(unconditional_recursion)]
        match *self {
            Predicate::Constant(ref comp, ref right) => comp.test(value, right),
            Predicate::And(ref left, ref right) => left.test(value) && right.test(value),
            Predicate::Or(ref left, ref right) => left.test(value) || right.test(value),
        }
    }
}

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<ColumnName>),
    Join(String, ColumnName),
    Where(ColumnName, Predicate),
    Limit(usize),
}

#[derive(Debug, Clone)]
pub enum PlanNode {
    Empty,
    Select(ColumnName, usize),
    Join(ColumnName, ColumnName),
    Where(ColumnName, Predicate),
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PlanNode::Select(ref col_name, limit) => write!(f, "Select({}, {})", col_name, limit),
            PlanNode::Join(ref left, ref right) => write!(f, "Join({}, {})", left, right),
            PlanNode::Where(ref col_name, ref pred) => write!(f, "Where({}, {:?})", col_name, pred),
            PlanNode::Empty => write!(f, "Empty()"),
        }
    }
}

fn parse_line(line: QueryLine, limit: usize)
              -> Vec<(PlanNode, Option<ColumnName>, Option<ColumnName>)> {
    match line {
        QueryLine::Select(cols) => {
            cols.into_iter()
                .map(|col| {
                    let col_id = col.id();
                    (PlanNode::Select(col, limit), Some(col_id), None)
                })
                .collect()
        }
        QueryLine::Where(left, pred) => {
            let left_id = left.id();
            vec![(PlanNode::Where(left, pred), None, Some(left_id))]
        }
        QueryLine::Join(left, right) => {
            let left_id = ColumnName::new(left, "id".to_owned());
            let right_id = right.id();
            vec![(PlanNode::Join(left_id.clone(), right),
                  Some(left_id),
                  Some(right_id))]
        }
        QueryLine::Limit => vec![],
    }
}

type NodeIndices = HashSet<NodeIndex>;

#[derive(Debug)]
pub enum Error {
    ParseError(grammar::ParseError),
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

    pub fn stage_plan_nodes(&self) -> Vec<Vec<&PlanNode>> {
        self.stages
            .iter()
            .map(|stage| {
                stage.iter()
                     .map(|node_index| &self.graph[node_index.to_owned()])
                     .collect()
            })
            .collect()
    }

    fn optimize(&mut self) {
        for stage in &mut self.stages {
            let groups = Self::group_nodes(&self.graph, stage);

            for (node_index, col_name, predicate, to_remove) in groups {
                for rem in to_remove {
                    stage.remove(&rem);
                    self.graph[rem] = PlanNode::Empty;
                }

                self.graph[node_index] = PlanNode::Where(col_name, predicate);
            }
        }
    }

    fn group_nodes(graph: &Graph<PlanNode, ColumnName>, stage: &NodeIndices)
                   -> Vec<(NodeIndex, ColumnName, Predicate, NodeIndices)> {
        let mut groups = vec![];
        let mut already_matched: HashSet<NodeIndex> = HashSet::new();

        for &node_index in stage.iter() {
            if already_matched.contains(&node_index) {
                continue;
            };

            let (col_name, predicate) = match graph[node_index] {
                PlanNode::Where(ref col_name, ref predicate) => (col_name, predicate),
                _ => continue,
            };

            let mut predicate = predicate.to_owned();
            let mut similar = HashSet::new();

            for &inner_index in stage.iter() {
                if node_index == inner_index {
                    continue;
                }

                let (inner_col, inner_pred) = match graph[inner_index] {
                    PlanNode::Where(ref inner_col, ref inner_pred) => (inner_col, inner_pred),
                    _ => continue,
                };

                if col_name != inner_col {
                    continue;
                }

                already_matched.insert(inner_index);
                similar.insert(inner_index);
                predicate = Predicate::And(Box::new(predicate), Box::new(inner_pred.to_owned()));
            }

            if similar.len() > 0 {
                groups.push((node_index, col_name.clone(), predicate, similar))
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
                    match self.graph[*node_index] {
                        PlanNode::Select(_, _) => stage_types.insert(1),
                        PlanNode::Join(_, _) => stage_types.insert(2),
                        PlanNode::Where(_, _) => stage_types.insert(3),
                        PlanNode::Empty => stage_types.insert(4),
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
        let node_indices =
            lines.into_iter()
                 .flat_map(|line| parse_line(line, limit))
                 .map(|(node, require, provide)| (graph.add_node(node.clone()), require, provide))
                 .collect::<Vec<(NodeIndex, Option<ColumnName>, Option<ColumnName>)>>();

        for &(node_index, ref req, _) in &node_indices {
            for &(inner_index, _, ref prov) in &node_indices {
                match (req, prov) {
                    (&Some(ref r), &Some(ref p)) => {
                        if r == p {
                            graph.add_edge(node_index, inner_index, prov.clone().unwrap());
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

impl str::FromStr for Plan {
    type Err = Error;

    fn from_str(query: &str) -> Result<Self, Self::Err> {
        let query_lines = try!(grammar::query(query));
        let plan = Plan::new(query_lines);
        try!(plan.is_valid());
        Ok(plan)
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

impl From<grammar::ParseError> for Error {
    fn from(err: grammar::ParseError) -> Error {
        Error::ParseError(err)
    }
}
