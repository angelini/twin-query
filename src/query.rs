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
}

#[derive(Debug, Clone)]
pub struct Predicate {
    comparator: Comparator,
    value: Value,
}

#[derive(Debug, Clone)]
pub struct Predicates {
    list: Vec<Predicate>,
}

impl Predicates {
    fn new(comp: Comparator, val: Value) -> Predicates {
        Predicates {
            list: vec![Predicate {
                           comparator: comp,
                           value: val,
                       }],
        }
    }

    pub fn test(&self, left: &Value) -> bool {
        self.list.iter().fold(true, |acc, predicate| {
            acc && predicate.comparator.test(left, &predicate.value)
        })
    }
}

#[derive(Debug, Clone)]
pub enum QueryNode {
    Select(ColumnName),
    Join(ColumnName, ColumnName),
    Where(ColumnName, Predicates),
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
            QueryNode::Select(ref name) => {
                set.insert(name.eid());
                Some(set)
            }
            QueryNode::Join(ref left, _) => {
                set.insert(left.eid());
                Some(set)
            }
            _ => None,
        };

        let provide = match node {
            QueryNode::Join(_, ref right) => Some(right.eid()),
            QueryNode::Where(ref left, _) => Some(left.eid()),
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
        write!(f, "{:?}", self.query)
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
            vec![PlanNode::from_query_node(QueryNode::Where(left, Predicates::new(comp, right)))]
        }
        QueryLine::Join(left, right) => {
            let left_eid = ColumnName::new(left, "eid".to_owned());
            vec![PlanNode::from_query_node(QueryNode::Join(left_eid, right))]
        }
    }
}

type NodeIndices = HashSet<NodeIndex>;

#[derive(Debug)]
pub enum Error {
    NoStages,
    EmptyStages,
    InvalidStageOrder,
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
        Plan {
            graph: graph,
            stages: stages,
        }
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

    fn stage_query_types(&self) -> Vec<HashSet<usize>> {
        self.stages
            .iter()
            .map(|stage| {
                let mut stage_types = HashSet::new();
                for node_index in stage {
                    let plan_node = &self.graph[*node_index];
                    match plan_node.query {
                        QueryNode::Select(_) => stage_types.insert(1),
                        QueryNode::Join(_, _) => stage_types.insert(2),
                        QueryNode::Where(_, _) => stage_types.insert(3),
                    };
                }
                stage_types
            })
            .collect()
    }

    fn build_graph(lines: Vec<QueryLine>) -> Graph<PlanNode, ColumnName> {
        let mut graph = Graph::new();

        let node_indices = lines.into_iter()
                                .flat_map(parse_line)
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
        try!(write!(f, "Plan: {:?}\n", self.stages));
        write!(f, "{}", Dot::new(&self.graph))
    }
}
