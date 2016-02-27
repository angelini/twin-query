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

#[derive(Debug)]
pub enum QueryLine {
    Select(Vec<ColumnName>),
    Join(String, ColumnName),
    Where(ColumnName, Comparator, Value),
}

#[derive(Debug, Clone)]
pub enum QueryNode {
    Select(ColumnName),
    Join(ColumnName, ColumnName),
    Where(ColumnName, Vec<(Comparator, Value)>),
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
            vec![PlanNode::from_query_node(QueryNode::Where(left, vec![(comp, right)]))]
        }
        QueryLine::Join(left, right) => {
            let left_eid = ColumnName::new(left, "eid".to_owned());
            vec![PlanNode::from_query_node(QueryNode::Join(left_eid, right))]
        }
    }
}

fn find_stage_index(stages: &[HashSet<NodeIndex>], node: &NodeIndex) -> Option<usize> {
    for (idx, stage) in stages.iter().enumerate() {
        if stage.contains(node) {
            return Some(idx);
        }
    }
    None
}

#[derive(Debug)]
pub enum Error {
    NoStages,
    EmptyStages,
    InvalidStageOrder,
}

#[derive(Debug)]
pub struct Plan {
    graph: Graph<PlanNode, ColumnName>,
    stages: Vec<HashSet<NodeIndex>>,
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

        let stage_types = self.stages
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
                              .collect::<Vec<HashSet<usize>>>();

        let stage_len = stage_types.len();
        for (index, types) in stage_types.iter().enumerate() {
            if index == stage_len - 1 && (types.contains(&2) || types.contains(&3)) {
                return Err(Error::InvalidStageOrder);
            }

            if index < stage_len - 1 && types.contains(&1) {
                return Err(Error::InvalidStageOrder);
            }
        }

        Ok(())
    }

    pub fn stage_nodes(&self) -> Vec<Vec<&QueryNode>> {
        self.stages
            .iter()
            .map(|stage| {
                stage.iter()
                     .map(|node_index| &self.graph[node_index.to_owned()].query)
                     .collect()
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

    fn build_stages(graph: &Graph<PlanNode, ColumnName>) -> Vec<HashSet<NodeIndex>> {
        let mut stages = vec![];

        for external in graph.externals(EdgeDirection::Incoming) {
            let mut dfs = Dfs::new(graph, external);
            while let Some(node) = dfs.next(graph) {
                let mut max_depth = -1;
                let provides = graph.neighbors_directed(node, EdgeDirection::Incoming);

                for provide in provides {
                    match find_stage_index(&stages, &provide) {
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
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "Plan: {:?}\n", self.stages));
        write!(f, "{}", Dot::new(&self.graph))
    }
}
