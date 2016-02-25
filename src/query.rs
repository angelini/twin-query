use petgraph::{Dfs, EdgeDirection, Graph};
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use std::cmp;
use std::collections::HashSet;
use std::fmt;
use std::ops::Index;

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
pub enum QueryNode {
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
            QueryNode::Select(ref name) => Some(name.eid()),
            QueryNode::Where(_, _, Rhs::Column(ref right)) => Some(right.eid()),
            _ => None,
        };

        let provide = match node {
            QueryNode::Where(Lhs::Column(ref left), _, _) => Some(left.eid()),
            _ => None,
        };

        PlanNode {
            node: node,
            require: require,
            provide: provide,
        }
    }
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.node)
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
    }
    None
}

#[derive(Debug)]
pub enum Error {
    NoStages,
    EmptyStages,
    InvalidQueryNodeCombination,
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
                                      match plan_node.node {
                                          QueryNode::Select(_) => stage_types.insert(1),
                                          QueryNode::Where(_, _, _) => stage_types.insert(2),
                                      };
                                  }
                                  stage_types
                              })
                              .collect::<Vec<HashSet<usize>>>();

        let stage_len = stage_types.len();
        for (index, types) in stage_types.iter().enumerate() {
            if types.len() > 1 {
                return Err(Error::InvalidQueryNodeCombination);
            }

            if index == stage_len - 1 && types.contains(&2) {
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
                     .map(|node_index| &self.graph.index(node_index.to_owned()).node)
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
                if node.require.is_some() && node.require == inner.provide {
                    graph.add_edge(node_index, inner_index, node.require.clone().unwrap());
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
