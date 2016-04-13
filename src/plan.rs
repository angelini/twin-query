use petgraph::{Dfs, EdgeDirection, Graph};
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str;

use data::{ColumnName, Value};

peg_file! grammar("grammar.rustpeg");

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Open-closed interval
/// min < time <= max
pub struct TimeBound {
    min: Option<usize>,
    max: Option<usize>,
}

impl TimeBound {
    fn from_predicate(predicate: &Predicate) -> Self {
        match *predicate {
            Predicate::Constant(ref comp, ref value) => {
                let int_val = match *value {
                    Value::Int(i) => i,
                    _ => panic!("TimeBounds must be built with int predicates"),
                };

                let (min, max) = match *comp {
                    Comparator::Equal => (Some(int_val - 1), Some(int_val)),
                    Comparator::Greater => (Some(int_val), None),
                    Comparator::GreaterOrEqual => (Some(int_val - 1), None),
                    Comparator::Less => (None, Some(int_val - 1)),
                    Comparator::LessOrEqual => (None, Some(int_val)),
                };

                TimeBound {
                    min: min,
                    max: max,
                }
            }
            Predicate::And(ref left, ref right) => {
                Self::from_predicate(left).combine(&Self::from_predicate(right))
            }
            Predicate::Or(_, _) => unimplemented!(),
        }
    }

    fn combine(&self, bound: &TimeBound) -> TimeBound {
        TimeBound {
            min: self.min.or(bound.min),
            max: self.max.or(bound.max),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlanNode {
    Select(ColumnName, usize),
    Join(ColumnName, ColumnName),
    Where(ColumnName, Predicate, Option<TimeBound>),
    WhereId(ColumnName, Vec<usize>),
}

impl PlanNode {
    fn table(&self) -> &str {
        match *self {
            PlanNode::Select(ref col_name, _) |
            PlanNode::Join(ref col_name, _) |
            PlanNode::Where(ref col_name, _, _) |
            PlanNode::WhereId(ref col_name, _) => &col_name.table,
        }
    }
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PlanNode::Select(ref col_name, limit) => write!(f, "Select({}, {})", col_name, limit),
            PlanNode::Join(ref left, ref right) => write!(f, "Join({}, {})", left, right),
            PlanNode::Where(ref col_name, ref pred, ref time_bound) => {
                write!(f, "Where({}, {:?}, {:?})", col_name, pred, time_bound)
            }
            PlanNode::WhereId(ref col_name, ref ids) => {
                write!(f, "WhereId({}, {:?})", col_name, ids)
            }
        }
    }
}

type Requires = Option<ColumnName>;
type Provides = Option<ColumnName>;

fn extract_ids(predicate: &Predicate) -> Option<Vec<usize>> {
    match *predicate {
        Predicate::Constant(Comparator::Equal, Value::Int(val)) => Some(vec![val]),
        Predicate::Or(ref left, ref right) => {
            match (extract_ids(&left), extract_ids(&right)) {
                (Some(mut left_ids), Some(mut right_ids)) => {
                    left_ids.append(&mut right_ids);
                    Some(left_ids)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn parse_line(line: QueryLine, limit: usize) -> Vec<(PlanNode, Requires, Provides)> {
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
            let node = if left == left_id {
                match extract_ids(&pred) {
                    Some(ids) => PlanNode::WhereId(left, ids),
                    None => PlanNode::Where(left, pred, None),
                }
            } else {
                PlanNode::Where(left, pred, None)
            };

            vec![(node, None, Some(left_id))]
        }
        QueryLine::Join(left, right) => {
            let left_id = ColumnName::new(left, "id".to_owned());
            let right_id = right.id();
            vec![(PlanNode::Join(left_id.clone(), right),
                  Some(left_id),
                  Some(right_id))]
        }
        QueryLine::Limit(_) => vec![],
    }
}

#[derive(Debug, Clone)]
pub struct Stage {
    pub nodes: HashSet<PlanNode>,
}

impl Stage {
    pub fn new(nodes: HashSet<PlanNode>) -> Stage {
        Stage { nodes: nodes }
    }

    pub fn contains(&self, node: &PlanNode) -> bool {
        self.nodes.contains(node)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    fn insert(&mut self, node: PlanNode) {
        self.nodes.insert(node);
    }

    fn replace(&mut self, to_remove: &[&PlanNode], to_add: Vec<PlanNode>) {
        for remove in to_remove {
            self.nodes.remove(remove);
        }

        for add in to_add {
            self.nodes.insert(add);
        }
    }

    fn group_where_nodes_by_column(&self) -> Vec<Vec<&PlanNode>> {
        let mut map = HashMap::new();

        for node in &self.nodes {
            match *node {
                PlanNode::Where(ref col_name, _, _) => {
                    let mut nodes = map.entry(col_name).or_insert_with(Vec::new);
                    nodes.push(node)
                }
                _ => continue,
            }
        }

        map.into_iter()
           .map(|(_, v)| v)
           .collect()
    }

    fn find_where_time_nodes(&self) -> Vec<&PlanNode> {
        self.nodes
            .iter()
            .filter(|&node| {
                match *node {
                    PlanNode::Where(ref col_name, _, _) => &col_name.column == "time",
                    _ => false,
                }
            })
            .collect()
    }

    fn find_by_table(&self, table: &str) -> Vec<&PlanNode> {
        self.nodes
            .iter()
            .filter(|node| node.table() == table)
            .collect()
    }
}

impl Default for Stage {
    fn default() -> Stage {
        Stage::new(HashSet::new())
    }
}

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
    pub stages: Vec<Stage>,
}

impl Plan {
    pub fn new(lines: Vec<QueryLine>) -> Plan {
        let graph = Self::build_graph(lines);
        let stages = Self::build_stages(&graph);

        println!("{}", Dot::new(&graph));

        let mut plan = Plan { stages: stages };
        plan.optimize();
        plan
    }

    pub fn is_valid(&self) -> Result<(), Error> {
        if self.stages.len() == 0 {
            return Err(Error::NoStages);
        }

        if self.stages.iter().any(|s| s.is_empty()) {
            return Err(Error::EmptyStages);
        }

        let stage_query_types = self.stage_query_types();
        let stages_len = stage_query_types.len();

        for (index, types) in stage_query_types.iter().enumerate() {
            if types.contains(&0) {
                return Err(Error::EmptyNodeInStages);
            }

            if index == stages_len - 1 &&
               (types.contains(&2) || types.contains(&3) || types.contains(&4)) {
                return Err(Error::InvalidStageOrder);
            }

            if index < stages_len - 1 && types.contains(&1) {
                return Err(Error::InvalidStageOrder);
            }
        }

        Ok(())
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

    fn build_stages(graph: &Graph<PlanNode, ColumnName>) -> Vec<Stage> {
        let mut stages = vec![];

        for external in graph.externals(EdgeDirection::Incoming) {
            let mut dfs = Dfs::new(graph, external);
            while let Some(node) = dfs.next(graph) {
                let mut max_depth = -1;
                let provides = graph.neighbors_directed(node, EdgeDirection::Incoming);

                for provide in provides {
                    match Self::find_stage_index(&stages, &graph[provide]) {
                        Some(stage_index) => max_depth = cmp::max(max_depth, stage_index as isize),
                        _ => continue,
                    }
                }

                let stage_index = (max_depth + 1) as usize;

                if stage_index >= stages.len() {
                    stages.push(Stage::default())
                }
                stages[stage_index].insert(graph[node].clone());
            }
        }

        stages.reverse();
        stages
    }

    fn optimize(&mut self) {
        self.stages = self.stages
                          .iter()
                          .map(Self::combine_where_nodes_on_same_column)
                          .map(|s| Self::set_time_bounds_on_where_nodes(&s))
                          .collect::<Vec<Stage>>();
    }

    fn combine_where_nodes_on_same_column(stage: &Stage) -> Stage {
        let mut new = stage.clone();
        let groups = stage.group_where_nodes_by_column();

        for group in groups {
            if group.len() > 1 {
                new.replace(&group, vec![Self::group_nodes_into_and_predicate(&group)])
            }
        }

        new
    }

    fn set_time_bounds_on_where_nodes(stage: &Stage) -> Stage {
        let mut new = stage.clone();
        let time_nodes = stage.find_where_time_nodes();

        for time_node in time_nodes {
            let (col_name, predicate) = match *time_node {
                PlanNode::Where(ref col_name, ref predicate, _) => (col_name, predicate),
                _ => panic!("Invalid time_node"),
            };
            let bound = TimeBound::from_predicate(predicate);
            let group = stage.find_by_table(&col_name.table)
                             .into_iter()
                             .filter(|&node| {
                                 match *node {
                                     PlanNode::Where(_, _, _) => true,
                                     _ => false,
                                 }
                             })
                             .collect::<Vec<&PlanNode>>();

            if group.len() == 1 {
                new.replace(&[time_node],
                            vec![PlanNode::Where(col_name.to_owned(),
                                                 predicate.to_owned(),
                                                 Some(bound))]);
            } else {
                let new_nodes = group.iter()
                                     .filter(|&n| *n != time_node)
                                     .map(|&node| {
                                         match *node {
                                             PlanNode::Where(ref c, ref p, _) => {
                                                 PlanNode::Where(c.to_owned(),
                                                                 p.to_owned(),
                                                                 Some(bound.clone()))
                                             }
                                             _ => panic!(),
                                         }
                                     })
                                     .collect();

                new.replace(&group, new_nodes)
            }
        }

        new
    }

    fn group_nodes_into_and_predicate(group: &[&PlanNode]) -> PlanNode {
        let mut col_name = None;
        let mut predicate = None;

        for node in group {
            let (inner_col_name, inner_pred) = match **node {
                PlanNode::Where(ref inner_c, ref inner_p, _) => (inner_c, inner_p),
                _ => panic!("Grouped non-where node"),
            };

            col_name = match col_name {
                Some(_) => col_name,
                None => Some(inner_col_name.to_owned()),
            };

            predicate = match predicate {
                Some(pred) => Some(Predicate::And(Box::new(pred), Box::new(inner_pred.to_owned()))),
                None => Some(inner_pred.to_owned()),
            }
        }

        match (col_name, predicate) {
            (Some(c), Some(p)) => PlanNode::Where(c, p, None),
            _ => panic!("Empty group"),
        }
    }

    fn stage_query_types(&self) -> Vec<HashSet<usize>> {
        self.stages
            .iter()
            .map(|stage| {
                let mut stage_types = HashSet::new();
                for node in &stage.nodes {
                    match *node {
                        PlanNode::Select(_, _) => stage_types.insert(1),
                        PlanNode::Join(_, _) => stage_types.insert(2),
                        PlanNode::Where(_, _, _) => stage_types.insert(3),
                        PlanNode::WhereId(_, _) => stage_types.insert(4),
                    };
                }
                stage_types
            })
            .collect()
    }

    fn find_stage_index(stages: &[Stage], node: &PlanNode) -> Option<usize> {
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
            let s = stage.nodes
                         .iter()
                         .map(|node| format!("{}", node))
                         .collect::<Vec<String>>();

            if idx != 0 {
                try!(write!(f, "      "));
            }
            try!(write!(f, "[ {} ]\n", s.join(", ")));
        }
        Ok(())
    }
}

impl From<grammar::ParseError> for Error {
    fn from(err: grammar::ParseError) -> Error {
        Error::ParseError(err)
    }
}
