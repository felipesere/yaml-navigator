#![allow(unused_macros)]
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_yaml::Value;

macro_rules! step {
    ("*") => {
        Step::All
    };
    ($a:expr) => {
        Step::from($a)
    };
}

macro_rules! query {
    ($($a:expr $(,)?)+) => {{
        let mut the_query = Query::default();
        $(
            the_query.steps.push(step!($a));
        )+
        the_query
    }};
}

macro_rules! r#where {
    ($field:literal => $body:expr) => {{
        Step::filter($field, $body)
    }};
}

macro_rules! sub {
    ($field:literal => $sub_query:expr) => {{
        Step::sub_query($field, $sub_query)
    }};
}

macro_rules! and {
    ($($a:expr $(,)?)+) => {{
        let mut arms: Vec<Query> = Vec::new();
        $(
            arms.push($a);
        )+
        Step::And(arms)
    }};
}

#[derive(Clone, Debug, Default)]
pub struct Query {
    pub steps: Vec<Step>,
}

impl Query {
    fn take_step(&self) -> Option<(Step, Query)> {
        if self.steps.is_empty() {
            return None;
        };
        let mut others = self.steps.clone();
        let next = others.remove(0);
        Some((next, Query { steps: others }))
    }
}

#[derive(Clone)]
pub enum Step {
    Field(String),
    And(Vec<Query>),
    At(usize),
    All,
    Filter(String, Arc<dyn Fn(&serde_yaml::Value) -> bool>),
    SubQuery(String, Query),
    Range(Range<usize>),
}

impl From<String> for Step {
    fn from(value: String) -> Self {
        if value == "*" {
            Step::All
        } else {
            Step::Field(value.into())
        }
    }
}

impl From<&str> for Step {
    fn from(value: &str) -> Self {
        let val = value.to_string();
        Step::from(val)
    }
}

impl From<usize> for Step {
    fn from(value: usize) -> Self {
        Step::At(value)
    }
}

impl From<Range<usize>> for Step {
    fn from(value: Range<usize>) -> Self {
        Step::Range(value)
    }
}

impl Step {
    fn name(&self) -> &'static str {
        match self {
            Step::Field(_) => "field",
            Step::And(_) => "and",
            Step::At(_) => "at",
            Step::Range(_) => "range",
            Step::All => "all",
            Step::Filter(_, _) => "filter",
            Step::SubQuery(_, _) => "sub_query",
        }
    }

    pub fn at(value: usize) -> Step {
        Step::At(value)
    }

    pub fn range(value: Range<usize>) -> Step {
        Step::Range(value)
    }

    pub fn field<S: Into<String>>(value: S) -> Step {
        Step::Field(value.into())
    }

    pub fn filter<D: DeserializeOwned, F: Fn(D) -> bool + 'static>(
        field: impl Into<String>,
        fun: F,
    ) -> Step {
        Step::Filter(
            field.into(),
            Arc::new(move |value: &serde_yaml::Value| {
                let Ok(actual) = serde_yaml::from_value(value.clone()) else {
                    return false;
                };
                fun(actual)
            }),
        )
    }

    pub fn sub_query(field: impl Into<String>, query: Query) -> Step {
        Step::SubQuery(field.into(), query)
    }
}

impl std::fmt::Debug for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Field(arg0) => f.debug_tuple("Field").field(arg0).finish(),
            Self::And(arg0) => f.debug_tuple("And").field(arg0).finish(),
            Self::At(arg0) => f.debug_tuple("Index").field(arg0).finish(),
            Self::All => write!(f, "All"),
            Self::Range(r) => f.debug_tuple("Range").field(r).finish(),
            Self::Filter(arg0, _arg1) => f.debug_tuple("Filter").field(arg0).finish(),
            Self::SubQuery(arg, _arg2) => f.debug_tuple("SubQuery").field(arg).finish(),
        }
    }
}

struct Paths<'input> {
    starting_point: &'input Value,
    query: Query,
}

pub struct ManyResults<'input> {
    paths_to_explore: VecDeque<Paths<'input>>,
}

impl<'input> Iterator for ManyResults<'input> {
    type Item = &'input Value;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(path_to_explore) = self.paths_to_explore.pop_front() {
            match dive(path_to_explore) {
                DiveOutcome::Final(value) => return Some(value),
                DiveOutcome::Branch(more_paths_to_consider) => {
                    self.paths_to_explore.extend(more_paths_to_consider);
                }
                DiveOutcome::Nothing => {}
            };
        }
        None
    }
}

enum DiveOutcome<'input> {
    Nothing,
    Final(&'input Value),
    Branch(Vec<Paths<'input>>),
}

fn dive(path: Paths<'_>) -> DiveOutcome<'_> {
    let Some((next_step, remaining_query)) = path.query.take_step() else {
        return DiveOutcome::Final(path.starting_point);
    };

    tracing::info!("Matching {next_step:?} against {:?}", path.starting_point);
    match (next_step, path.starting_point) {
        (Step::Field(f), Value::Mapping(m)) => {
            let accessor = Value::String(f);
            let Some(next_value) = m.get(accessor) else {
                return DiveOutcome::Nothing;
            };
            dive(Paths {
                starting_point: next_value,
                query: remaining_query,
            })
        }
        (Step::At(idx), Value::Sequence(s)) => {
            let Some(next_value) = s.get(idx) else {
                return DiveOutcome::Nothing;
            };
            dive(Paths {
                starting_point: next_value,
                query: remaining_query,
            })
        }
        (Step::Range(r), Value::Sequence(sequence)) => {
            let mut additional_paths = Vec::new();
            for point in &sequence[r] {
                additional_paths.push(Paths {
                    starting_point: point,
                    query: remaining_query.clone(),
                });
            }
            DiveOutcome::Branch(additional_paths)
        }
        (Step::All, Value::Sequence(sequence)) => {
            let mut additional_paths = Vec::new();
            for point in sequence {
                additional_paths.push(Paths {
                    starting_point: point,
                    query: remaining_query.clone(),
                });
            }
            DiveOutcome::Branch(additional_paths)
        }
        (Step::Filter(field, predicate), s @ Value::String(_)) => {
            if field != "." {
                return DiveOutcome::Nothing;
            }

            if !predicate(s) {
                return DiveOutcome::Nothing;
            }

            dive(Paths {
                starting_point: path.starting_point,
                query: remaining_query,
            })
        }
        (Step::Filter(field, predicate), Value::Sequence(sequence)) => {
            let mut additional_paths = Vec::new();
            for val in sequence {
                let value_to_check = if field == "." {
                    val
                } else {
                    let Some(value) = val.get(&field) else {
                        return DiveOutcome::Nothing
                    };
                    value
                };

                if !predicate(value_to_check) {
                    continue;
                }

                additional_paths.push(Paths {
                    starting_point: val,
                    query: remaining_query.clone(),
                })
            }

            DiveOutcome::Branch(additional_paths)
        }
        (Step::Filter(field, predicate), Value::Mapping(m)) => {
            let value_to_check = if field == "." {
                path.starting_point
            } else {
                let Some(value) = m.get(field) else {
                    return DiveOutcome::Nothing
                };
                value
            };

            if !predicate(value_to_check) {
                return DiveOutcome::Nothing;
            }

            dive(Paths {
                starting_point: path.starting_point,
                query: remaining_query,
            })
        }
        (Step::SubQuery(field, sub_query), Value::Mapping(m)) => {
            let value_to_check = if field == "." {
                path.starting_point
            } else {
                let Some(value) = m.get(field) else {
                    return DiveOutcome::Nothing
                };
                value
            };

            if navigate_iter(value_to_check, sub_query).next().is_none() {
                return DiveOutcome::Nothing;
            }
            dive(Paths {
                starting_point: path.starting_point,
                query: remaining_query,
            })
        }
        (Step::And(sub_queries), Value::Mapping(_m)) => {
            let value = path.starting_point;
            let all_match = sub_queries
                .iter()
                .all(|q| dbg!(navigate_iter(value, q.clone()).next().is_some()));

            if !all_match {
                return DiveOutcome::Nothing;
            }
            dive(Paths {
                starting_point: path.starting_point,
                query: remaining_query,
            })
        }
        (step, value) => {
            let step = step.name();
            let value = value_name(value);
            tracing::warn!("'{step}' not supported for '{value}'",);

            DiveOutcome::Nothing
        }
    }
}

fn value_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Sequence(_) => "sequence",
        Value::Mapping(_) => "mapping",
        Value::Tagged(_) => "tagged",
    }
}

pub fn navigate_iter(input: &Value, query: Query) -> ManyResults<'_> {
    ManyResults {
        paths_to_explore: VecDeque::from([Paths {
            starting_point: input,
            query,
        }]),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use indoc::indoc;

    use super::*;

    #[test]
    fn it_works() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                  surname: Sere
                  age: 32
                  address:
                    street: Foo
                    postcode: 12345
                    city: Legoland
                  hobbies:
                    - tennis
                    - computer
                - name: Charlotte
                  surname: Fereday
                  age: 31
                  address:
                    street: Bar
                    postcode: 12345
                    city: Legoland
                  sports:
                   - swimming
                   - yoga
            "#};
        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let first_persons_name = query!["people", 0, "name",];

        let felipe = navigate_iter(&yaml, first_persons_name).next().unwrap();
        assert_eq!(felipe, &Value::String("Felipe".into()));

        let yoga = query!("people", "*", "sports", 1,);

        let yoga: Vec<_> = navigate_iter(&yaml, yoga).collect();
        assert_eq!(yoga, vec![&Value::String("yoga".to_string())]);
    }

    #[test]
    fn range_of_indizes() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                - name: Charlotte
                - name: Alice
                - name: Bob
                - name: Malory
        "#};

        let query = query!("people", 1..4, "name",);

        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let names: Vec<_> = navigate_iter(&yaml, query)
            .filter_map(|v| v.as_str())
            .collect();

        assert_eq!(names, vec!["Charlotte", "Alice", "Bob"]);
    }

    #[test]
    fn filter_clause() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                  surname: Sere
                  age: 32
                  address:
                    street: Foo
                    postcode: 12345
                    city: Legoland
                  hobbies:
                    - tennis
                    - computer
                - name: Charlotte
                  surname: Fereday
                  age: 31
                  address:
                    street: Bar
                    postcode: 12345
                    city: Legoland
                  sports:
                   - swimming
                   - yoga
            "#};
        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let names_of_people_aged_over_31 =
            query!["people", r#where!("age" => |age: u32| age > 30), "name",];

        let felipe = navigate_iter(&yaml, names_of_people_aged_over_31)
            .next()
            .unwrap();
        assert_eq!(felipe, &Value::String("Felipe".into()));
    }

    #[test]
    fn find_nothing() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                  surname: Sere
                  age: 32
                  address:
                    street: Foo
                    postcode: 12345
                    city: Legoland
                  hobbies:
                    - tennis
                    - computer
                - name: Charlotte
                  surname: Fereday
                  age: 31
                  address:
                    street: Bar
                    postcode: 12345
                    city: Legoland
                  sports:
                   - swimming
                   - yoga
            "#};
        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let missing_property = query!["people", "*", "car"];

        let no_one = navigate_iter(&yaml, missing_property).next();
        assert!(no_one.is_none());

        let filter_does_not_match = query!["people", r#where!("age" => |age: u32| age < 4),];

        let no_one = navigate_iter(&yaml, filter_does_not_match).next();
        assert!(no_one.is_none());
    }

    #[test]
    fn filter_on_mapping() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                  surname: Sere
                  age: 32
                  address:
                    street: Foo
                    postcode: 12345
                    city: Legoland
                  hobbies:
                    - tennis
                    - computer
                - name: Charlotte
                  surname: Fereday
                  age: 31
                  address:
                    street: Bar
                    postcode: 12345
                    city: Legoland
                  sports:
                   - swimming
                   - yoga
            "#};
        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let finds_address = query![
            "people",
            "*",
            "address",
            r#where!("street" => |street: String| street == "Foo")
        ];

        let felipe: Vec<_> = navigate_iter(&yaml, finds_address).collect();
        assert_eq!(felipe.len(), 1);
    }

    #[test]
    fn sub_query_for_filter() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                  surname: Sere
                  age: 32
                  address:
                    street: Foo
                    postcode: 12345
                    city: Legoland
                  hobbies:
                    - tennis
                    - computer
                - name: Charlotte
                  surname: Fereday
                  age: 31
                  address:
                    street: Bar
                    postcode: 12345
                    city: Legoland
                  sports:
                   - swimming
                   - yoga
            "#};
        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let live_on_foo_street = query![
            "address",
            r#where!("street" => |street: String| street == "Foo"),
        ];

        let finds_address = query!["people", "*", sub!("." => live_on_foo_street),];

        let felipe: Vec<_> = navigate_iter(&yaml, finds_address).collect();
        assert_eq!(felipe.len(), 1);
    }

    #[tracing_test::traced_test]
    #[test]
    fn logical_and_connecting_two_subqueries() {
        let raw = indoc! {r#"
            people:
                - name: Felipe
                  surname: Sere
                  age: 32
                  address:
                    street: Foo
                    postcode: 12345
                    city: Legoland
                  hobbies:
                    - tennis
                    - computer
                - name: Charlotte
                  surname: Fereday
                  age: 31
                  address:
                    street: Bar
                    postcode: 12345
                    city: Legoland
                  sports:
                   - swimming
                   - yoga
            "#};
        let yaml: Value = serde_yaml::from_str(raw).unwrap();

        let finds_address = query![
            "people",
            "*",
            and![
                query!(
                    "address",
                    r#where!("street" => |name: String| name == "Foo")
                ),
                query!(
                    "hobbies",
                    r#where!("." => |hobby: String| hobby == "tennis")
                ),
            ]
        ];

        let felipe: Vec<_> = navigate_iter(&yaml, finds_address).collect();
        assert_eq!(felipe.len(), 1);
    }
}
