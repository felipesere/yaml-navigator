use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use address::{find_more_addresses, Address};
use serde::de::DeserializeOwned;
use serde_yaml::Value;

use crate::address::LocationFragment;

mod address;
pub use gat_lending_iterator::LendingIterator;

#[macro_export]
macro_rules! step {
    ("*") => {
        $crate::Step::All
    };
    ($a:expr) => {
        $crate::Step::from($a)
    };
}

#[macro_export]
macro_rules! query {
    ($($a:expr $(,)?)+) => {{
        let mut the_query = $crate::Query::default();
        $(
            the_query.steps.push($crate::step!($a));
        )+
        the_query
    }};
}

#[macro_export]
macro_rules! r#where {
    ($field:literal => $body:expr) => {{
        $crate::Step::filter($field, $body)
    }};
}

#[macro_export]
macro_rules! sub {
    ($field:literal => $sub_query:expr) => {{
        $crate::Step::sub_query($field, $sub_query)
    }};
}

#[macro_export]
macro_rules! and {
    ($($a:expr $(,)?)+) => {{
        let mut arms: Vec<$crate::Query> = Vec::new();
        $(
            arms.push($a);
        )+
        $crate::Step::And(arms)
    }};
}

#[macro_export]
macro_rules! or {
    ($($a:expr $(,)?)+) => {{
        let mut arms: Vec<$crate::Query> = Vec::new();
        $(
            arms.push($a);
        )+
        $crate::Step::Or(arms)
    }};
}

#[macro_export]
macro_rules! branch {
    ($($a:expr $(,)?)+) => {{
        let mut arms: Vec<$crate::Query> = Vec::new();
        $(
            arms.push($a.into());
        )+
        $crate::Step::Branch(arms)
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
    Recursive,
    Field(String),
    And(Vec<Query>),
    Or(Vec<Query>),
    Branch(Vec<Query>),
    At(usize),
    All,
    Filter(String, Arc<dyn Fn(&serde_yaml::Value) -> bool>),
    SubQuery(String, Query),
    Range(Range<usize>),
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

impl From<String> for Step {
    fn from(value: String) -> Self {
        match value.as_str() {
            "*" => Step::All,
            "..." => Step::Recursive,
            _ => Step::Field(value),
        }
    }
}

impl From<&str> for Step {
    fn from(value: &str) -> Self {
        let val = value.to_string();
        Step::from(val)
    }
}

impl From<&str> for Query {
    fn from(value: &str) -> Self {
        Query {
            steps: vec![value.into()],
        }
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
            Step::Recursive => "recursive",
            Step::Field(_) => "field",
            Step::And(_) => "and",
            Step::Or(_) => "or",
            Step::Branch(_) => "branch",
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
            Self::Branch(arg0) => f.debug_tuple("Branch").field(arg0).finish(),
            Self::Or(arg0) => f.debug_tuple("Or").field(arg0).finish(),
            Self::At(arg0) => f.debug_tuple("Index").field(arg0).finish(),
            Self::All => write!(f, "All"),
            Self::Range(r) => f.debug_tuple("Range").field(r).finish(),
            Self::Filter(arg0, _arg1) => f.debug_tuple("Filter").field(arg0).finish(),
            Self::SubQuery(arg, _arg2) => f.debug_tuple("SubQuery").field(arg).finish(),
            Self::Recursive => write!(f, "Recursive"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Candidate {
    pub(crate) starting_point: Address,
    pub(crate) remaining_query: Query,
    pub(crate) search_kind: SearchKind,
}

/// The kind of search process we are in
#[derive(Debug, Default, Clone)]
pub(crate) enum SearchKind {
    /// Just searching from the root of the tree downwards
    #[default]
    Normal,
    /// A recursive search where the `Query` can match from any point downwards
    Recursive(Query),
}

pub struct ManyResults<'input> {
    candidates: VecDeque<Candidate>,
    root_node: &'input Value,
}

/// A bit `Context` that goes along with
/// the `serde_yaml::Value` that are yielded
/// when iterating over a document.
#[derive(Debug)]
pub struct Context {
    pub path: Path,
}

#[derive(Debug)]
pub struct Path(Vec<LocationFragment>);

impl Path {
    /// Represents the path as a jq-ish
    /// string like `.foo[12].bar`
    pub fn as_jq(&self) -> String {
        let mut buf = "".to_string();
        for s in &self.0 {
            match s {
                LocationFragment::Field(f) => {
                    buf.push_str(".");
                    buf.push_str(&f);
                }
                LocationFragment::Index(at) => {
                    buf.push_str("[");
                    buf.push_str(&at.to_string());
                    buf.push_str("]");
                }
            }
        }
        buf
    }
}

impl From<Address> for Path {
    fn from(value: Address) -> Self {
        Self(value.0)
    }
}

impl<'input> Iterator for ManyResults<'input> {
    type Item = (Context, &'input Value);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(path_to_explore) = self.candidates.pop_front() {
            tracing::trace!(
                "Next candidate to explore: '{}' with query: '{:?}'",
                path_to_explore.starting_point,
                path_to_explore.remaining_query,
            );
            let found = find_more_addresses(path_to_explore, self.root_node);
            tracing::trace!("Found something...");
            self.candidates.extend(found.branching);

            if let Some(address) = found.hit {
                tracing::trace!("We got a hit: {address}");
                let node = get(self.root_node, &address);
                if let Some(v) = node {
                    return Some((
                        Context {
                            path: address.into(),
                        },
                        v,
                    ));
                }
            };
        }

        None
    }
}

fn get_mut<'a>(node: &'a mut Value, adr: &Address) -> Option<&'a mut Value> {
    let mut current_node = Some(node);
    for fragment in &adr.0 {
        let actual_node = current_node?;
        match fragment {
            LocationFragment::Field(f) if f == "." => current_node = Some(actual_node),
            LocationFragment::Field(f) => current_node = actual_node.get_mut(f),
            LocationFragment::Index(i) => current_node = actual_node.get_mut(i),
        }
    }

    current_node
}

fn get<'a>(node: &'a Value, adr: &Address) -> Option<&'a Value> {
    let mut current_node = Some(node);
    for fragment in &adr.0 {
        let actual_node = current_node?;
        match fragment {
            LocationFragment::Field(f) if f == "." => current_node = Some(actual_node),
            LocationFragment::Field(f) => current_node = actual_node.get(f),
            LocationFragment::Index(i) => current_node = actual_node.get(i),
        }
    }

    current_node
}

pub struct ManyMutResults<'input> {
    candidates: VecDeque<Candidate>,
    // the addresses here have to be relative to the root
    found_addresses: VecDeque<Address>,
    root_node: &'input mut Value,
}

impl<'input> gat_lending_iterator::LendingIterator for ManyMutResults<'input> {
    type Item<'a> = (Context, &'a mut Value)
        where
            Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        while let Some(path_to_explore) = self.candidates.pop_front() {
            tracing::trace!("Looking at {}", path_to_explore.starting_point);
            let found = find_more_addresses(path_to_explore, self.root_node);
            self.candidates.extend(found.branching);

            if let Some(address) = found.hit {
                tracing::trace!("We got a hit: {address}");
                self.found_addresses.push_back(address);
            }
        }

        while let Some(address) = self.found_addresses.pop_front() {
            // SAFETY: see https://docs.rs/polonius-the-crab/0.3.1/polonius_the_crab/#the-arcanemagic
            let self_ = unsafe { &mut *(self as *mut Self) };
            if let Some(found_node) = get_mut(self_.root_node, &address) {
                return Some((
                    Context {
                        path: address.into(),
                    },
                    found_node,
                ));
            }
        }
        None
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

pub fn navigate_iter(root_node: &Value, query: Query) -> ManyResults<'_> {
    ManyResults {
        root_node,
        candidates: VecDeque::from([Candidate {
            starting_point: Address::default(),
            remaining_query: query,
            search_kind: SearchKind::Normal,
        }]),
    }
}

pub fn navigate_iter_mut(input: &mut Value, query: Query) -> ManyMutResults<'_> {
    ManyMutResults {
        root_node: input,
        candidates: VecDeque::from_iter([Candidate {
            starting_point: Address::default(),
            remaining_query: query,
            search_kind: SearchKind::Normal,
        }]),
        found_addresses: VecDeque::default(),
    }
}

#[cfg(test)]
mod tests {
    use gat_lending_iterator::LendingIterator;
    use std::assert_eq;
    use tracing_test::traced_test;

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

        let (ctx, felipe) = navigate_iter(&yaml, first_persons_name).next().unwrap();
        assert_eq!(felipe, &Value::String("Felipe".into()));
        assert_eq!(&ctx.path.as_jq(), ".people[0].name");

        let yoga = query!("people", "*", "sports", 1,);

        let yoga: Vec<_> = navigate_iter(&yaml, yoga)
            .map(|(ctx, val)| (ctx.path.as_jq(), val))
            .collect();
        assert_eq!(
            yoga,
            vec![(
                ".people[1].sports[1]".to_string(),
                &Value::String("yoga".to_string())
            )]
        );
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
            .filter_map(|(_ctx, v)| v.as_str())
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

        let (_, felipe) = navigate_iter(&yaml, names_of_people_aged_over_31)
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

        let age_of_people_living_on_foo_street_playing_tennis = query![
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
            ],
            "age"
        ];

        let felipe: Vec<_> =
            navigate_iter(&yaml, age_of_people_living_on_foo_street_playing_tennis)
                .map(|(_ctx, v)| v)
                .collect();
        assert_eq!(felipe, vec![&serde_yaml::Value::Number(32.into())]);
    }

    #[test]
    fn logical_or_for_alternatives() {
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

        let computer_or_swimming = query![
            "people",
            "*",
            or![
                query!(
                    "hobbies",
                    r#where!("." => |name: String| name == "computer")
                ),
                query!(
                    "sports",
                    r#where!("." => |sport: String| sport == "swimming")
                ),
            ],
            "age"
        ];

        let both: Vec<_> = navigate_iter(&yaml, computer_or_swimming)
            .map(|(_ctx, v)| v)
            .collect();
        assert_eq!(
            both,
            vec![
                &serde_yaml::Value::Number(32.into()),
                &serde_yaml::Value::Number(31.into())
            ]
        );
    }

    #[test]
    fn modify_found_values() {
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
        let mut yaml: Value = serde_yaml::from_str(raw).unwrap();

        let felipes_name = query![
            "people",
            "*",
            r#where!("name" => |name: String| name == "Felipe"),
        ];

        {
            let mut iter = navigate_iter_mut(&mut yaml, felipes_name);

            while let Some((_, name)) = iter.next() {
                *name = serde_yaml::Value::from("epileF");
            }
        }

        let modified = serde_yaml::to_string(&yaml).unwrap();

        expect_test::expect![[r#"
            people:
            - name: epileF
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
        "#]]
        .assert_eq(&modified);

        let hobbies_and_sport = query!["people", "*", branch!["hobbies", "sports"], "*"];

        let all: Vec<_> = navigate_iter(&yaml, hobbies_and_sport.clone()).collect();
        assert_eq!(all.len(), 4);

        {
            let mut iter = navigate_iter_mut(&mut yaml, hobbies_and_sport.clone());
            while let Some((_ctx, hobby_or_sport)) = iter.next() {
                *hobby_or_sport = serde_yaml::Value::from("F1");
            }
        }

        let modified = serde_yaml::to_string(&yaml).unwrap();

        expect_test::expect![[r#"
            people:
            - name: epileF
              surname: Sere
              age: 32
              address:
                street: Foo
                postcode: 12345
                city: Legoland
              hobbies:
              - F1
              - F1
            - name: Charlotte
              surname: Fereday
              age: 31
              address:
                street: Bar
                postcode: 12345
                city: Legoland
              sports:
              - F1
              - F1
        "#]]
        .assert_eq(&modified);
    }

    #[test]
    #[traced_test]
    fn recursive_search() {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(indoc! {r#"
            kind: Foo
            apiVersion: v1
            metadata:
              labels:
                alpha: 1
              annotations:
                bravo: 2
            spec:
              template:
                metadata:
                  labels:
                    charlie: 3
                  annotations:
                    delta: 4
                foo:
                  bar:
                    labels:
                      echo: 5
                    annotations: 
                      foxtrott: 6
            "#})
        .unwrap();

        let annotations_everywhere = query!["...", "annotations"];

        let all: Vec<_> = navigate_iter(&yaml, annotations_everywhere.clone()).collect();
        assert_eq!(3, all.len());

        {
            let mut iter = navigate_iter_mut(&mut yaml, annotations_everywhere);
            while let Some((_ctx, annotations)) = iter.next() {
                if let Some(m) = annotations.as_mapping_mut() {
                    m.insert("new".into(), 100.into());
                };
            }
        }

        expect_test::expect![[r#"
            kind: Foo
            apiVersion: v1
            metadata:
              labels:
                alpha: 1
              annotations:
                bravo: 2
                new: 100
            spec:
              template:
                metadata:
                  labels:
                    charlie: 3
                  annotations:
                    delta: 4
                    new: 100
                foo:
                  bar:
                    labels:
                      echo: 5
                    annotations:
                      foxtrott: 6
                      new: 100
        "#]]
        .assert_eq(&serde_yaml::to_string(&yaml).unwrap());

        let labels: Vec<_> = navigate_iter(&yaml, query!["spec", "...", "labels"]).collect();
        assert_eq!(2, labels.len());
    }
}
