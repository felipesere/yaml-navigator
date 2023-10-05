#![allow(dead_code)]
use std::collections::VecDeque;
use std::fmt::Write;

use serde_yaml::Value;

use crate::{get, value_name, Candidate, Query, SearchKind, Step};

#[derive(Clone, Default, Debug)]
pub(crate) struct Address(pub(crate) Vec<LocationFragment>);

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Address[")?;
        for location in &self.0 {
            f.write_char('.')?;
            location.fmt(f)?;
        }
        f.write_str("]")?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) enum LocationFragment {
    Field(String),
    Index(usize),
}

impl std::fmt::Display for LocationFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocationFragment::Field(s) => f.write_str(s),
            LocationFragment::Index(i) => f.write_str(i.to_string().as_str()),
        }
    }
}

impl From<&String> for LocationFragment {
    fn from(value: &String) -> Self {
        Self::Field(value.clone())
    }
}

impl From<String> for LocationFragment {
    fn from(value: String) -> Self {
        Self::Field(value)
    }
}

impl From<usize> for LocationFragment {
    fn from(value: usize) -> Self {
        Self::Index(value)
    }
}

impl Address {
    pub(crate) fn extend(&self, fragment: impl Into<LocationFragment>) -> Address {
        let mut this = self.clone();
        this.0.push(fragment.into());
        this
    }

    pub(crate) fn append(&self, mut relative_address: Address) -> Address {
        let mut this = self.clone();
        this.0.append(&mut relative_address.0);
        this
    }
}

fn matching_addresses(root_node: &Value, query: Query) -> Vec<Address> {
    let mut candidates = VecDeque::from_iter([Candidate {
        starting_point: Address::default(),
        remaining_query: query,
        search_kind: crate::SearchKind::default(),
    }]);
    let mut addresses = Vec::new();

    while let Some(path_to_explore) = candidates.pop_front() {
        let found = find_more_addresses(path_to_explore, root_node);
        if let Some(address) = found.hit {
            addresses.push(address);
        }
        candidates.extend(found.branching);
    }

    addresses
}

/// The outcome of advancing the the query
#[derive(Debug)]
pub(crate) struct FindAddresses {
    /// there was a path that matched the query at this `Address`
    pub(crate) hit: Option<Address>,
    /// multiple viable candidates were found
    pub(crate) branching: Vec<Candidate>,
}

impl FindAddresses {
    fn hit(address: Address) -> FindAddresses {
        FindAddresses {
            hit: Some(address),
            branching: Vec::default(),
        }
    }

    fn nothing() -> FindAddresses {
        FindAddresses {
            hit: None,
            branching: Vec::default(),
        }
    }

    fn branching(candidates: Vec<Candidate>) -> FindAddresses {
        FindAddresses {
            hit: None,
            branching: candidates,
        }
    }
}

pub(crate) fn find_more_addresses(path: Candidate, root: &Value) -> FindAddresses {
    let current_address = path.starting_point;
    tracing::debug!("Looking at {current_address}");
    // Are we at the end of the query?
    let Some((next_step, remaining_query)) = path.remaining_query.take_step() else {
        return FindAddresses::hit(current_address);
    };
    tracing::debug!("The next step to take is {next_step}");

    // if not, can we get the node for the current address?
    let Some(node) = get(root, &current_address) else {
        return FindAddresses::nothing();
    };

    // if our `Candidate` is a recursive, we already know we need to add
    // all possible fields/indices as additional candidates
    // and keep searching with the normal query...
    let mut additional_paths = Vec::new();
    if let SearchKind::Recursive(recursive_query) = &path.search_kind {
        if let Some(m) = node.as_mapping() {
            for (key, _) in m {
                let field = key.as_str().unwrap().to_string();
                tracing::debug!("Adding a candidate for \"{field}\" in mapping for recursion from {current_address}");
                additional_paths.push(Candidate {
                    starting_point: current_address.extend(field),
                    remaining_query: recursive_query.clone(),
                    search_kind: path.search_kind.clone(),
                })
            }
        }
        if let Some(s) = node.as_sequence() {
            for idx in 0..s.len() {
                tracing::debug!(
                    "Adding a candidate for \"{idx}\" in sequence for recursion from {current_address}"
                );
                additional_paths.push(Candidate {
                    starting_point: current_address.extend(idx),
                    remaining_query: recursive_query.clone(),
                    search_kind: path.search_kind.clone(),
                })
            }
        }
    }

    tracing::debug!("Checking pairing between {next_step} and {node:?}");
    let mut next = 'match_block: {
        match (next_step, node) {
            (Step::Field(f), Value::Mapping(m)) => {
                if m.get(&f).is_none() {
                    FindAddresses::nothing()
                } else {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.extend(&f),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                }
            }
            (Step::At(idx), Value::Sequence(s)) => {
                if s.get(idx).is_none() {
                    FindAddresses::nothing()
                } else {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.extend(idx),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                }
            }
            (Step::Range(r), Value::Sequence(_)) => {
                let mut additional_paths = Vec::new();
                for point in r {
                    additional_paths.push(Candidate {
                        starting_point: current_address.extend(point),
                        remaining_query: remaining_query.clone(),
                        search_kind: crate::SearchKind::default(),
                    });
                }
                FindAddresses::branching(additional_paths)
            }
            (Step::All, Value::Sequence(sequence)) => {
                let mut additional_paths = Vec::new();
                for point in 0..sequence.len() {
                    additional_paths.push(Candidate {
                        starting_point: current_address.extend(point),
                        remaining_query: remaining_query.clone(),
                        search_kind: crate::SearchKind::default(),
                    });
                }
                FindAddresses::branching(additional_paths)
            }
            (Step::Filter(field, predicate), s @ Value::String(_)) => {
                if field != "." || !predicate(s) {
                    FindAddresses::nothing()
                } else {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.extend(&field),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                }
            }
            (Step::Filter(field, predicate), Value::Sequence(sequence)) => {
                let mut additional_paths = Vec::new();
                for (idx, val) in sequence.iter().enumerate() {
                    let value_to_check = if field == "." {
                        val
                    } else {
                        let Some(value) = val.get(&field) else {
                            break 'match_block FindAddresses::nothing();
                        };
                        value
                    };

                    if !predicate(value_to_check) {
                        continue;
                    }

                    additional_paths.push(Candidate {
                        starting_point: current_address.extend(idx),
                        remaining_query: remaining_query.clone(),
                        search_kind: crate::SearchKind::default(),
                    })
                }

                FindAddresses::branching(additional_paths)
            }
            (Step::Filter(field, predicate), val @ Value::Mapping(_)) => {
                let value_to_check = if field == "." {
                    val
                } else {
                    let Some(value) = val.as_mapping().unwrap().get(&field) else {
                        break 'match_block FindAddresses::nothing();
                    };
                    value
                };

                if predicate(value_to_check) {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.extend(&field),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                } else {
                    FindAddresses::nothing()
                }
            }
            (Step::SubQuery(field, sub_query), val @ Value::Mapping(_)) => {
                let value_to_check = if field == "." {
                    val
                } else {
                    let Some(value) = val.as_mapping().unwrap().get(&field) else {
                        break 'match_block FindAddresses::nothing();
                    };
                    value
                };

                if matching_addresses(value_to_check, sub_query).is_empty() {
                    FindAddresses::nothing()
                } else {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.extend(&field),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                }
            }
            (Step::And(sub_queries), val @ Value::Mapping(_)) => {
                let value = val;
                let all_match = sub_queries
                    .iter()
                    .all(|q| !matching_addresses(value, q.clone()).is_empty());

                if !all_match {
                    FindAddresses::nothing()
                } else {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.clone(),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                }
            }
            (Step::Or(sub_queries), val @ Value::Mapping(_)) => {
                let value = val;
                let any_match = sub_queries
                    .iter()
                    .any(|q| !matching_addresses(value, q.clone()).is_empty());

                if !any_match {
                    FindAddresses::nothing()
                } else {
                    find_more_addresses(
                        Candidate {
                            starting_point: current_address.clone(),
                            remaining_query,
                            search_kind: crate::SearchKind::default(),
                        },
                        root,
                    )
                }
            }
            (Step::Branch(sub_queries), value @ Value::Mapping(_)) => {
                let mut additional_paths = Vec::new();
                for sub_query in sub_queries {
                    for relative_address in matching_addresses(value, sub_query) {
                        additional_paths.push(Candidate {
                            starting_point: current_address.append(relative_address),
                            remaining_query: remaining_query.clone(),
                            search_kind: crate::SearchKind::default(),
                        })
                    }
                }

                FindAddresses::branching(additional_paths)
            }
            (Step::Recursive, _) => {
                // Fall into recursive mode...
                FindAddresses::branching(vec![Candidate {
                    starting_point: current_address,
                    remaining_query: remaining_query.clone(),
                    search_kind: crate::SearchKind::Recursive(remaining_query.clone()),
                }])
            }
            (step, value) => {
                let step = step.name();
                let value = value_name(value);
                tracing::debug!("'{step}' not supported for '{value}'",);

                FindAddresses::nothing()
            }
        }
    };

    tracing::debug!("This is the outcome of running the step: {next:?}");

    next.branching.extend(additional_paths);
    next
}
