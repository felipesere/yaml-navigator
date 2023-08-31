use std::collections::VecDeque;

use serde_yaml::Value;

use crate::{get, value_name, Address, Candidate, Query, Step};

fn matching_addresses(root_node: &Value, query: Query) -> Vec<Address> {
    let mut candidates = VecDeque::from_iter([Candidate {
        starting_point: Address::default(),
        remaining_query: query,
    }]);
    let mut addresses = Vec::new();

    while let Some(path_to_explore) = candidates.pop_front() {
        match find_more_addresses(path_to_explore, root_node) {
            FindAddresses::Hit(address) => addresses.push(address),
            FindAddresses::Branching(more_candidates) => {
                candidates.extend(more_candidates);
            }
            FindAddresses::Nothing => {}
        };
    }

    addresses
}

pub(crate) enum FindAddresses {
    Hit(Address),
    Branching(Vec<Candidate>),
    Nothing,
}

pub(crate) fn find_more_addresses(path: Candidate, root: &Value) -> FindAddresses {
    let current_address = path.starting_point;
    // Are we at the end of the query?
    let Some((next_step, remaining_query)) = path.remaining_query.take_step() else {
        return FindAddresses::Hit(current_address);
    };

    // if not, can we get the node for the current address?
    let Some(node) = get(root, &current_address) else {
        return FindAddresses::Nothing;
    };

    match (next_step, node) {
        (Step::Field(f), Value::Mapping(m)) => {
            if m.get(&f).is_none() {
                return FindAddresses::Nothing;
            };
            find_more_addresses(
                Candidate {
                    starting_point: current_address.extend(&f),
                    remaining_query,
                },
                root,
            )
        }
        (Step::At(idx), Value::Sequence(s)) => {
            if s.get(idx).is_none() {
                return FindAddresses::Nothing;
            };
            find_more_addresses(
                Candidate {
                    starting_point: current_address.extend(idx),
                    remaining_query,
                },
                root,
            )
        }
        (Step::Range(r), Value::Sequence(_)) => {
            let mut additional_paths = Vec::new();
            for point in r {
                additional_paths.push(Candidate {
                    starting_point: current_address.extend(point),
                    remaining_query: remaining_query.clone(),
                });
            }
            FindAddresses::Branching(additional_paths)
        }
        (Step::All, Value::Sequence(sequence)) => {
            let mut additional_paths = Vec::new();
            for point in 0..sequence.len() {
                additional_paths.push(Candidate {
                    starting_point: current_address.extend(point),
                    remaining_query: remaining_query.clone(),
                });
            }
            FindAddresses::Branching(additional_paths)
        }
        (Step::Filter(field, predicate), s @ Value::String(_)) => {
            if field != "." {
                return FindAddresses::Nothing;
            }

            if !predicate(s) {
                return FindAddresses::Nothing;
            }

            find_more_addresses(
                Candidate {
                    starting_point: current_address.extend(&field),
                    remaining_query,
                },
                root,
            )
        }
        (Step::Filter(field, predicate), Value::Sequence(sequence)) => {
            let mut additional_paths = Vec::new();
            for (idx, val) in sequence.iter().enumerate() {
                let value_to_check = if field == "." {
                    val
                } else {
                    let Some(value) = val.get(&field) else {
                        return FindAddresses::Nothing;
                    };
                    value
                };

                if !predicate(value_to_check) {
                    continue;
                }

                additional_paths.push(Candidate {
                    starting_point: current_address.extend(idx),
                    remaining_query: remaining_query.clone(),
                })
            }

            FindAddresses::Branching(additional_paths)
        }
        (Step::Filter(field, predicate), val @ Value::Mapping(_)) => {
            let value_to_check = if field == "." {
                val
            } else {
                let Some(value) = val.as_mapping().unwrap().get(&field) else {
                    return FindAddresses::Nothing;
                };
                value
            };

            if !predicate(value_to_check) {
                return FindAddresses::Nothing;
            }

            find_more_addresses(
                Candidate {
                    starting_point: current_address.extend(&field),
                    remaining_query,
                },
                root,
            )
        }
        (Step::SubQuery(field, sub_query), val @ Value::Mapping(_)) => {
            let value_to_check = if field == "." {
                val
            } else {
                let Some(value) = val.as_mapping().unwrap().get(&field) else {
                    return FindAddresses::Nothing;
                };
                value
            };

            if matching_addresses(value_to_check, sub_query).is_empty() {
                return FindAddresses::Nothing;
            }
            find_more_addresses(
                Candidate {
                    starting_point: current_address.extend(&field),
                    remaining_query,
                },
                root,
            )
        }
        (Step::And(sub_queries), val @ Value::Mapping(_)) => {
            let value = val;
            let all_match = sub_queries
                .iter()
                .all(|q| !matching_addresses(value, q.clone()).is_empty());

            if !all_match {
                return FindAddresses::Nothing;
            }
            find_more_addresses(
                Candidate {
                    starting_point: current_address.clone(),
                    remaining_query,
                },
                root,
            )
        }
        (Step::Or(sub_queries), val @ Value::Mapping(_)) => {
            let value = val;
            let any_match = sub_queries
                .iter()
                .any(|q| !matching_addresses(value, q.clone()).is_empty());

            if !any_match {
                return FindAddresses::Nothing;
            }
            find_more_addresses(
                Candidate {
                    starting_point: current_address.clone(),
                    remaining_query,
                },
                root,
            )
        }
        (Step::Branch(sub_queries), value @ Value::Mapping(_)) => {
            let mut additional_paths = Vec::new();
            for sub_query in sub_queries {
                for relative_address in matching_addresses(value, sub_query) {
                    additional_paths.push(Candidate {
                        starting_point: current_address.append(relative_address),
                        remaining_query: remaining_query.clone(),
                    })
                }
            }

            FindAddresses::Branching(additional_paths)
        }
        (step, value) => {
            let step = step.name();
            let value = value_name(value);
            tracing::warn!("'{step}' not supported for '{value}'",);

            FindAddresses::Nothing
        }
    }
}
