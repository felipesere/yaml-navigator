use std::collections::VecDeque;

use serde_yaml::Value;

use crate::{get, value_name, Address, Candidate, Query, Step};

struct AddressIterator<'input> {
    candidates: VecDeque<Candidate>,
    // the addresses here have to be relative to the root
    found_addresses: VecDeque<Address>,
    root_node: &'input Value,
}

fn iter<'input>(root_node: &'input Value, query: Query) -> AddressIterator<'input> {
    AddressIterator {
        candidates: VecDeque::from_iter([Candidate {
            starting_point: Address::default(),
            remaining_query: query,
        }]),
        found_addresses: VecDeque::default(),
        root_node,
    }
}

enum FindingMoreNodes {
    Hit(Address),
    Branching(Vec<Candidate>),
    Nothing,
}

impl<'input> Iterator for AddressIterator<'input> {
    type Item = Address;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(address) = self.found_addresses.pop_front() {
            return Some(address);
        }

        while let Some(path_to_explore) = self.candidates.pop_front() {
            match find_more_addresses(path_to_explore, self.root_node) {
                FindingMoreNodes::Hit(address) => self.found_addresses.push_back(address),
                FindingMoreNodes::Branching(more_candidates) => {
                    self.candidates.extend(more_candidates);
                }
                FindingMoreNodes::Nothing => {}
            };
        }

        None
    }
}

fn find_more_addresses(path: Candidate, root: &Value) -> FindingMoreNodes {
    let current_address = path.starting_point;
    // Are we at the end of the query?
    let Some((next_step, remaining_query)) = path.remaining_query.take_step() else {
        return FindingMoreNodes::Hit(current_address);
    };

    // if not, can we get the node for the current address?
    let Some(node) = get(root, &current_address) else {
        return FindingMoreNodes::Nothing;
    };

    match (next_step, node) {
        (Step::Field(f), Value::Mapping(m)) => {
            if m.get(&f).is_none() {
                return FindingMoreNodes::Nothing;
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
                return FindingMoreNodes::Nothing;
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
            FindingMoreNodes::Branching(additional_paths)
        }
        (Step::All, Value::Sequence(sequence)) => {
            let mut additional_paths = Vec::new();
            for point in 0..sequence.len() {
                additional_paths.push(Candidate {
                    starting_point: current_address.extend(point),
                    remaining_query: remaining_query.clone(),
                });
            }
            FindingMoreNodes::Branching(additional_paths)
        }
        (Step::Filter(field, predicate), s @ Value::String(_)) => {
            if field != "." {
                return FindingMoreNodes::Nothing;
            }

            if !predicate(s) {
                return FindingMoreNodes::Nothing;
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
                        return FindingMoreNodes::Nothing;
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

            FindingMoreNodes::Branching(additional_paths)
        }
        (Step::Filter(field, predicate), val @ Value::Mapping(_)) => {
            let value_to_check = if field == "." {
                val
            } else {
                let Some(value) = val.as_mapping().unwrap().get(&field) else {
                    return FindingMoreNodes::Nothing;
                };
                value
            };

            if !predicate(value_to_check) {
                return FindingMoreNodes::Nothing;
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
                    return FindingMoreNodes::Nothing;
                };
                value
            };

            if iter(value_to_check, sub_query).next().is_none() {
                return FindingMoreNodes::Nothing;
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
                .all(|q| iter(value, q.clone()).next().is_some());

            if !all_match {
                return FindingMoreNodes::Nothing;
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
                .any(|q| iter(value, q.clone()).next().is_some());

            if !any_match {
                return FindingMoreNodes::Nothing;
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
                for relative_address in iter(value, sub_query) {
                    additional_paths.push(Candidate {
                        starting_point: current_address.append(relative_address),
                        remaining_query: remaining_query.clone(),
                    })
                }
            }

            FindingMoreNodes::Branching(additional_paths)
        }
        (step, value) => {
            let step = step.name();
            let value = value_name(value);
            tracing::warn!("'{step}' not supported for '{value}'",);

            FindingMoreNodes::Nothing
        }
    }
}
