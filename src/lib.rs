use std::collections::VecDeque;
use std::panic;

use serde_yaml::Value;

#[derive(Clone, Debug)]
pub struct Query {
    steps: Vec<Step>,
}

impl Query {
    fn take_step(&self) -> Option<(Step, Query)> {
        if self.steps.len() == 0 {
            return None;
        };
        let mut others = self.steps.clone();
        let next = others.remove(0);
        Some((next, Query { steps: others }))
    }
}

#[derive(Debug, Clone)]
enum Step {
    Field(String),
    Index(usize),
    All,
}

struct Paths<'input> {
    starting_point: &'input Value,
    query: Query,
}

pub struct ManyResults<'input> {
    paths_to_explore: VecDeque<Paths<'input>>,
}

impl<'input> Iterator for ManyResults<'input> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(path_to_explore) = self.paths_to_explore.pop_front() {
            match dive(path_to_explore) {
                DiveOutcome::Final(value) => return Some(value.clone()),
                DiveOutcome::Nothing => {} // might have to explore more paths here!
                DiveOutcome::Branch(more_paths_to_consider) => {
                    self.paths_to_explore.extend(more_paths_to_consider);
                }
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
        (Step::Index(idx), Value::Sequence(s)) => {
            let Some(next_value) = s.get(idx) else {
                    return DiveOutcome::Nothing;
            };
            dive(Paths {
                starting_point: next_value,
                query: remaining_query,
            })
        }
        (Step::All, Value::Sequence(s)) => {
            let mut x = Vec::new();
            for val in s {
                x.push(Paths {
                    starting_point: val,
                    query: remaining_query.clone(),
                });
            }
            DiveOutcome::Branch(x)
        }
        (step, value) => {
            panic!("{step:?} not supported for {value:?}");
        }
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

        // TODO: A macro to do query!["people", 0, "name"] would be ace!
        let first_persons_name = Query {
            steps: vec![
                Step::Field("people".to_string()),
                Step::Index(0),
                Step::Field("name".to_string()),
            ],
        };

        let felipe = navigate_iter(&yaml, first_persons_name).next().unwrap();
        assert_eq!(felipe, Value::String("Felipe".into()));

        let yoga = Query {
            steps: vec![
                Step::Field("people".to_string()),
                Step::All,
                Step::Field("sports".to_string()),
                Step::Index(1),
            ],
        };

        let yoga: Vec<_> = navigate_iter(&yaml, yoga).collect();
        assert_eq!(yoga, vec![Value::String("yoga".to_string())]);
    }
}
