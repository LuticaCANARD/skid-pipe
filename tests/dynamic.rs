#![cfg(feature = "dynamic")]

use std::{boxed::Box, cell::Cell, rc::Rc};

use skid_pipe::{RuntimePipe, RuntimeStep, TryChain};

#[derive(Debug, PartialEq)]
enum Value {
    Raw(u8),
    Frame(u16),
    Score(u16),
    Class(bool),
}

#[derive(Debug, PartialEq)]
enum Error {
    Empty,
    UnexpectedValue,
}

enum ConfiguredStep {
    Decode,
    Score,
    Classify,
}

#[test]
fn builds_a_heterogeneous_pipeline_from_runtime_configuration() {
    let configuration = [
        ConfiguredStep::Decode,
        ConfiguredStep::Score,
        ConfiguredStep::Classify,
    ];
    let mut pipeline = RuntimePipe::<Value, Error>::with_capacity(configuration.len());

    for step in configuration {
        match step {
            ConfiguredStep::Decode => {
                pipeline.push(|value| match value {
                    Value::Raw(0) => Err(Error::Empty),
                    Value::Raw(raw) => Ok(Value::Frame(u16::from(raw))),
                    _ => Err(Error::UnexpectedValue),
                });
            }
            ConfiguredStep::Score => {
                pipeline.push(|value| match value {
                    Value::Frame(frame) => Ok(Value::Score(frame * 2)),
                    _ => Err(Error::UnexpectedValue),
                });
            }
            ConfiguredStep::Classify => {
                pipeline.push(|value| match value {
                    Value::Score(score) => Ok(Value::Class(score > 20)),
                    _ => Err(Error::UnexpectedValue),
                });
            }
        }
    }

    assert_eq!(pipeline.len(), 3);
    assert_eq!(pipeline.run(Value::Raw(16)), Ok(Value::Class(true)));
    assert_eq!(pipeline.run(Value::Raw(8)), Ok(Value::Class(false)));
}

#[test]
fn stops_at_the_first_error() {
    let later_calls = Rc::new(Cell::new(0_u8));
    let mut pipeline = RuntimePipe::<u8, &'static str>::new();

    pipeline.push(|_| Err("rejected"));
    pipeline.push({
        let later_calls = Rc::clone(&later_calls);
        move |value| {
            later_calls.set(later_calls.get() + 1);
            Ok(value)
        }
    });

    assert_eq!(pipeline.run(4), Err("rejected"));
    assert_eq!(later_calls.get(), 0);
}

#[test]
fn keeps_step_state_between_runs() {
    let mut calls = 0_u8;
    let mut pipeline = RuntimePipe::<u8, core::convert::Infallible>::new();
    pipeline.push(move |value| {
        calls += 1;
        Ok(value + calls)
    });

    assert_eq!(pipeline.run(10), Ok(11));
    assert_eq!(pipeline.run(10), Ok(12));
}

struct Double;

impl RuntimeStep<u8, core::convert::Infallible> for Double {
    fn call(&mut self, input: u8) -> Result<u8, core::convert::Infallible> {
        Ok(input * 2)
    }
}

#[test]
fn accepts_registry_owned_steps_and_implements_try_chain() {
    let step: Box<dyn RuntimeStep<u8, core::convert::Infallible>> = Box::new(Double);
    let mut pipeline = RuntimePipe::default();

    assert!(pipeline.is_empty());
    pipeline.push_boxed(step);

    assert_eq!(TryChain::run(&mut pipeline, 4), Ok(8));
}
