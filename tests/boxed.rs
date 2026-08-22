#![cfg(feature = "alloc")]

use skid_pipe::{BoxedPipe, BoxedTryPipe, Chain, Pipe, TryPipe};

#[test]
fn boxed_pipe_runs_like_the_pipeline_it_wraps() {
    let mut pipeline = BoxedPipe::new(Pipe::new(|value: u8| value + 1).then(|value: u8| value * 2));

    assert_eq!(pipeline.run(4), 10);
}

#[test]
fn boxed_pipe_composes_a_runtime_decided_number_of_stages() {
    let offsets = vec![1, 2, 3];
    let mut pipeline = BoxedPipe::new(Pipe::new(|value: i32| value));

    for offset in offsets {
        pipeline = pipeline.then(move |value| value + offset);
    }

    assert_eq!(pipeline.run(10), 16);
}

#[test]
fn boxed_pipe_keeps_stage_state_across_runs() {
    let mut calls = 0_u32;
    let mut pipeline = BoxedPipe::new(Pipe::new(move |value: u32| {
        calls += 1;
        value + calls
    }));

    assert_eq!(pipeline.run(10), 11);
    assert_eq!(pipeline.run(10), 12);
}

#[test]
fn boxed_pipe_is_itself_a_chain() {
    let boxed = BoxedPipe::new(Pipe::new(|value: u8| value + 1));
    let mut rewrapped = BoxedPipe::new(boxed);

    assert_eq!(Chain::run(&mut rewrapped, 4), 5);
}

#[test]
fn boxed_try_pipe_stops_at_the_first_error() {
    use std::{cell::Cell, rc::Rc};

    let decode_calls = Rc::new(Cell::new(0_u32));
    let classify_calls = Rc::new(Cell::new(0_u32));

    let mut pipeline = {
        let decode_calls = Rc::clone(&decode_calls);
        let classify_calls = Rc::clone(&classify_calls);

        BoxedTryPipe::new(TryPipe::new(move |value: u8| {
            decode_calls.set(decode_calls.get() + 1);
            if value == 0 {
                Err("empty")
            } else {
                Ok(u16::from(value))
            }
        }))
        .try_then(move |value| {
            classify_calls.set(classify_calls.get() + 1);
            Ok(value > 10)
        })
    };

    assert_eq!(pipeline.run(12), Ok(true));
    assert_eq!(pipeline.run(0), Err("empty"));

    assert_eq!(decode_calls.get(), 2);
    assert_eq!(classify_calls.get(), 1);
}
