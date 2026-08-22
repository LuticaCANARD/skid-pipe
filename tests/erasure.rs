use core::future::Future;
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

use skid_pipe::{AsyncChain, AsyncPipe, Chain, DynChain, DynTryChain, Pipe, Step, TryPipe};

fn poll_to_completion<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => continue,
        }
    }
}

fn build_classifier() -> impl Chain<u16, Output = bool> {
    Pipe::new(|value: u16| value as f32 / 4095.0).then(|value: f32| value > 0.5)
}

#[test]
fn builder_function_erases_the_concrete_pipeline_type() {
    let mut pipeline = build_classifier();

    assert!(pipeline.run(3000));
    assert!(!pipeline.run(100));
}

#[test]
fn dyn_chain_selects_between_pipelines_at_runtime() {
    let mut double = Pipe::new(|value: i32| value * 2);
    let mut negate = Pipe::new(|value: i32| -value);

    for (use_double, input, expected) in [(true, 4, 8), (false, 4, -4)] {
        let erased: DynChain<'_, i32, i32> = if use_double { &mut double } else { &mut negate };
        assert_eq!(erased.run(input), expected);
    }
}

#[test]
fn dyn_chain_keeps_stage_state_across_runs() {
    let mut calls = 0_u32;
    let mut pipeline = Pipe::new(move |value: u32| {
        calls += 1;
        value + calls
    });
    let erased: DynChain<'_, u32, u32> = &mut pipeline;

    assert_eq!(erased.run(10), 11);
    assert_eq!(erased.run(10), 12);
}

struct SaturatingOffset {
    offset: u8,
}

impl Step<u8> for SaturatingOffset {
    type Output = u8;

    fn call(&mut self, input: u8) -> Self::Output {
        input.saturating_add(self.offset)
    }
}

#[test]
fn hand_written_step_implementations_compose_like_closures() {
    let mut pipeline = Pipe::new(|value: u8| value * 2).then(SaturatingOffset { offset: 100 });

    assert_eq!(pipeline.run(3), 106);
    assert_eq!(pipeline.run(120), 255);
}

#[test]
fn dyn_try_chain_erases_fallible_pipelines() {
    let mut pipeline = TryPipe::new(|value: u8| {
        if value == 0 {
            Err("empty")
        } else {
            Ok(u16::from(value))
        }
    })
    .try_then(|value: u16| Ok(value > 10));

    let erased: DynTryChain<'_, u8, bool, &'static str> = &mut pipeline;

    assert_eq!(erased.run(12), Ok(true));
    assert_eq!(erased.run(0), Err("empty"));
}

fn build_async_doubler() -> impl AsyncChain<u8, Output = u16> {
    AsyncPipe::new(|value: u8| core::future::ready(value + 1))
        .then(|value: u8| core::future::ready(u16::from(value) * 2))
}

#[test]
fn async_builder_function_erases_the_concrete_pipeline_type() {
    let mut pipeline = build_async_doubler();

    assert_eq!(poll_to_completion(pipeline.run(4)), 10);
}
