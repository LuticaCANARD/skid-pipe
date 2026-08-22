use core::future::Future;
use std::{
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};

use skid_pipe::AsyncPipe;

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

struct YieldOnce<Output> {
    output: Option<Output>,
    yielded: bool,
}

impl<Output> YieldOnce<Output> {
    fn new(output: Output) -> Self {
        Self {
            output: Some(output),
            yielded: false,
        }
    }
}

impl<Output: Unpin> Future for YieldOnce<Output> {
    type Output = Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();

        if !this.yielded {
            this.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(this.output.take().expect("future must only complete once"))
        }
    }
}

async fn decode(raw: u16) -> u16 {
    raw + 1
}

async fn normalize(frame: u16) -> f32 {
    frame as f32 / 2.0
}

async fn classify(score: f32) -> bool {
    score > 20.0
}

async fn trim(value: &str) -> &str {
    value.trim()
}

async fn length(value: &str) -> usize {
    value.len()
}

#[test]
fn runs_async_steps_with_type_changes() {
    let mut pipeline = AsyncPipe::new(decode).then(normalize).then(classify);

    assert!(poll_to_completion(pipeline.run(64)));
    assert!(!poll_to_completion(pipeline.run(8)));
}

#[test]
fn async_fn_mut_stage_keeps_state_between_completed_runs() {
    let mut calls = 0_i32;
    let counter = move |value: i32| {
        calls += 1;
        core::future::ready(value + calls)
    };
    let mut pipeline = AsyncPipe::new(counter).then(|value| core::future::ready(value * 2));

    assert_eq!(poll_to_completion(pipeline.run(3)), 8);
    assert_eq!(poll_to_completion(pipeline.run(3)), 10);
}

#[test]
fn composes_futures_that_yield_before_they_complete() {
    let mut pipeline = AsyncPipe::new(|value: u8| YieldOnce::new(value + 1))
        .then(|value| YieldOnce::new(u16::from(value) * 2));

    assert_eq!(poll_to_completion(pipeline.run(4)), 10);
}

#[test]
fn composes_async_functions_that_borrow_their_input() {
    let mut pipeline = AsyncPipe::new(trim).then(length);

    assert_eq!(poll_to_completion(pipeline.run("  pipe  ")), 4);
}
