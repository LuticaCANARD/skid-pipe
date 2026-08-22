use core::future::Future;
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

use skid_pipe::{AsyncChain, AsyncPipe, Chain, Pipe, Step};

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

fn build_async_doubler() -> impl AsyncChain<u8, Output = u16> {
    AsyncPipe::new(|value: u8| core::future::ready(value + 1))
        .then(|value: u8| core::future::ready(u16::from(value) * 2))
}

#[test]
fn async_builder_function_erases_the_concrete_pipeline_type() {
    let mut pipeline = build_async_doubler();

    assert_eq!(poll_to_completion(pipeline.run(4)), 10);
}
