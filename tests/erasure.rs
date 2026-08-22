use core::future::Future;
use core::task::{Context, Poll};

use skid_pipe::{AsyncChain, AsyncPipe, AsyncStep, Chain, Pipe, Step, TryAsyncPipe, TryAsyncStep};

mod common;

use common::poll_to_completion;

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

struct ReadyWiden;

impl AsyncStep<u8> for ReadyWiden {
    type Output = u16;
    type Future<'a>
        = core::future::Ready<u16>
    where
        Self: 'a;

    fn call(&mut self, input: u8) -> Self::Future<'_> {
        core::future::ready(u16::from(input))
    }
}

struct TryReadyWiden;

impl TryAsyncStep<u8, &'static str> for TryReadyWiden {
    type Output = u16;
    type Future<'a>
        = core::future::Ready<Result<u16, &'static str>>
    where
        Self: 'a;

    fn call(&mut self, input: u8) -> Self::Future<'_> {
        core::future::ready(Ok(u16::from(input)))
    }
}

struct BorrowingFuture<'a> {
    calls: &'a mut u16,
    input: u8,
}

impl Future for BorrowingFuture<'_> {
    type Output = u16;

    fn poll(self: core::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        *this.calls += 1;
        Poll::Ready(u16::from(this.input) + *this.calls)
    }
}

struct BorrowingWiden {
    calls: u16,
}

impl AsyncStep<u8> for BorrowingWiden {
    type Output = u16;
    type Future<'a>
        = BorrowingFuture<'a>
    where
        Self: 'a;

    fn call(&mut self, input: u8) -> Self::Future<'_> {
        BorrowingFuture {
            calls: &mut self.calls,
            input,
        }
    }
}

#[test]
fn hand_written_async_step_implementations_expose_their_future_type() {
    let mut pipeline = AsyncPipe::new(|value: u8| core::future::ready(value + 1)).then(ReadyWiden);
    let mut try_pipeline =
        TryAsyncPipe::new(|value: u8| core::future::ready(Ok::<_, &'static str>(value + 1)))
            .try_then(TryReadyWiden);

    assert_eq!(poll_to_completion(pipeline.run(4)), 5_u16);
    assert_eq!(poll_to_completion(try_pipeline.run(4)), Ok(5_u16));
}

#[test]
fn a_hand_written_stage_future_may_borrow_mutable_stage_state() {
    let mut pipeline = AsyncPipe::new(BorrowingWiden { calls: 0 });

    assert_eq!(poll_to_completion(pipeline.run(4)), 5);
    assert_eq!(poll_to_completion(pipeline.run(4)), 6);
}
