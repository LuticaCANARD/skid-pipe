//! Asynchronous branching without a dedicated combinator: an `if`/`match`
//! inside the stage future, where only the selected arm is awaited.
//!
//! A stage closure is `FnMut`, so branch state lives in a `Cell` captured by
//! shared reference. Moving it into the returned future instead would make the
//! closure `FnOnce`, which cannot be a repeatable stage.

use core::{cell::Cell, future::Future};
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

use skid_pipe::AsyncPipe;

fn block_on_ready<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly required an executor"),
    }
}

#[test]
fn awaits_only_the_selected_arm_then_continues_the_pipeline() {
    let true_calls = Cell::new(0_u8);
    let false_calls = Cell::new(0_u8);

    let mut pipeline = AsyncPipe::new(|value: i32| core::future::ready(value))
        .then(|value: i32| {
            let true_calls = &true_calls;
            let false_calls = &false_calls;

            async move {
                if value >= 0 {
                    true_calls.set(true_calls.get() + 1);
                    core::future::ready(value * 2).await
                } else {
                    false_calls.set(false_calls.get() + 1);
                    core::future::ready(-value).await
                }
            }
        })
        .then(|value| core::future::ready(value + 1));

    assert_eq!(block_on_ready(pipeline.run(4)), 9);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 0);

    assert_eq!(block_on_ready(pipeline.run(-4)), 5);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 1);
}

#[test]
fn async_match_dispatches_over_more_than_two_arms() {
    let mut pipeline = AsyncPipe::new(|value: i32| core::future::ready(value.signum())).then(
        |sign: i32| async move {
            match sign {
                1 => core::future::ready("positive").await,
                -1 => core::future::ready("negative").await,
                _ => core::future::ready("zero").await,
            }
        },
    );

    assert_eq!(block_on_ready(pipeline.run(7)), "positive");
    assert_eq!(block_on_ready(pipeline.run(-7)), "negative");
    assert_eq!(block_on_ready(pipeline.run(0)), "zero");
}
