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
fn runs_only_the_selected_async_branch_then_continues_the_pipeline() {
    let true_calls = Cell::new(0_u8);
    let false_calls = Cell::new(0_u8);
    let when_true = |value: i32| {
        true_calls.set(true_calls.get() + 1);
        core::future::ready(value * 2)
    };
    let when_false = |value: i32| {
        false_calls.set(false_calls.get() + 1);
        core::future::ready(-value)
    };
    let mut pipeline = AsyncPipe::new(|value: i32| core::future::ready(value))
        .then_branch(
            |value| *value >= 0,
            AsyncPipe::new(when_true),
            AsyncPipe::new(when_false),
        )
        .then(|value| core::future::ready(value + 1));

    assert_eq!(block_on_ready(pipeline.run(4)), 9);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 0);

    assert_eq!(block_on_ready(pipeline.run(-4)), 5);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 1);
}
