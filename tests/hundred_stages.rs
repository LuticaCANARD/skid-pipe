use core::future::{Future, Ready, ready};
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

use skid_pipe::{AsyncPipe, Pipe, TryAsyncPipe, TryPipe};

// The 100-stage builder is shared with the benches, the footprint example
// and the no_std fixture so every consumer composes the same chain.
#[macro_use]
#[path = "../benches/support/footprint.rs"]
mod footprint;

fn increment(value: u16) -> u16 {
    value + 1
}

fn try_increment(value: u16) -> Result<u16, ()> {
    Ok(value + 1)
}

fn async_increment(value: u16) -> Ready<u16> {
    ready(value + 1)
}

fn try_async_increment(value: u16) -> Ready<Result<u16, ()>> {
    ready(Ok(value + 1))
}

fn poll_ready<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("ready stages must complete in one poll"),
    }
}

fn assert_send<T: Send>(_: &T) {}

#[test]
fn runs_one_hundred_synchronous_stages() {
    let mut pipeline = append_ninety_nine!(Pipe::new(increment), then, increment);

    assert_eq!(pipeline.run(0), 100);
}

#[test]
fn runs_one_hundred_fallible_stages() {
    let mut pipeline = append_ninety_nine!(TryPipe::new(try_increment), try_then, try_increment);

    assert_eq!(pipeline.run(0), Ok(100));
}

#[test]
fn runs_one_hundred_asynchronous_stages() {
    let mut pipeline = append_ninety_nine!(AsyncPipe::new(async_increment), then, async_increment);
    let future = pipeline.run(0);
    assert_send(&future);

    assert_eq!(poll_ready(future), 100);
}

#[test]
fn runs_one_hundred_fallible_asynchronous_stages() {
    let mut pipeline = append_ninety_nine!(
        TryAsyncPipe::new(try_async_increment),
        try_then,
        try_async_increment
    );

    let future = pipeline.run(0);
    assert_send(&future);

    assert_eq!(poll_ready(future), Ok(100));
}
