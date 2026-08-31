#![cfg(feature = "tokio")]
//! `AsyncChainSend` / `TryAsyncChainSend` restate the composition with `Send`
//! promised, so the compiler checks at each impl that the bounds are enough —
//! an insufficient set would not build. What is worth testing is the other
//! direction: that the bounds are not so strong they reject a pipeline that is
//! genuinely `Send`. The rejection side is covered by the `compile_fail`
//! examples on `TokioAsyncChainExt::spawn`, where rustdoc actually runs them.
//!
//! The interesting shape is a chain long enough to fold, because that is where
//! the `Send` recursion runs. Forty-one stages folds under both widths: without
//! `wide` as sixteen over sixteen over nine, with it as thirty-two over nine.

use std::rc::Rc;

use skid_pipe::{AsyncPipe, TokioAsyncChainExt, TokioTryAsyncChainExt, TryAsyncPipe};

#[macro_use]
#[path = "../benches/support/footprint.rs"]
mod support;

fn increment(value: u16) -> core::future::Ready<u16> {
    core::future::ready(value + 1)
}

fn try_increment(value: u16) -> core::future::Ready<Result<u16, &'static str>> {
    core::future::ready(Ok(value + 1))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime must build")
}

/// Forty-one plain stages across the fold boundary, spawned onto a `Send` task.
#[test]
fn spawns_a_chain_long_enough_to_fold() {
    let runtime = runtime();

    let task = {
        let _guard = runtime.enter();
        let pipeline = append_ten!(
            append_ten!(
                append_ten!(
                    append_ten!(AsyncPipe::new(increment), then, increment),
                    then,
                    increment
                ),
                then,
                increment
            ),
            then,
            increment
        );
        pipeline.spawn(0)
    };

    assert_eq!(runtime.block_on(task).expect("task must complete"), 41);
}

/// The same for the fallible ladder, whose `Send` bounds also carry `Error`.
#[test]
fn spawns_a_fallible_chain_long_enough_to_fold() {
    let runtime = runtime();

    let task = {
        let _guard = runtime.enter();
        let pipeline = append_ten!(
            append_ten!(
                append_ten!(
                    append_ten!(TryAsyncPipe::new(try_increment), try_then, try_increment),
                    try_then,
                    try_increment
                ),
                try_then,
                try_increment
            ),
            try_then,
            try_increment
        );
        pipeline.spawn(0)
    };

    assert_eq!(runtime.block_on(task).expect("task must complete"), Ok(41));
}

/// A stage that keeps `Send` state across runs is still spawnable: the bounds
/// ask each stage to be `Send`, not to be a plain function.
#[test]
fn spawns_a_stateful_send_pipeline() {
    let runtime = runtime();

    let task = {
        let _guard = runtime.enter();
        let mut calls = 0_u16;
        let counting = move |value: u16| {
            calls += 1;
            core::future::ready(value + calls)
        };
        AsyncPipe::new(counting).then(increment).spawn(10)
    };

    assert_eq!(runtime.block_on(task).expect("task must complete"), 12);
}

/// A non-`Send` chain of the same folding length still runs locally, so the
/// `Send` ladder did not become the only way to reach a long pipeline.
#[test]
fn runs_a_long_non_send_chain_locally() {
    let runtime = runtime();
    let local = tokio::task::LocalSet::new();

    let output = runtime.block_on(local.run_until(async {
        let offset = Rc::new(1_u16);
        let held = move |value: u16| {
            let offset = Rc::clone(&offset);
            async move { value + *offset }
        };
        let pipeline = append_ten!(
            append_ten!(
                append_ten!(
                    append_ten!(AsyncPipe::new(held), then, increment),
                    then,
                    increment
                ),
                then,
                increment
            ),
            then,
            increment
        );
        pipeline
            .spawn_local(0)
            .await
            .expect("local task must complete")
    }));

    assert_eq!(output, 41);
}
