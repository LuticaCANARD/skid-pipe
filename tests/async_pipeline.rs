use core::cell::Cell;
use core::future::Future;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    task::{Context, Waker},
};

use skid_pipe::AsyncPipe;

mod common;

use common::{PendingDropProbe, ReadyWithPanickingDrop, YieldOnce, poll_to_completion};

async fn decode(raw: u16) -> u16 {
    raw + 1
}

async fn normalize(frame: u16) -> f32 {
    frame as f32 / 2.0
}

async fn classify(score: f32) -> bool {
    score > 20.0
}

async fn describe(classified: bool) -> &'static str {
    if classified { "accepted" } else { "rejected" }
}

async fn description_length(description: &'static str) -> usize {
    description.len()
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
fn runs_five_type_changing_stages_through_the_flat_future() {
    let mut pipeline = AsyncPipe::new(decode)
        .then(normalize)
        .then(classify)
        .then(describe)
        .then(description_length);

    assert_eq!(poll_to_completion(pipeline.run(64)), "accepted".len());
    assert_eq!(poll_to_completion(pipeline.run(8)), "rejected".len());
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
fn creating_and_dropping_an_unpolled_run_is_lazy() {
    let calls = Cell::new(0_u8);
    let mut pipeline = AsyncPipe::new(|value: u8| {
        calls.set(calls.get() + 1);
        core::future::ready(value + 1)
    });

    let unpolled = pipeline.run(4);
    assert_eq!(calls.get(), 0);
    drop(unpolled);
    assert_eq!(calls.get(), 0);

    assert_eq!(poll_to_completion(pipeline.run(4)), 5);
    assert_eq!(calls.get(), 1);
}

#[test]
fn composes_futures_that_yield_before_they_complete() {
    let mut pipeline = AsyncPipe::new(|value: u8| YieldOnce::new(value + 1))
        .then(|value| YieldOnce::new(u16::from(value) * 2));

    assert_eq!(poll_to_completion(pipeline.run(4)), 10);
}

#[test]
fn dropping_an_incomplete_run_releases_the_pipeline() {
    let mut calls = 0_u8;
    let mut pipeline = AsyncPipe::new(move |value: u8| {
        calls += 1;
        YieldOnce::new(value + calls)
    });

    {
        let future = pipeline.run(4);
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());

        assert!(future.as_mut().poll(&mut context).is_pending());
    }

    assert_eq!(poll_to_completion(pipeline.run(4)), 6);
}

#[test]
fn composes_async_functions_that_borrow_their_input() {
    let mut pipeline = AsyncPipe::new(trim).then(length);

    assert_eq!(poll_to_completion(pipeline.run("  pipe  ")), 4);
}

#[test]
fn dropping_each_active_future_in_a_five_stage_chain_is_safe() {
    for pending_stage in 1..=5 {
        let pending_drops = Cell::new(0_u8);
        let selected_stage = Cell::new(pending_stage);
        let make = |value, stage| {
            PendingDropProbe::new(value + 1, selected_stage.get() == stage, &pending_drops)
        };
        let mut pipeline = AsyncPipe::new(|value| make(value, 1))
            .then(|value| make(value, 2))
            .then(|value| make(value, 3))
            .then(|value| make(value, 4))
            .then(|value| make(value, 5));

        {
            let mut future = pin!(pipeline.run(0_u8));
            let mut context = Context::from_waker(Waker::noop());
            assert!(future.as_mut().poll(&mut context).is_pending());
        }

        assert_eq!(pending_drops.get(), 1, "pending stage {pending_stage}");
        selected_stage.set(0);
        assert_eq!(poll_to_completion(pipeline.run(0_u8)), 5);
    }
}

#[test]
fn a_panicking_future_destructor_is_not_run_twice() {
    let drops = Cell::new(0_u8);
    let mut pipeline = AsyncPipe::new(|value| core::future::ready(value + 1))
        .then(|value| core::future::ready(value + 1))
        .then(|value| ReadyWithPanickingDrop {
            output: Some(value + 1),
            drops: &drops,
        })
        .then(|value| core::future::ready(value + 1))
        .then(|value| core::future::ready(value + 1));

    {
        let mut future = pin!(pipeline.run(0_u8));
        let mut context = Context::from_waker(Waker::noop());
        let unwind = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));
        assert!(unwind.is_err());
    }

    assert_eq!(drops.get(), 1);
}

#[test]
fn preserves_order_across_multiple_quad_groups() {
    let order = Cell::new(0_u64);
    let mark = |stage| {
        let order = &order;
        move |value| {
            order.set(order.get() * 10 + stage);
            core::future::ready(value + stage)
        }
    };
    let mut pipeline = AsyncPipe::new(mark(1))
        .then(mark(2))
        .then(mark(3))
        .then(mark(4))
        .then(mark(5))
        .then(mark(6))
        .then(mark(7))
        .then(mark(8))
        .then(mark(9));

    assert_eq!(poll_to_completion(pipeline.run(0_u64)), 45);
    assert_eq!(order.get(), 123_456_789);
}
