use core::{
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;

use skid_pipe::TryAsyncPipe;

mod common;

use common::{PendingDropProbe, ReadyWithPanickingDrop, YieldOnce, poll_to_completion};

struct AlwaysPending;

impl Future for AlwaysPending {
    type Output = Result<u8, &'static str>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

async fn fetch(value: u8) -> Result<u16, &'static str> {
    Ok(u16::from(value) + 1)
}

async fn decode(value: u16) -> Result<f32, &'static str> {
    Ok(value as f32 / 2.0)
}

async fn validate(value: f32) -> Result<bool, &'static str> {
    Ok(value > 10.0)
}

async fn describe(value: bool) -> Result<&'static str, &'static str> {
    Ok(if value { "accepted" } else { "rejected" })
}

async fn description_length(value: &'static str) -> Result<usize, &'static str> {
    Ok(value.len())
}

async fn trim(value: &str) -> Result<&str, &'static str> {
    Ok(value.trim())
}

async fn length(value: &str) -> Result<usize, &'static str> {
    Ok(value.len())
}

#[test]
fn composes_fallible_async_steps_with_type_changes() {
    let mut pipeline = TryAsyncPipe::new(fetch).try_then(decode).try_then(validate);

    assert_eq!(poll_to_completion(pipeline.run(24)), Ok(true));
    assert_eq!(poll_to_completion(pipeline.run(2)), Ok(false));
}

#[test]
fn runs_five_type_changing_try_stages_through_the_flat_future() {
    let mut pipeline = TryAsyncPipe::new(fetch)
        .try_then(decode)
        .try_then(validate)
        .try_then(describe)
        .try_then(description_length);

    assert_eq!(poll_to_completion(pipeline.run(24)), Ok("accepted".len()));
    assert_eq!(poll_to_completion(pipeline.run(2)), Ok("rejected".len()));
}

#[test]
fn composes_fallible_async_functions_that_borrow_their_input() {
    let mut pipeline = TryAsyncPipe::new(trim).try_then(length);

    assert_eq!(poll_to_completion(pipeline.run("  pipe  ")), Ok(4));
}

#[test]
fn stops_before_calling_steps_after_the_first_error() {
    let rejected_calls = Cell::new(0_u8);
    let skipped_calls = Cell::new(0_u8);
    let reject = |_: u8| {
        rejected_calls.set(rejected_calls.get() + 1);
        core::future::ready(Err::<u16, _>("rejected"))
    };
    let never = |value: u16| {
        skipped_calls.set(skipped_calls.get() + 1);
        core::future::ready(Ok::<_, &'static str>(value + 1))
    };
    let mut pipeline = TryAsyncPipe::new(reject).try_then(never);

    assert_eq!(poll_to_completion(pipeline.run(4)), Err("rejected"));
    assert_eq!(rejected_calls.get(), 1);
    assert_eq!(skipped_calls.get(), 0);
}

#[test]
fn stops_at_every_error_position_in_a_five_stage_chain() {
    for fail_at in 1..=5 {
        let calls = Cell::new(0_u8);
        let run = |value, stage| {
            calls.set(calls.get() + 1);
            core::future::ready(if fail_at == stage {
                Err(stage)
            } else {
                Ok(value + 1)
            })
        };
        let mut pipeline = TryAsyncPipe::new(|value| run(value, 1))
            .try_then(|value| run(value, 2))
            .try_then(|value| run(value, 3))
            .try_then(|value| run(value, 4))
            .try_then(|value| run(value, 5));

        assert_eq!(poll_to_completion(pipeline.run(0_u8)), Err(fail_at));
        assert_eq!(calls.get(), fail_at);
    }
}

#[test]
fn handles_pending_futures_on_success_and_failure_paths() {
    let skipped_calls = Cell::new(0_u8);
    let mut succeeds =
        TryAsyncPipe::new(|value: u8| YieldOnce::new(Ok::<_, &'static str>(u16::from(value) + 1)))
            .try_then(|value| YieldOnce::new(Ok::<_, &'static str>(value * 2)));
    let mut fails = TryAsyncPipe::new(|_: u8| YieldOnce::new(Err::<u16, _>("pending error")))
        .try_then(|value| {
            skipped_calls.set(skipped_calls.get() + 1);
            YieldOnce::new(Ok::<_, &'static str>(value * 2))
        });

    assert_eq!(poll_to_completion(succeeds.run(4)), Ok(10));
    assert_eq!(poll_to_completion(fails.run(4)), Err("pending error"));
    assert_eq!(skipped_calls.get(), 0);
}

#[test]
fn stateful_fn_mut_steps_keep_state_between_completed_runs() {
    let mut calls = 0_u8;
    let count = move |value: u8| {
        calls += 1;
        core::future::ready(Ok::<_, &'static str>(value + calls))
    };
    let mut pipeline = TryAsyncPipe::new(count)
        .try_then(|value| core::future::ready(Ok::<_, &'static str>(value * 2)));

    assert_eq!(poll_to_completion(pipeline.run(3)), Ok(8));
    assert_eq!(poll_to_completion(pipeline.run(3)), Ok(10));
}

#[test]
fn creating_and_dropping_an_unpolled_try_run_is_lazy() {
    let calls = Cell::new(0_u8);
    let mut pipeline = TryAsyncPipe::new(|value: u8| {
        calls.set(calls.get() + 1);
        core::future::ready(Ok::<_, &'static str>(value + 1))
    });

    let unpolled = pipeline.run(4);
    assert_eq!(calls.get(), 0);
    drop(unpolled);
    assert_eq!(calls.get(), 0);

    assert_eq!(poll_to_completion(pipeline.run(4)), Ok(5));
    assert_eq!(calls.get(), 1);
}

#[test]
fn dropping_a_pending_run_future_releases_the_pipeline_for_another_run() {
    let invocations = Cell::new(0_u8);
    let first = |value: u8| {
        invocations.set(invocations.get() + 1);
        if invocations.get() == 1 {
            EitherPendingOrReady::Pending(AlwaysPending)
        } else {
            EitherPendingOrReady::Ready(core::future::ready(Ok(value + 1)))
        }
    };
    let mut pipeline = TryAsyncPipe::new(first);

    {
        let mut abandoned = pin!(pipeline.run(4));
        let mut context = Context::from_waker(Waker::noop());
        assert!(abandoned.as_mut().poll(&mut context).is_pending());
    }

    assert_eq!(poll_to_completion(pipeline.run(4)), Ok(5));
    assert_eq!(invocations.get(), 2);
}

#[test]
fn dropping_each_active_future_in_a_five_stage_try_chain_is_safe() {
    for pending_stage in 1..=5 {
        let pending_drops = Cell::new(0_u8);
        let selected_stage = Cell::new(pending_stage);
        let make = |value, stage| {
            PendingDropProbe::new(
                Ok::<_, &'static str>(value + 1),
                selected_stage.get() == stage,
                &pending_drops,
            )
        };
        let mut pipeline = TryAsyncPipe::new(|value| make(value, 1))
            .try_then(|value| make(value, 2))
            .try_then(|value| make(value, 3))
            .try_then(|value| make(value, 4))
            .try_then(|value| make(value, 5));

        {
            let mut future = pin!(pipeline.run(0_u8));
            let mut context = Context::from_waker(Waker::noop());
            assert!(future.as_mut().poll(&mut context).is_pending());
        }

        assert_eq!(pending_drops.get(), 1, "pending stage {pending_stage}");
        selected_stage.set(0);
        assert_eq!(poll_to_completion(pipeline.run(0_u8)), Ok(5));
    }
}

#[test]
fn a_panicking_try_future_destructor_is_not_run_twice() {
    let drops = Cell::new(0_u8);
    let mut pipeline = TryAsyncPipe::new(|value| core::future::ready(Ok::<_, ()>(value + 1)))
        .try_then(|value| core::future::ready(Ok::<_, ()>(value + 1)))
        .try_then(|value| ReadyWithPanickingDrop {
            output: Some(Ok::<_, ()>(value + 1)),
            drops: &drops,
        })
        .try_then(|value| core::future::ready(Ok::<_, ()>(value + 1)))
        .try_then(|value| core::future::ready(Ok::<_, ()>(value + 1)));

    {
        let mut future = pin!(pipeline.run(0_u8));
        let mut context = Context::from_waker(Waker::noop());
        let unwind = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));
        assert!(unwind.is_err());
    }

    assert_eq!(drops.get(), 1);
}

enum EitherPendingOrReady {
    Pending(AlwaysPending),
    Ready(core::future::Ready<Result<u8, &'static str>>),
}

impl Future for EitherPendingOrReady {
    type Output = Result<u8, &'static str>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Both variants are `Unpin`, so this projection needs no unsafe code.
        match self.get_mut() {
            Self::Pending(future) => Pin::new(future).poll(context),
            Self::Ready(future) => Pin::new(future).poll(context),
        }
    }
}
