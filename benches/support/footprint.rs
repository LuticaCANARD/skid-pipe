//! Reproducible footprint probes for the ten- and 100-stage async paths, and
//! the single home of the `append_*!` / `await_ten!` / `apply_hundred!`
//! builders.
//!
//! This file is `#[path]`-included by `benches/composition.rs`,
//! `tests/hundred_stages.rs`, `examples/measure_footprint.rs` and
//! `tests/fixtures/no_std`, each with `#[macro_use]`, so all four compose the
//! same stages. Everything here is fixture-only and not part of `skid-pipe`.

use core::{
    future::Future,
    mem::size_of_val,
    pin::pin,
    task::{Context, Poll, Waker},
};

use skid_pipe::{AsyncPipe, TryAsyncPipe};

#[inline(never)]
fn ready_increment(value: u16) -> core::future::Ready<u16> {
    core::future::ready(value + 1)
}

#[inline(never)]
fn try_ready_increment(value: u16) -> core::future::Ready<Result<u16, ()>> {
    core::future::ready(value.checked_add(1).ok_or(()))
}

macro_rules! await_ten {
    ($value:expr, $stage:path) => {{
        let value = $stage($value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        let value = $stage(value).await;
        $stage(value).await
    }};
}

macro_rules! try_await_ten {
    ($value:expr, $stage:path) => {{
        let value = $stage($value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        let value = $stage(value).await?;
        $stage(value).await?
    }};
}

macro_rules! apply_hundred {
    ($value:expr, $ten:ident, $stage:path) => {{
        let value = $ten!($value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        let value = $ten!(value, $stage);
        $ten!(value, $stage)
    }};
}

macro_rules! append_ten {
    ($pipeline:expr, $method:ident, $stage:path) => {
        $pipeline
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
    };
}

macro_rules! append_nine {
    ($pipeline:expr, $method:ident, $stage:path) => {
        $pipeline
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
    };
}

macro_rules! append_ninety_nine {
    ($pipeline:expr, $method:ident, $stage:path) => {{
        let pipeline = append_ten!($pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        let pipeline = append_ten!(pipeline, $method, $stage);
        append_nine!(pipeline, $method, $stage)
    }};
}

async fn direct_async(input: u16) -> u16 {
    apply_hundred!(input, await_ten, ready_increment)
}

// Ten stages stand in for a realistic chain: long enough to peel one
// `ThenQuadFuture` group plus the pair and first-stage links, short enough to
// be the shape the README recommends on a constrained target.
async fn direct_async_ten(input: u16) -> u16 {
    await_ten!(input, ready_increment)
}

async fn direct_try_async_ten(input: u16) -> Result<u16, ()> {
    Ok(try_await_ten!(input, try_ready_increment))
}

async fn direct_try_async(input: u16) -> Result<u16, ()> {
    Ok(apply_hundred!(input, try_await_ten, try_ready_increment))
}

fn poll_ready<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("Ready stages must complete in one poll"),
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_direct_async(input: u16) -> u16 {
    poll_ready(direct_async(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_pipeline_async(input: u16) -> u16 {
    let mut pipeline = append_ninety_nine!(AsyncPipe::new(ready_increment), then, ready_increment);
    poll_ready(pipeline.run(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_direct_try_async(input: u16) -> i32 {
    match poll_ready(direct_try_async(input)) {
        Ok(output) => i32::from(output),
        Err(()) => -1,
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_pipeline_try_async(input: u16) -> i32 {
    let mut pipeline = append_ninety_nine!(
        TryAsyncPipe::new(try_ready_increment),
        try_then,
        try_ready_increment
    );
    match poll_ready(pipeline.run(input)) {
        Ok(output) => i32::from(output),
        Err(()) => -1,
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_direct_ten_async(input: u16) -> u16 {
    poll_ready(direct_async_ten(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_pipeline_ten_async(input: u16) -> u16 {
    let mut pipeline = append_nine!(AsyncPipe::new(ready_increment), then, ready_increment);
    poll_ready(pipeline.run(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_direct_ten_try_async(input: u16) -> i32 {
    match poll_ready(direct_try_async_ten(input)) {
        Ok(output) => i32::from(output),
        Err(()) => -1,
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_pipeline_ten_try_async(input: u16) -> i32 {
    let mut pipeline = append_nine!(
        TryAsyncPipe::new(try_ready_increment),
        try_then,
        try_ready_increment
    );
    match poll_ready(pipeline.run(input)) {
        Ok(output) => i32::from(output),
        Err(()) => -1,
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_direct_async_future_bytes(input: u16) -> usize {
    size_of_val(&direct_async(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_pipeline_async_future_bytes(input: u16) -> usize {
    let mut pipeline = append_ninety_nine!(AsyncPipe::new(ready_increment), then, ready_increment);
    size_of_val(&pipeline.run(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_direct_try_async_future_bytes(input: u16) -> usize {
    size_of_val(&direct_try_async(input))
}

#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn skid_pipe_measure_pipeline_try_async_future_bytes(input: u16) -> usize {
    let mut pipeline = append_ninety_nine!(
        TryAsyncPipe::new(try_ready_increment),
        try_then,
        try_ready_increment
    );
    size_of_val(&pipeline.run(input))
}
