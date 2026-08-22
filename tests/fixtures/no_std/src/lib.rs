#![no_std]

use skid_pipe::{AsyncPipe, Pipe, TryAsyncPipe, TryPipe};

// Also the single home of the `append_*!` builders used below.
#[macro_use]
#[path = "../../../../benches/support/footprint.rs"]
pub mod measure;

#[inline(never)]
fn ready_increment(value: u16) -> core::future::Ready<u16> {
    core::future::ready(value + 1)
}

fn increment(value: u16) -> u16 {
    value.wrapping_add(1)
}

fn try_increment(value: u16) -> Result<u16, ()> {
    value.checked_add(1).ok_or(())
}

#[inline(never)]
fn try_ready_increment(value: u16) -> core::future::Ready<Result<u16, ()>> {
    core::future::ready(value.checked_add(1).ok_or(()))
}

pub fn normalize_and_classify(sample: u16) -> bool {
    let mut pipeline = Pipe::new(|value: u16| value as f32 / 4095.0).then(|value| value > 0.5);

    pipeline.run(sample)
}

pub async fn increment_then_double(sample: u16) -> u16 {
    let mut pipeline = AsyncPipe::new(|value| core::future::ready(value + 1))
        .then(|value| core::future::ready(value * 2));

    pipeline.run(sample).await
}

pub fn checked_increment_then_double(sample: u16) -> Result<u16, ()> {
    let mut pipeline = TryPipe::new(|value: u16| value.checked_add(1).ok_or(()))
        .try_then(|value: u16| value.checked_mul(2).ok_or(()));

    pipeline.run(sample)
}

pub async fn checked_async_increment_then_double(sample: u16) -> Result<u16, ()> {
    let mut pipeline =
        TryAsyncPipe::new(|value: u16| core::future::ready(value.checked_add(1).ok_or(())))
            .try_then(|value: u16| core::future::ready(value.checked_mul(2).ok_or(())));

    pipeline.run(sample).await
}

pub fn one_hundred_sync_stages(sample: u16) -> u16 {
    let mut pipeline = append_ninety_nine!(Pipe::new(increment), then, increment);

    pipeline.run(sample)
}

pub fn one_hundred_try_stages(sample: u16) -> Result<u16, ()> {
    let mut pipeline = append_ninety_nine!(TryPipe::new(try_increment), try_then, try_increment);

    pipeline.run(sample)
}

pub async fn one_hundred_async_stages(sample: u16) -> u16 {
    let mut pipeline = append_ninety_nine!(AsyncPipe::new(ready_increment), then, ready_increment);

    pipeline.run(sample).await
}

pub async fn one_hundred_try_async_stages(sample: u16) -> Result<u16, ()> {
    let mut pipeline = append_ninety_nine!(
        TryAsyncPipe::new(try_ready_increment),
        try_then,
        try_ready_increment
    );

    pipeline.run(sample).await
}
