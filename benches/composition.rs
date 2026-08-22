use core::future::{Future, Ready, ready};
use std::{
    hint::black_box,
    pin::pin,
    task::{Context, Poll, Waker},
};

use criterion::{Criterion, criterion_group, criterion_main};
use skid_pipe::{AsyncPipe, Pipe, TryPipe};

const INPUT: u16 = 2_731;

#[inline(never)]
fn decode(input: u16) -> u32 {
    u32::from(input).wrapping_mul(1_103).wrapping_add(97)
}

#[inline(never)]
fn normalize(input: u32) -> f32 {
    input as f32 / 4_096.0
}

#[inline(never)]
fn classify(input: f32) -> bool {
    input > 500.0
}

fn direct_sync(input: u16) -> bool {
    classify(normalize(decode(input)))
}

#[inline(never)]
fn decode_fallible(input: u16) -> Result<u32, ()> {
    Ok(decode(input))
}

#[inline(never)]
fn normalize_fallible(input: u32) -> Result<f32, ()> {
    Ok(normalize(input))
}

#[inline(never)]
fn classify_fallible(input: f32) -> Result<bool, ()> {
    Ok(classify(input))
}

fn direct_fallible(input: u16) -> Result<bool, ()> {
    let decoded = decode_fallible(input)?;
    let normalized = normalize_fallible(decoded)?;
    classify_fallible(normalized)
}

// Both benchmark arms await these exact `Ready<T>` stages. This isolates the
// cost of composing them instead of comparing different future state machines.
#[inline(never)]
fn decode_ready(input: u16) -> Ready<u32> {
    ready(decode(input))
}

#[inline(never)]
fn normalize_ready(input: u32) -> Ready<f32> {
    ready(normalize(input))
}

#[inline(never)]
fn classify_ready(input: f32) -> Ready<bool> {
    ready(classify(input))
}

async fn direct_ready(input: u16) -> bool {
    let decoded = decode_ready(input).await;
    let normalized = normalize_ready(decoded).await;
    classify_ready(normalized).await
}

fn block_on<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

fn bench_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_three_stage");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_sync(black_box(INPUT))));
    });

    let mut pipeline = Pipe::new(decode).then(normalize).then(classify);
    group.bench_function("pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(INPUT))));
    });

    group.finish();
}

fn bench_fallible(c: &mut Criterion) {
    let mut group = c.benchmark_group("fallible_three_stage_success");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_fallible(black_box(INPUT))));
    });

    let mut pipeline = TryPipe::new(decode_fallible)
        .try_then(normalize_fallible)
        .try_then(classify_fallible);
    group.bench_function("try_pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(INPUT))));
    });

    group.finish();
}

fn bench_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_three_stage_ready");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(block_on(direct_ready(black_box(INPUT)))));
    });

    let mut pipeline = AsyncPipe::new(decode_ready)
        .then(normalize_ready)
        .then(classify_ready);
    group.bench_function("async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(INPUT)))));
    });

    group.finish();
}

criterion_group!(composition, bench_sync, bench_fallible, bench_async);
criterion_main!(composition);
