use std::{
    future::Future,
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

#[inline(never)]
async fn decode_async(input: u16) -> u32 {
    decode(input)
}

#[inline(never)]
async fn normalize_async(input: u32) -> f32 {
    normalize(input)
}

#[inline(never)]
async fn classify_async(input: f32) -> bool {
    classify(input)
}

async fn direct_async(input: u16) -> bool {
    let decoded = decode_async(input).await;
    let normalized = normalize_async(decoded).await;
    classify_async(normalized).await
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
        bencher.iter(|| black_box(block_on(direct_async(black_box(INPUT)))));
    });

    let mut pipeline = AsyncPipe::new(decode_async)
        .then(normalize_async)
        .then(classify_async);
    group.bench_function("async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(INPUT)))));
    });

    group.finish();
}

criterion_group!(composition, bench_sync, bench_fallible, bench_async);
criterion_main!(composition);
