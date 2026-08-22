use core::future::{Future, Ready, ready};
use std::{
    hint::black_box,
    pin::pin,
    task::{Context, Poll, Waker},
};

use criterion::{Criterion, criterion_group, criterion_main};
use skid_pipe::{AsyncPipe, Pipe, TryAsyncPipe, TryPipe};

// `append_*!` / `await_ten!` / `apply_hundred!` live here so the benches, the
// footprint example and the no_std fixture all build the same 100 stages.
#[macro_use]
#[path = "support/footprint.rs"]
mod footprint;

const INPUT: u16 = 2_731;

macro_rules! call_ten {
    ($value:expr, $stage:path) => {{
        let value = $stage($value);
        let value = $stage(value);
        let value = $stage(value);
        let value = $stage(value);
        let value = $stage(value);
        let value = $stage(value);
        let value = $stage(value);
        let value = $stage(value);
        let value = $stage(value);
        $stage(value)
    }};
}

macro_rules! try_ten {
    ($value:expr, $stage:path) => {{
        let value = $stage($value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        let value = $stage(value)?;
        $stage(value)?
    }};
}

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

fn direct_type_changing_fallible(input: u16) -> Result<bool, ()> {
    let decoded = decode_fallible(input)?;
    let normalized = normalize_fallible(decoded)?;
    classify_fallible(normalized)
}

#[derive(Clone, Copy)]
struct FallibleValue {
    value: u32,
    fail_at: u8,
}

const FALLIBLE_SUCCESS: FallibleValue = FallibleValue {
    value: INPUT as u32,
    fail_at: 0,
};

#[inline(never)]
fn fallible_stage<const INDEX: u8>(mut input: FallibleValue) -> Result<FallibleValue, u8> {
    if input.fail_at == INDEX {
        return Err(INDEX);
    }

    input.value = input
        .value
        .rotate_left(u32::from(INDEX))
        .wrapping_add(u32::from(INDEX) * 97);
    Ok(input)
}

fn direct_fallible_1(input: FallibleValue) -> Result<FallibleValue, u8> {
    fallible_stage::<1>(input)
}

fn direct_fallible_3(input: FallibleValue) -> Result<FallibleValue, u8> {
    let value = fallible_stage::<1>(input)?;
    let value = fallible_stage::<2>(value)?;
    fallible_stage::<3>(value)
}

fn direct_fallible_8(input: FallibleValue) -> Result<FallibleValue, u8> {
    let value = fallible_stage::<1>(input)?;
    let value = fallible_stage::<2>(value)?;
    let value = fallible_stage::<3>(value)?;
    let value = fallible_stage::<4>(value)?;
    let value = fallible_stage::<5>(value)?;
    let value = fallible_stage::<6>(value)?;
    let value = fallible_stage::<7>(value)?;
    fallible_stage::<8>(value)
}

#[inline(never)]
fn fallible_ready_stage<const INDEX: u8>(input: FallibleValue) -> Ready<Result<FallibleValue, u8>> {
    ready(fallible_stage::<INDEX>(input))
}

async fn direct_try_async_ready(input: FallibleValue) -> Result<FallibleValue, u8> {
    let value = fallible_ready_stage::<1>(input).await?;
    let value = fallible_ready_stage::<2>(value).await?;
    fallible_ready_stage::<3>(value).await
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

#[derive(Clone, Copy)]
struct HundredValue {
    value: u32,
    stage: u8,
    fail_at: u8,
}

const HUNDRED_SUCCESS: HundredValue = HundredValue {
    value: INPUT as u32,
    stage: 0,
    fail_at: 0,
};

#[inline(never)]
fn hundred_stage(mut input: HundredValue) -> HundredValue {
    input.stage += 1;
    input.value = input
        .value
        .rotate_left(u32::from(input.stage % 31))
        .wrapping_add(u32::from(input.stage) * 97);
    input
}

#[inline(never)]
fn hundred_try_stage(input: HundredValue) -> Result<HundredValue, u8> {
    let output = hundred_stage(input);
    if output.stage == output.fail_at {
        Err(output.stage)
    } else {
        Ok(output)
    }
}

#[inline(never)]
fn hundred_ready_stage(input: HundredValue) -> Ready<HundredValue> {
    ready(hundred_stage(input))
}

#[inline(never)]
fn hundred_try_ready_stage(input: HundredValue) -> Ready<Result<HundredValue, u8>> {
    ready(hundred_try_stage(input))
}

fn direct_hundred(input: HundredValue) -> HundredValue {
    apply_hundred!(input, call_ten, hundred_stage)
}

fn direct_hundred_try(input: HundredValue) -> Result<HundredValue, u8> {
    Ok(apply_hundred!(input, try_ten, hundred_try_stage))
}

async fn direct_hundred_async(input: HundredValue) -> HundredValue {
    apply_hundred!(input, await_ten, hundred_ready_stage)
}

async fn direct_hundred_try_async(input: HundredValue) -> Result<HundredValue, u8> {
    Ok(apply_hundred!(
        input,
        try_await_ten,
        hundred_try_ready_stage
    ))
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
    let mut group = c.benchmark_group("fallible_success/1_stage");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_fallible_1(black_box(FALLIBLE_SUCCESS))));
    });

    let mut pipeline = TryPipe::new(fallible_stage::<1>);
    group.bench_function("try_pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(FALLIBLE_SUCCESS))));
    });

    group.finish();

    let mut group = c.benchmark_group("fallible_success/3_stage");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_fallible_3(black_box(FALLIBLE_SUCCESS))));
    });

    let mut pipeline = TryPipe::new(fallible_stage::<1>)
        .try_then(fallible_stage::<2>)
        .try_then(fallible_stage::<3>);
    group.bench_function("try_pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(FALLIBLE_SUCCESS))));
    });

    group.finish();

    let mut group = c.benchmark_group("fallible_success/8_stage");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_fallible_8(black_box(FALLIBLE_SUCCESS))));
    });

    let mut pipeline = TryPipe::new(fallible_stage::<1>)
        .try_then(fallible_stage::<2>)
        .try_then(fallible_stage::<3>)
        .try_then(fallible_stage::<4>)
        .try_then(fallible_stage::<5>)
        .try_then(fallible_stage::<6>)
        .try_then(fallible_stage::<7>)
        .try_then(fallible_stage::<8>);
    group.bench_function("try_pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(FALLIBLE_SUCCESS))));
    });

    group.finish();
}

fn bench_type_changing_fallible(c: &mut Criterion) {
    let mut group = c.benchmark_group("fallible_type_changing_success");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_type_changing_fallible(black_box(INPUT))));
    });

    let mut pipeline = TryPipe::new(decode_fallible)
        .try_then(normalize_fallible)
        .try_then(classify_fallible);
    group.bench_function("try_pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(INPUT))));
    });

    group.finish();
}

fn bench_fallible_errors(c: &mut Criterion) {
    let mut pipeline = TryPipe::new(fallible_stage::<1>)
        .try_then(fallible_stage::<2>)
        .try_then(fallible_stage::<3>)
        .try_then(fallible_stage::<4>)
        .try_then(fallible_stage::<5>)
        .try_then(fallible_stage::<6>)
        .try_then(fallible_stage::<7>)
        .try_then(fallible_stage::<8>);

    for (position, fail_at) in [("first", 1), ("middle", 4), ("last", 8)] {
        let input = FallibleValue {
            value: u32::from(INPUT),
            fail_at,
        };
        let mut group = c.benchmark_group(format!("fallible_error/{position}"));

        group.bench_function("direct", |bencher| {
            bencher.iter(|| black_box(direct_fallible_8(black_box(input))));
        });
        group.bench_function("try_pipe", |bencher| {
            bencher.iter(|| black_box(pipeline.run(black_box(input))));
        });

        group.finish();
    }
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

fn bench_try_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_async_three_stage_ready_success");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| {
            black_box(block_on(direct_try_async_ready(black_box(
                FALLIBLE_SUCCESS,
            ))))
        });
    });

    let mut pipeline = TryAsyncPipe::new(fallible_ready_stage::<1>)
        .try_then(fallible_ready_stage::<2>)
        .try_then(fallible_ready_stage::<3>);
    group.bench_function("try_async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(FALLIBLE_SUCCESS)))));
    });

    group.finish();
}

fn bench_hundred_stages(c: &mut Criterion) {
    let mut group = c.benchmark_group("hundred_stage/sync_success");
    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_hundred(black_box(HUNDRED_SUCCESS))));
    });
    let mut pipeline = append_ninety_nine!(Pipe::new(hundred_stage), then, hundred_stage);
    group.bench_function("pipe", |bencher| {
        bencher.iter(|| black_box(pipeline.run(black_box(HUNDRED_SUCCESS))));
    });
    group.finish();

    let mut group = c.benchmark_group("hundred_stage/fallible_success");
    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(direct_hundred_try(black_box(HUNDRED_SUCCESS))));
    });
    let mut try_pipeline =
        append_ninety_nine!(TryPipe::new(hundred_try_stage), try_then, hundred_try_stage);
    group.bench_function("try_pipe", |bencher| {
        bencher.iter(|| black_box(try_pipeline.run(black_box(HUNDRED_SUCCESS))));
    });
    group.finish();

    for (position, fail_at) in [("first", 1), ("middle", 50), ("last", 100)] {
        let input = HundredValue {
            fail_at,
            ..HUNDRED_SUCCESS
        };
        let mut group = c.benchmark_group(format!("hundred_stage/fallible_error/{position}"));
        group.bench_function("direct", |bencher| {
            bencher.iter(|| black_box(direct_hundred_try(black_box(input))));
        });
        group.bench_function("try_pipe", |bencher| {
            bencher.iter(|| black_box(try_pipeline.run(black_box(input))));
        });
        group.finish();
    }

    let mut group = c.benchmark_group("hundred_stage/async_ready_success");
    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(block_on(direct_hundred_async(black_box(HUNDRED_SUCCESS)))));
    });
    let mut async_pipeline = append_ninety_nine!(
        AsyncPipe::new(hundred_ready_stage),
        then,
        hundred_ready_stage
    );
    group.bench_function("async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(async_pipeline.run(black_box(HUNDRED_SUCCESS)))));
    });
    group.finish();

    let mut group = c.benchmark_group("hundred_stage/try_async_ready_success");
    group.bench_function("direct", |bencher| {
        bencher.iter(|| {
            black_box(block_on(direct_hundred_try_async(black_box(
                HUNDRED_SUCCESS,
            ))))
        });
    });
    let mut try_async_pipeline = append_ninety_nine!(
        TryAsyncPipe::new(hundred_try_ready_stage),
        try_then,
        hundred_try_ready_stage
    );
    group.bench_function("try_async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(try_async_pipeline.run(black_box(HUNDRED_SUCCESS)))));
    });
    group.finish();

    for (position, fail_at) in [("first", 1), ("middle", 50), ("last", 100)] {
        let input = HundredValue {
            fail_at,
            ..HUNDRED_SUCCESS
        };
        let mut group = c.benchmark_group(format!("hundred_stage/try_async_error/{position}"));
        group.bench_function("direct", |bencher| {
            bencher.iter(|| black_box(block_on(direct_hundred_try_async(black_box(input)))));
        });
        group.bench_function("try_async_pipe", |bencher| {
            bencher.iter(|| black_box(block_on(try_async_pipeline.run(black_box(input)))));
        });
        group.finish();
    }
}

criterion_group!(
    composition,
    bench_sync,
    bench_fallible,
    bench_type_changing_fallible,
    bench_fallible_errors,
    bench_async,
    bench_try_async,
    bench_hundred_stages
);
criterion_main!(composition);
