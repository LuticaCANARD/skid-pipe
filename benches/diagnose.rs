// Diagnostic benchmark, not part of the maintained snapshot.
//
// `BENCHMARKS.md` reports two `TryAsyncPipe` rows far above their direct
// baseline: the 3-stage ready-success row and the 100-stage first-error row.
// The maintained bench measures the gap but cannot say where it goes. These
// groups split each row into the costs that make it up:
//
// - `construct/*` creates and drops a run future without polling it, so it
//   measures only the nest of link futures a run builds.
// - `same_shape/*` runs `AsyncPipe` and `TryAsyncPipe` over identical payloads
//   and stage bodies, so the delta is the fallible machinery alone.
// - `first_error_depth/*` walks the chain length, so the per-group fixed cost
//   of a short-circuit shows up as a slope.
use core::future::{Future, Ready, ready};
use std::{
    hint::black_box,
    pin::pin,
    task::{Context, Poll, Waker},
};

use criterion::{Criterion, criterion_group, criterion_main};
use skid_pipe::{AsyncPipe, TryAsyncPipe};

#[macro_use]
#[path = "support/footprint.rs"]
mod footprint;

#[derive(Clone, Copy)]
struct Value {
    value: u32,
    stage: u8,
    fail_at: u8,
}

const SUCCESS: Value = Value {
    value: 2_731,
    stage: 0,
    fail_at: 0,
};

const FAIL_FIRST: Value = Value {
    value: 2_731,
    stage: 0,
    fail_at: 1,
};

#[inline(never)]
fn step(mut input: Value) -> Value {
    input.stage += 1;
    input.value = input
        .value
        .rotate_left(u32::from(input.stage % 31))
        .wrapping_add(u32::from(input.stage) * 97);
    input
}

#[inline(never)]
fn try_step(input: Value) -> Result<Value, u8> {
    let output = step(input);
    if output.stage == output.fail_at {
        Err(output.stage)
    } else {
        Ok(output)
    }
}

// The two stage functions below wrap the same bodies. `same_shape` pairs them
// so the only difference between its arms is `Result`.
#[inline(never)]
fn ready_step(input: Value) -> Ready<Value> {
    ready(step(input))
}

#[inline(never)]
fn try_ready_step(input: Value) -> Ready<Result<Value, u8>> {
    ready(try_step(input))
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

fn bench_construct(c: &mut Criterion) {
    let mut group = c.benchmark_group("construct");

    let mut three = TryAsyncPipe::new(try_ready_step)
        .try_then(try_ready_step)
        .try_then(try_ready_step);
    group.bench_function("try_async_pipe/3_stage", |bencher| {
        bencher.iter(|| drop(black_box(three.run(black_box(SUCCESS)))));
    });

    let mut hundred =
        append_ninety_nine!(TryAsyncPipe::new(try_ready_step), try_then, try_ready_step);
    group.bench_function("try_async_pipe/100_stage", |bencher| {
        bencher.iter(|| drop(black_box(hundred.run(black_box(SUCCESS)))));
    });

    let mut hundred_plain = append_ninety_nine!(AsyncPipe::new(ready_step), then, ready_step);
    group.bench_function("async_pipe/100_stage", |bencher| {
        bencher.iter(|| drop(black_box(hundred_plain.run(black_box(SUCCESS)))));
    });

    group.finish();
}

fn bench_same_shape(c: &mut Criterion) {
    let mut group = c.benchmark_group("same_shape/3_stage_success");

    let mut plain = AsyncPipe::new(ready_step).then(ready_step).then(ready_step);
    group.bench_function("async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(plain.run(black_box(SUCCESS)))));
    });

    let mut fallible = TryAsyncPipe::new(try_ready_step)
        .try_then(try_ready_step)
        .try_then(try_ready_step);
    group.bench_function("try_async_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(fallible.run(black_box(SUCCESS)))));
    });

    group.finish();
}

fn bench_first_error_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("first_error_depth");

    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(block_on(try_ready_step(black_box(FAIL_FIRST)))));
    });

    let mut one = TryAsyncPipe::new(try_ready_step);
    group.bench_function("1_stage", |bencher| {
        bencher.iter(|| black_box(block_on(one.run(black_box(FAIL_FIRST)))));
    });

    let mut three = TryAsyncPipe::new(try_ready_step)
        .try_then(try_ready_step)
        .try_then(try_ready_step);
    group.bench_function("3_stage", |bencher| {
        bencher.iter(|| black_box(block_on(three.run(black_box(FAIL_FIRST)))));
    });

    let mut ten = append_nine!(TryAsyncPipe::new(try_ready_step), try_then, try_ready_step);
    group.bench_function("10_stage", |bencher| {
        bencher.iter(|| black_box(block_on(ten.run(black_box(FAIL_FIRST)))));
    });

    let mut hundred =
        append_ninety_nine!(TryAsyncPipe::new(try_ready_step), try_then, try_ready_step);
    group.bench_function("100_stage", |bencher| {
        bencher.iter(|| black_box(block_on(hundred.run(black_box(FAIL_FIRST)))));
    });

    group.finish();
}

criterion_group!(
    diagnose,
    bench_construct,
    bench_same_shape,
    bench_first_error_depth
);
criterion_main!(diagnose);
