// `skid-pipe` against the `futures` combinators, benchmark-only.
//
// The two compose different things. `FutureExt::then` composes futures: the
// chain is a value that one `await` consumes, so a caller that runs the same
// computation twice builds it twice. `AsyncPipe` composes functions: the
// pipeline is built once and issues a fresh run future per call. Every arm here
// therefore pays for whatever it must rebuild per run, which is the difference
// under test. Stage bodies, payloads and the `Ready` futures they return are
// identical across arms.
use core::future::{Future, Ready, ready};
use std::{
    hint::black_box,
    pin::pin,
    task::{Context, Poll, Waker},
};

use criterion::{Criterion, criterion_group, criterion_main};
use futures::future::{FutureExt, TryFutureExt};
use skid_pipe::{AsyncPipe, TryAsyncPipe};

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

#[inline(never)]
fn ready_step(input: Value) -> Ready<Value> {
    ready(step(input))
}

#[inline(never)]
fn try_ready_step(input: Value) -> Ready<Result<Value, u8>> {
    ready(try_step(input))
}

// A plain `async fn` is the zero-dependency alternative to both crates: it is
// reusable and composes the same stages, so it is the baseline everything else
// has to beat.
async fn direct_three(input: Value) -> Value {
    let value = ready_step(input).await;
    let value = ready_step(value).await;
    ready_step(value).await
}

async fn direct_try_three(input: Value) -> Result<Value, u8> {
    let value = try_ready_step(input).await?;
    let value = try_ready_step(value).await?;
    try_ready_step(value).await
}

macro_rules! then_ten {
    ($head:expr, $stage:path) => {
        $head
            .then($stage)
            .then($stage)
            .then($stage)
            .then($stage)
            .then($stage)
            .then($stage)
            .then($stage)
            .then($stage)
            .then($stage)
    };
}

macro_rules! and_then_ten {
    ($head:expr, $stage:path) => {
        $head
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
            .and_then($stage)
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

// Unrolled, not a `for` loop: the other two arms are static chains, and a
// runtime loop is a different shape for the optimizer to work on.
async fn direct_ten(input: Value) -> Value {
    let value = ready_step(input).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    let value = ready_step(value).await;
    ready_step(value).await
}

async fn direct_try_ten(input: Value) -> Result<Value, u8> {
    let value = try_ready_step(input).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    let value = try_ready_step(value).await?;
    try_ready_step(value).await
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

fn bench_async_three(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_futures/async_3_stage_success");

    group.bench_function("direct_async_fn", |bencher| {
        bencher.iter(|| black_box(block_on(direct_three(black_box(SUCCESS)))));
    });

    let mut pipeline = AsyncPipe::new(ready_step).then(ready_step).then(ready_step);
    group.bench_function("skid_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(SUCCESS)))));
    });

    // Rebuilt every iteration because one `await` consumes it.
    group.bench_function("futures_then", |bencher| {
        bencher.iter(|| {
            let chain = ready_step(black_box(SUCCESS))
                .then(ready_step)
                .then(ready_step);
            black_box(block_on(chain))
        });
    });

    group.finish();
}

fn bench_try_async_three(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_futures/try_async_3_stage_success");

    group.bench_function("direct_async_fn", |bencher| {
        bencher.iter(|| black_box(block_on(direct_try_three(black_box(SUCCESS)))));
    });

    let mut pipeline = TryAsyncPipe::new(try_ready_step)
        .try_then(try_ready_step)
        .try_then(try_ready_step);
    group.bench_function("skid_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(SUCCESS)))));
    });

    group.bench_function("futures_and_then", |bencher| {
        bencher.iter(|| {
            let chain = try_ready_step(black_box(SUCCESS))
                .and_then(try_ready_step)
                .and_then(try_ready_step);
            black_box(block_on(chain))
        });
    });

    group.finish();
}

fn bench_try_async_first_error(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_futures/try_async_3_stage_first_error");

    group.bench_function("direct_async_fn", |bencher| {
        bencher.iter(|| black_box(block_on(direct_try_three(black_box(FAIL_FIRST)))));
    });

    let mut pipeline = TryAsyncPipe::new(try_ready_step)
        .try_then(try_ready_step)
        .try_then(try_ready_step);
    group.bench_function("skid_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(FAIL_FIRST)))));
    });

    group.bench_function("futures_and_then", |bencher| {
        bencher.iter(|| {
            let chain = try_ready_step(black_box(FAIL_FIRST))
                .and_then(try_ready_step)
                .and_then(try_ready_step);
            black_box(block_on(chain))
        });
    });

    group.finish();
}

fn bench_async_ten(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_futures/async_10_stage_success");

    group.bench_function("direct_async_fn", |bencher| {
        bencher.iter(|| black_box(block_on(direct_ten(black_box(SUCCESS)))));
    });

    let mut pipeline = append_nine!(AsyncPipe::new(ready_step), then, ready_step);
    group.bench_function("skid_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(SUCCESS)))));
    });

    group.bench_function("futures_then", |bencher| {
        bencher.iter(|| {
            let chain = then_ten!(ready_step(black_box(SUCCESS)), ready_step);
            black_box(block_on(chain))
        });
    });

    group.finish();
}

fn bench_try_async_ten_first_error(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_futures/try_async_10_stage_first_error");

    group.bench_function("direct_async_fn", |bencher| {
        bencher.iter(|| black_box(block_on(direct_try_ten(black_box(FAIL_FIRST)))));
    });

    let mut pipeline = append_nine!(TryAsyncPipe::new(try_ready_step), try_then, try_ready_step);
    group.bench_function("skid_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(FAIL_FIRST)))));
    });

    group.bench_function("futures_and_then", |bencher| {
        bencher.iter(|| {
            let chain = and_then_ten!(try_ready_step(black_box(FAIL_FIRST)), try_ready_step);
            black_box(block_on(chain))
        });
    });

    group.finish();
}

criterion_group!(
    vs_futures,
    bench_async_three,
    bench_try_async_three,
    bench_try_async_first_error,
    bench_async_ten,
    bench_try_async_ten_first_error
);
criterion_main!(vs_futures);
