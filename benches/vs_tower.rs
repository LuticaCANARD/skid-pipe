// `skid-pipe` against Tower's reusable `Service` composition.
//
// Tower has a different contract: callers must drive readiness before calling a
// service, and it is a `std` service/middleware framework rather than a no_std
// function pipeline. This benchmark measures that complete ready-and-call path
// with the exact same fallible `Ready` stages, but reports it separately from
// `vs_futures`.
use core::future::{Future, Ready, ready};
use std::{
    convert::Infallible,
    hint::black_box,
    pin::pin,
    task::{Context, Poll, Waker},
};

use criterion::{Criterion, criterion_group, criterion_main};
use skid_pipe::TryAsyncPipe;
use tower::{Service, ServiceBuilder, ServiceExt, service_fn};

#[derive(Clone, Copy)]
struct Value {
    value: u32,
    stage: u8,
}

const INPUT: Value = Value {
    value: 2_731,
    stage: 0,
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
fn try_ready_step(input: Value) -> Ready<Result<Value, Infallible>> {
    ready(Ok(step(input)))
}

async fn direct_three(input: Value) -> Result<Value, Infallible> {
    let value = try_ready_step(input).await?;
    let value = try_ready_step(value).await?;
    try_ready_step(value).await
}

fn tower_echo(input: Value) -> Ready<Result<Value, Infallible>> {
    ready(Ok(input))
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

fn bench_try_async_three(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_tower/try_async_3_stage_success");

    group.bench_function("direct_async_fn", |bencher| {
        bencher.iter(|| black_box(block_on(direct_three(black_box(INPUT)))));
    });

    let mut pipeline = TryAsyncPipe::new(try_ready_step)
        .try_then(try_ready_step)
        .try_then(try_ready_step);
    group.bench_function("skid_pipe", |bencher| {
        bencher.iter(|| black_box(block_on(pipeline.run(black_box(INPUT)))));
    });

    // `ready().await.call()` is Tower's normal invocation contract. `and_then`
    // layers invoke the exact same `Ready<Result<_, _>>` stage as the direct
    // and `TryAsyncPipe` arms after the inner service completes.
    let mut service = ServiceBuilder::new()
        .and_then(try_ready_step)
        .and_then(try_ready_step)
        .and_then(try_ready_step)
        .service(service_fn(tower_echo));
    group.bench_function("tower_ready_call", |bencher| {
        bencher.iter(|| {
            black_box(block_on(async {
                service
                    .ready()
                    .await
                    .expect("infallible service must be ready")
                    .call(black_box(INPUT))
                    .await
                    .expect("infallible service must not fail")
            }))
        });
    });

    group.finish();
}

criterion_group!(vs_tower, bench_try_async_three);
criterion_main!(vs_tower);
