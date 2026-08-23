# skid-pipe

> Reusable, state-capable, fully static computation pipelines for Rust `core`.

`skid-pipe` turns a chain of ordinary Rust functions into a reusable value.
Its default core uses only `core`:

- `no_std`
- zero default library dependencies
- zero allocation
- static dispatch
- no runtime or executor
- safe public API; `unsafe` is isolated to async future pin projection
- native, WebAssembly, and embedded-core compatible

It is not an immediate-value pipe operator, a `Result`-only framework, or a
network middleware stack. It is for defining an ordinary computation once and
running that same typed computation repeatedly.

```rust
use skid_pipe::Pipe;

let mut classify = Pipe::new(|raw: u16| raw as f32 / 4095.0)
    .then(|ratio: f32| ratio > 0.5);

assert!(classify.run(3000));
assert!(!classify.run(100));
```

Each stage may change the value type. Rust checks every adjacent connection:

```text
u16 ──▶ f32 ──▶ bool
```

## When it earns its keep

Use plain Rust when a computation runs once:

```rust
# fn normalize(raw: u8) -> Result<u16, ()> { Ok(u16::from(raw)) }
# fn classify(value: u16) -> Result<bool, ()> { Ok(value > 10) }
# fn process(raw: u8) -> Result<bool, ()> {
let normalized = normalize(raw)?;
let classified = classify(normalized)?;
# Ok(classified)
# }
```

Use `skid-pipe` when the composed computation itself must become a value that
you can:

- build in one module and return as `impl Chain`;
- run repeatedly while `FnMut` stages retain state;
- reuse across native, Wasm, and `no_std` targets.

This crate does not replace Rust control flow. Branches remain ordinary
`if`/`match` expressions inside stages.

## Why this shape

`Pipe::new(a).then(b).then(c)` stores `a → b → c` as one reusable value while
preserving the type of every connection. This makes it a useful boundary for
portable computation such as sensor processing, serialization, validation, or
model pre/post-processing.

The deliberately narrow contract is the point:

- ordinary functions and `FnMut` closures, including captured state;
- synchronous, fallible, and async computations as separate static APIs;
- no allocator, executor, dynamic dispatch, macro expansion, or service model.

Use a value-piping crate when execution should happen immediately. Use Tower
when the problem is service readiness, backpressure, retry, timeout, or HTTP
middleware.

## Core model

Appending a stage creates a new recursive type:

```rust
use skid_pipe::Pipe;

fn f1(value: u8) -> u16 { u16::from(value) }
fn f2(value: u16) -> u32 { u32::from(value) }
fn f3(value: u32) -> bool { value > 10 }

let _pipeline = Pipe::new(f1).then(f2).then(f3);
```

```text
Pipe<F3, Pipe<F2, Pipe<F1, End>>>
```

The newest stage is stored at the head, but `run` evaluates the tail first.
Values therefore flow left to right exactly as written:

```text
input ──▶ F1 ──▶ F2 ──▶ F3 ──▶ output
```

The core path allocates nothing and uses no dynamic dispatch.

## Cost model and benchmark

The recursive generic representation gives the compiler complete visibility of
every stage. It adds no allocation or virtual call, but exact generated code
still depends on the compiler, target, and stages. Long pipelines with many
distinct type combinations can also increase compile time and binary size.

The native Criterion benchmark compares equivalent direct calls with `Pipe`,
`TryPipe`, `AsyncPipe`, and `TryAsyncPipe` composition. Fallible cases cover
different chain lengths and error positions, including first, middle, and last
errors in a 100-stage chain. Async comparisons use the same
`core::future::Ready`-returning stages on both sides. Construction is outside
the measured loop, matching a reusable pipeline's normal use:

```sh
cargo +1.86 bench --bench composition -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 50
```

Criterion is a benchmark-only development dependency; the published library's
default normal dependency graph remains empty. The opt-in `tokio` feature is
not included in these measurements.

Treat the resulting nanoseconds as machine-local evidence, not a portable
performance promise. Flash size, stack use, and assembly require target-specific
measurement before making embedded optimization claims.

The checked-in [benchmark snapshot](BENCHMARKS.md) records the full direct
comparison, including 100-stage runtime, future layout, and a Cortex-M code
size probe. It intentionally reports the unfavorable long-async cases too.

## Examples

| Example | Demonstrates | Run |
|---|---|---|
| [`typed_sensor.rs`](examples/typed_sensor.rs) | Type-changing embedded-style processing | `cargo run --example typed_sensor` |
| [`fallible_protocol.rs`](examples/fallible_protocol.rs) | First-error short-circuiting | `cargo run --example fallible_protocol` |
| [`stateful_router.rs`](examples/stateful_router.rs) | Branching and state retained across runs | `cargo run --example stateful_router` |

## Fallible pipelines

`TryPipe` composes ordinary `Result<T, E>` functions. It runs left to right
and stops at the first error.

```rust
use skid_pipe::TryPipe;

#[derive(Debug, PartialEq)]
enum Error {
    Empty,
}

fn decode(value: u8) -> Result<u16, Error> {
    if value == 0 {
        Err(Error::Empty)
    } else {
        Ok(u16::from(value))
    }
}

fn classify(value: u16) -> Result<bool, Error> {
    Ok(value > 10)
}

let mut pipeline = TryPipe::new(decode).try_then(classify);

assert_eq!(pipeline.run(12), Ok(true));
assert_eq!(pipeline.run(0), Err(Error::Empty));
```

All stages use one caller-selected error type. The crate does not synthesize,
box, or convert errors.

## Stateful pipelines

Stages implement `FnMut`, so a pipeline may retain state between completed
runs without allocation:

```rust
use skid_pipe::Pipe;

let mut calls = 0_u32;
let mut pipeline = Pipe::new(move |value: u16| {
    calls += 1;
    (value, calls)
});

assert_eq!(pipeline.run(7), (7, 1));
assert_eq!(pipeline.run(7), (7, 2));
```

The mutable pipeline borrow makes this sequencing explicit. Synchronization for
state shared outside the pipeline remains the caller's responsibility.

## Branching

Branching is a normal stage. The selected arm may run its own typed sub-pipeline,
and an enum can merge branches with different output types:

```rust
use skid_pipe::Pipe;

enum Input {
    Sensor(u16),
    Command(&'static str),
}

enum Routed {
    SensorScore(u32),
    CommandAccepted(bool),
}

let mut sensor = Pipe::new(|raw: u16| u32::from(raw) * 2);
let mut command = Pipe::new(|name: &'static str| name == "start");

let mut pipeline = Pipe::new(|input: Input| input).then(move |input| match input {
    Input::Sensor(raw) => Routed::SensorScore(sensor.run(raw)),
    Input::Command(name) => Routed::CommandAccepted(command.run(name)),
});
```

There is no branch DSL to learn and no branch object to allocate.

## Async without an executor dependency

`AsyncPipe` composes functions that return futures. It returns the composed
future without boxing it, polling it, or selecting an executor.

```rust
use skid_pipe::AsyncPipe;

async fn fetch(value: u8) -> u16 {
    u16::from(value)
}

async fn classify(value: u16) -> bool {
    value > 10
}

# async fn example() {
let mut pipeline = AsyncPipe::new(fetch).then(classify);
assert!(pipeline.run(12).await);
# }
```

The caller may use Tokio, Embassy, a browser/Wasm integration, or any other
environment. Creating a run future is lazy: the first stage is not called until
the future is polled. Construction is not free, though: the run future writes
one pointer and one state tag per group of eight stages, so an unpolled run — or
one that short-circuits on the first stage's error — still costs `O(stages / 8)`
stores. `run` holds the mutable pipeline borrow until its future completes or is
dropped, so a stateful pipeline instance cannot run concurrently. The default
core crate does not depend on any executor.

### Lazy construction feature

`skid-pipe` builds a run future's whole nest of link futures when `run` is
called. That is the faster arrangement end to end, and it is the default. A
workload that creates run futures it may drop before ever polling them — a
select arm that loses, a task cancelled at its first await point — pays that
construction for nothing. For those, enable:

```toml
[dependencies]
skid-pipe = { version = "0.2", features = ["lazy-construction"] }
```

Each link future then parks its input and builds its tail on the first poll,
so creating one is a single layer's work no matter how long the chain is. The
public API, the run future's size, and the guarantee that no stage runs before
the first poll are all unchanged; only where the nest is built moves.

It is a trade, not a free win. On the snapshot machine, creating a 100-stage
run future drops from 9.8489 ns to 1.2309 ns, while the 100-stage first-error
run regresses 19.6% and the three-stage success rows 9.2% (`AsyncPipe`) and
1.6% (`TryAsyncPipe`). Measure your own workload before enabling it; if your
run futures are always polled to completion, leave it off. See
[BENCHMARKS.md](BENCHMARKS.md) for the full numbers.

### Tokio feature

Enable the optional integration when the application already uses Tokio:

```toml
[dependencies]
skid-pipe = { version = "0.2", features = ["tokio"] }
```

The feature enables Tokio's minimal `rt` feature and exports two extension
traits: `TokioAsyncChainExt` and `TokioTryAsyncChainExt`. They move a pipeline
and one input into a Tokio task, making the required ownership boundary
explicit. The default feature set remains `no_std`, dependency-free, and free
of Tokio.

For [`tokio::spawn`](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html),
import the extension trait and consume the pipeline with `spawn`:

```rust,ignore
use skid_pipe::{AsyncPipe, TokioAsyncChainExt};

let task = AsyncPipe::new(fetch)
    .then(classify)
    .spawn(12);
```

`spawn` requires the pipeline, input, output, and concrete run future to meet
Tokio's `Send + 'static` boundary. A run future created from a pipeline that
stays on the caller's stack borrows that pipeline and therefore cannot itself
be made `'static`.

For a non-`Send` stage, use Tokio's
[`LocalSet::spawn_local`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html#method.spawn_local)
with the same ownership pattern:

```rust,ignore
use skid_pipe::{AsyncPipe, TokioAsyncChainExt};

let result = local.run_until(async {
    AsyncPipe::new(local_stage)
        .then(classify)
        .spawn_local(12)
        .await
}).await;
```

One stateful pipeline processes runs sequentially. To handle jobs concurrently,
move a distinct pipeline value into each task, or keep one pipeline in a
long-lived task and send jobs to that task. Aborting a task drops its active
run future; already-polled `FnMut` state changes are not rolled back.

`TryAsyncPipe` provides the same static composition for futures that resolve
to `Result<T, E>`. It stops before calling any stage after the first error:

```rust
use skid_pipe::TryAsyncPipe;

async fn fetch(value: u8) -> Result<u16, &'static str> {
    Ok(u16::from(value))
}

async fn validate(value: u16) -> Result<bool, &'static str> {
    if value == 0 { Err("empty") } else { Ok(value > 10) }
}

# async fn example() {
let mut pipeline = TryAsyncPipe::new(fetch).try_then(validate);
assert_eq!(pipeline.run(12).await, Ok(true));
# }
```

Its returned future likewise keeps a mutable borrow until completion or drop.
Dropping a pending run permits a later run, but state changes already made by
polled `FnMut` stages are retained.

## Long async chains and embedded stacks

All four pipeline variants are compiled and executed with 100 stages on the
declared Rust 1.86 MSRV without requiring callers to raise rustc's default
recursion limit. Async chains use flat eight-stage internal state machines to
keep compiler layout recursion bounded, and each machine borrows the
sub-pipeline it drives as one pointer rather than one per stage.

This is a supported compilation and behavior boundary, not a promise that a
100-stage future fits every firmware task stack. Pipeline future size grows
with the number of stages, captured state, intermediate values, and the
largest active stage future. Measure the concrete target before deployment:

```rust
# use skid_pipe::AsyncPipe;
let mut pipeline = AsyncPipe::new(|value: u8| core::future::ready(value + 1));
let future = pipeline.run(1);
let bytes = core::mem::size_of_val(&future);
assert!(bytes > 0);
```

On a constrained executor task, prefer shorter pipelines with explicit await
boundaries over one 100-stage future. This lets each phase's run future finish
before the next is created and gives the linker and stack analysis smaller,
more useful units to inspect.

The synchronous paths contain no unsafe code. Async sequencing keeps only one
active stage future in an internal union and uses isolated pin projection in
`src/future.rs`; the crate denies unsafe code outside that module, and CI fails
if any other file opts back in. The active union variant is tracked explicitly,
dropped in place exactly once, and tested under Miri across pending,
cancellation, short-circuit, and 100-stage paths.
Callers never need unsafe code.

## API boundaries

A concrete pipeline type grows with every stage. Return an opaque static trait
from a builder function when callers should not name that recursive type:

```rust
use skid_pipe::{Chain, Pipe};

fn build() -> impl Chain<u16, Output = bool> {
    Pipe::new(|value: u16| value as f32 / 4095.0)
        .then(|ratio: f32| ratio > 0.5)
}
```

`Step`, `TryStep`, `AsyncStep`, and `TryAsyncStep` are public and open to
hand-written implementations for named stateful stages. Builder functions may
return the corresponding `Chain`, `TryChain`, `AsyncChain`, or `TryAsyncChain`
trait to hide their recursive concrete type. These execution traits are
`Sized`; the core deliberately offers no type-erased, boxed, or
runtime-configured pipeline. That keeps every stage connection statically
checked, allocation-free, and free of dynamic dispatch.

## What this crate is not

`skid-pipe` is deliberately not:

- an HTTP middleware stack;
- an async runtime or executor;
- a retry, timeout, or authentication framework;
- a global-state or dependency-injection container;
- a parallel stream processor;
- a persistent workflow engine;
- a state-machine DSL;
- a plugin loader;
- a replacement for straightforward local variables and `?`.

If you need readiness, backpressure, timeout, retry, or network middleware, use
a service abstraction such as Tower. If the computation is local and one-shot,
ordinary procedural Rust is usually clearer.

## Platform validation

CI checks the static core on stable Rust and the declared MSRV (Rust 1.86),
including representative targets:

- `wasm32-unknown-unknown`
- `wasm32v1-none`
- `thumbv6m-none-eabi`
- `thumbv7em-none-eabihf`
- `riscv32imac-unknown-none-elf`

The core stays ecosystem-neutral. Integrations that require a HAL, executor,
logging framework, or model runtime belong in separate adapter crates.

## Versioning and compatibility

The minimum supported Rust version (MSRV) is Rust 1.86. CI checks both the MSRV
and stable Rust. An MSRV increase is treated as a semver-minor change and is
recorded in the [changelog](CHANGELOG.md).

While the crate is below 1.0, a minor release may change public APIs. Patch
releases are reserved for fixes, documentation, and compatible performance
improvements; they do not intentionally break existing code. When practical,
an API replacement is deprecated before removal. Any required migration and
its replacement API are called out in the changelog for the release.

The core compatibility boundary remains `no_std`, allocation-free, statically
dispatched composition. HTTP clients, retry, timeout, authentication, and
global state management belong to applications or separate transport/service
layers.

## License

Licensed under the [MIT License](LICENSE-MIT).

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --features tokio
cargo test --features lazy-construction
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.86 bench --bench composition -- --warm-up-time 1 --measurement-time 2 --sample-size 50
cargo check --target wasm32-unknown-unknown
cargo check --target wasm32v1-none
cargo check --target thumbv6m-none-eabi
cargo check --target thumbv7em-none-eabihf
cargo check --target riscv32imac-unknown-none-elf
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target wasm32v1-none
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target thumbv6m-none-eabi
cargo check --target thumbv6m-none-eabi --features lazy-construction
cargo +nightly-2026-04-03 miri test --test async_pipeline --test erasure --test try_async_pipeline --test hundred_stages
cargo +nightly-2026-04-03 miri test --features lazy-construction --test async_pipeline --test erasure --test try_async_pipeline --test hundred_stages
```
