<img src="assets/skid-pipe.svg" alt="" width="64" height="64">

# skid-pipe

> Reusable, stateful computation pipelines with zero-allocation composition
> for embedded, Wasm, and native Rust.

Build sensor preprocessing, stateful filters, or model pre/post-processing as
typed computation modules. Compose those modules once, then run them for each
input while retaining their state. Share the computation across firmware,
simulation, and native replay; keep device I/O in the caller.

The default library uses only Rust `core`:

- `no_std`
- zero default library dependencies
- zero allocation in composition (stages control their own resource use)
- static dispatch
- no runtime or executor
- no `unsafe` anywhere in the crate (`#![forbid(unsafe_code)]`)
- native, WebAssembly, and embedded-core compatible

```rust
use skid_pipe::{Chain, Pipe};

fn preprocessing(offset: u16) -> impl Chain<u16, Output = u16> {
    Pipe::new(move |raw: u16| raw.saturating_sub(offset).min(4095))
}

fn decision() -> impl Chain<u16, Output = bool> {
    let mut high = false;
    Pipe::new(move |level: u16| {
        if level >= 3000 { high = true; }
        else if level <= 2000 { high = false; }
        high
    })
}

let mut sensor = Pipe::from_chain(preprocessing(100))
    .then_chain(decision());

assert!(sensor.run(3200));
assert!(sensor.run(2600)); // retains the decision between thresholds
assert!(!sensor.run(1500));
```

Each module may change the value type. The composed chain statically checks
every connection, including modules returned as opaque `impl Chain` values.
The [portable sensor demo](examples/portable-sensor/README.md) expands this into
decode, calibration, filtering, feature extraction, and classification:

```text
Native replay ─────┐
Wasm replay ───────┼──▶ one shared no_std sensor module ──▶ observations
Firmware adapter ─┘
```

The demo executes native and Wasm replay and checks their results. Firmware
targets are compile-checked; MCU hardware execution is not yet validated.

## When it earns its keep

Use plain Rust when a short, fixed computation is already clear, even if it
runs repeatedly:

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
- compose with other modules through `from_chain` and `then_chain`;
- run repeatedly while `FnMut` stages retain state;
- reuse across native, Wasm, and `no_std` targets.

Ordinary functions and closures can also be portable, stateful, and allocation-free.
This crate supplies a common stage/chain contract for assembling those computations;
it does not make a `.then()` expression intrinsically faster or more portable.
Branches remain ordinary `if`/`match` expressions inside stages. Sampling cadence,
queues, missing inputs, and concurrency remain the caller's responsibility.

## Compose computation modules

`Pipe::from_chain(module)` starts a synchronous pipeline from any `Chain`, and
`.then_chain(module)` appends another chain. Both own the supplied module through
the public `ChainStage<C>` adapter. Construction does not run stages; each run
executes them in order and preserves their existing state. `.then(stage)` still
accepts ordinary functions, closures, and named `Step` implementations.

These adapters also work with builders returning `impl Chain` and chains that
borrow local state. They do not box, erase, clone, or reset a module. Rebuild a
module explicitly when a new input stream needs fresh state. As with `.then()`,
incompatible types are rejected when the result is used as a `Chain`, such as
at `run()` or a typed builder boundary.

Chain-as-stage currently covers synchronous `Pipe`. Fallible and async adapters
need their own error, borrow, and cancellation contracts before being added.
The existing `TryPipe`, `AsyncPipe`, and `TryAsyncPipe` APIs remain available.

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
middleware. [Alternatives](#alternatives) compares both, and measures this
crate against the `futures` combinators and against a plain `async fn`.

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
cargo +1.98.1 bench --bench composition -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 50
```

Criterion is a benchmark-only development dependency; the published library's
default normal dependency graph remains empty. Neither the opt-in `tokio`
feature nor `wide` is included in these measurements.

Treat the resulting nanoseconds as machine-local evidence, not a portable
performance promise. Flash size, stack use, and assembly require target-specific
measurement before making embedded optimization claims.

The checked-in [benchmark snapshot](BENCHMARKS.md) records the current direct
comparison, module-composition case, 100-stage runtime, future layout, and the
historical Cortex-M code-size probe. It intentionally reports unfavorable
long-async cases too.

A second benchmark, `benches/vs_futures.rs`, compares composition against the
`futures` combinators and against a plain `async fn`. See
[Alternatives](#alternatives).

`benches/vs_tower.rs` separately compares the same fallible `Ready` stages to
Tower's normal ready-and-call service path. It is intentionally separate: Tower
implements a service contract rather than a local function pipeline.

## Examples

| Example | Demonstrates | Run |
|---|---|---|
| [Portable sensor](examples/portable-sensor/README.md) | Shared stateful modules; native/Wasm execution comparison; firmware compilation | `python3 scripts/check-portable-sensor.py` |
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

`AsyncPipe` and `TryAsyncPipe` also retain `FnMut` state. Update the captured
state in the closure before constructing its future:

```rust
use skid_pipe::AsyncPipe;

# async fn example() {
let mut calls = 0_u32;
let mut pipeline = AsyncPipe::new(move |value: u16| {
    calls += 1;
    core::future::ready((value, calls))
});

assert_eq!(pipeline.run(7).await, (7, 1));
assert_eq!(pipeline.run(7).await, (7, 2));
# }
```

The closure runs when the pipeline's future is polled, so dropping an unpolled
run does not update the counter. Changes made by a stage that has already run
are retained when a later stage fails or the run is cancelled.

Updating a copied capture **inside** an `async move` block behaves differently:

```rust
use skid_pipe::AsyncPipe;

# async fn example() {
let mut calls = 0_u32;
let mut pipeline = AsyncPipe::new(move |value: u16| async move {
    calls += 1;
    (value, calls)
});

assert_eq!(pipeline.run(7).await, (7, 1));
assert_eq!(pipeline.run(7).await, (7, 1)); // each future updates its own copy
# }
```

Here `u32` is `Copy`, so each future receives a fresh copy. Moving owned,
non-`Copy` state out of the closure can instead make it `FnOnce`, which cannot
serve as a reusable stage. The blanket `AsyncStep` implementation accepts
`FnMut` functions returning a fixed future type; that future cannot borrow the
closure's own mutable state.

When state must be updated inside the future, capture shared state by reference
or implement a named `AsyncStep` / `TryAsyncStep`. A named stage's
`Future<'a>` may borrow `&'a mut self`, including across suspension points.
For example, shared local state can use [`Cell`](https://doc.rust-lang.org/core/cell/struct.Cell.html):

```rust
use core::cell::Cell;
use skid_pipe::AsyncPipe;

# async fn example() {
let calls = Cell::new(0_u32);
let mut pipeline = AsyncPipe::new(|value: u16| {
    let calls = &calls;
    async move {
        calls.set(calls.get() + 1);
        (value, calls.get())
    }
});

assert_eq!(pipeline.run(7).await, (7, 1));
assert_eq!(pipeline.run(7).await, (7, 2));
# }
```

A shared `Cell` reference is not `Send`; use suitable synchronized state if the
future must move between threads. `TryAsyncPipe` supports the same state
patterns, with errors stopping later stages.

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
environment. Creating a run future is lazy and has constant work with respect
to the chain length: it captures the pipeline borrow and input, but no stage
runs and no per-group state is built until the future is first polled. `run`
holds the mutable pipeline borrow until its future completes or is dropped, so
a stateful pipeline instance cannot run concurrently. The default core crate
does not depend on any executor.

### Tokio feature

Enable the optional integration when the application already uses Tokio:

```toml
[dependencies]
skid-pipe = { version = "0.4", features = ["tokio"] }
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

`spawn` requires the pipeline, input, output, and composed run future to meet
Tokio's `Send + 'static` boundary. A run future created from a pipeline that
stays on the caller's stack borrows that pipeline and therefore cannot itself
be made `'static`.

The composed future is an unnameable `impl Future`, so generic code cannot
add a `Send` bound directly to `AsyncChain::run`'s return type.
`AsyncChainSend<'run, Input>` and `TryAsyncChainSend<'run, Input, Error>`
promise `Send` for the duration of one pipeline borrow. Their `run_send`
methods also support stages that borrow local state:

```rust
use skid_pipe::{AsyncChainSend, AsyncPipe};

# async fn example() {
let offset = 3_u32;
let mut pipeline = AsyncPipe::new(|value: u32| core::future::ready(value + offset));
let future = pipeline.run_send(4);
fn require_send<F: core::future::Future + Send>(future: F) -> F { future }
assert_eq!(require_send(future).await, 7);
# }
```

Use `run_send` when borrowed stage state must produce a `Send` future. Rust's
GAT lifetime limitations can prevent proving `Send` for the ordinary `run`
future in this case. `run_send` checks stage futures for the actual borrow
lifetime and does not require the captured references to be `'static`.

Tokio's `spawn` still requires ownership and `'static`. A builder hiding an
owned pipeline's concrete type must promise `Send` execution for every borrow:

```rust
use skid_pipe::{AsyncChain, AsyncChainSend, AsyncPipe};

async fn fetch(value: u8) -> u16 { u16::from(value) }
async fn classify(value: u16) -> bool { value > 10 }

fn build() -> impl AsyncChain<u8, Output = bool> + for<'run> AsyncChainSend<'run, u8> {
    AsyncPipe::new(fetch).then(classify)
}
```

The fallible equivalent is `for<'run> TryAsyncChainSend<'run, Input, Error>`.
For a generic helper borrowing a pipeline, bind the particular borrow instead:
`P: AsyncChainSend<'run, Input>` with `pipeline: &'run mut P`.

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
declared Rust 1.98.1 MSRV without requiring callers to raise rustc's default
recursion limit. Async chains put each group of sixteen stages into one `async`
block, and rustc overlaps a group's stage futures into a single slot, so the
run future stops growing once a group is full.

Past 127 stages that no longer holds: the calling crate has to raise
`#![recursion_limit]` itself. The `wide` feature widens a group to
thirty-two, which shrinks the run future further — a 100-stage chain goes from
120 B to 72 B — but does not move that 127-stage ceiling.

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

The crate contains no unsafe code at all, and `#![forbid(unsafe_code)]` keeps
it that way. Async sequencing is an ordinary `async` block per group of stages,
so rustc generates each state machine, its discriminant, its drop glue and its
pin projection, and only one stage future in a group is live at a time because
the compiler overlaps them. Callers never need unsafe code either.

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

## Alternatives

Several crates carry "pipeline" in their name while composing different things.
Knowing which one you need settles most of the choice:

| | Composes | Result is |
|---|---|---|
| `pipe-trait`, `pipeline`, `pipeop`, `apply` | a value through functions | evaluated on the spot |
| `futures` combinators | futures | a chain one `await` consumes |
| `tower` | request/response services | a `Service` with readiness |
| `skid-pipe` | functions | a value you run repeatedly |

`x.pipe(f).pipe(g)` runs immediately and leaves nothing behind, so those crates
are not alternatives to this one despite the shared vocabulary. Use `tower` when
the problem is readiness, backpressure, retry, timeout, or HTTP middleware;
`skid-pipe` models none of those and should not be bent into them. `tower` needs
`std`.

`futures` is the real overlap: it is `no_std`-capable and its combinators chain
async stages. The difference is that it composes futures, so a caller running
the same computation twice builds the chain twice. `benches/vs_futures.rs`
measures that on identical stage bodies, payloads, and `Ready` futures:

The [2026-09-13 remeasurement](BENCHMARKS.md)
used Rust 1.98.1 on an Intel i9-9900K under WSL2/Linux. Ranges below span the
point estimates of two runs with identical settings; they are not confidence
intervals.

| Case | Direct async fn | skid-pipe | futures | futures / skid-pipe |
|---|---:|---:|---:|---:|
| Async, 3 stages | 9.516–10.153 ns | 9.474–9.858 ns | 30.452–30.490 ns | 3.09–3.22x |
| Try async, 3 stages | 14.068–16.277 ns | 14.101–15.801 ns | 33.444–34.076 ns | 2.12–2.42x |
| Try async, 3 stages, first error | 8.348–8.566 ns | 8.326–8.856 ns | 18.411–21.453 ns | 2.08–2.58x |
| Async, 10 stages | 31.751–40.519 ns | 32.061–32.775 ns | 102.23–139.48 ns | 3.12–4.35x |
| Try async, 10 stages, first error | 9.269–18.866 ns | 11.879–11.950 ns | 53.741–79.262 ns | 4.50–6.67x |

In these fixtures, skid-pipe takes about 68–69% less time than futures at three
ordinary async stages and 68–77% less at ten. The fallible first-error case at
ten stages takes 78–85% less time. Pipeline construction is outside the loop,
while each futures chain is rebuilt inside it.

Direct async fn and skid-pipe are close on the ordinary three- and ten-stage
workloads; their ordering can change between runs. This does not establish a
consistent speed advantage over direct code. A long pipeline also has costs:
the separate 100-stage TryAsyncPipe first-error fixture takes 21.34 ns versus
7.87 ns directly in this run. The report retains the previous run's full
confidence-interval appendix and the current point-estimate tables.

These measurements exercise immediately-ready futures and do not predict
network/DB throughput or executor scheduling. The older Intel/WSL2 snapshot
uses a different compiler and machine and is not a Rust-upgrade comparison.
Use skid-pipe when the assembled computation should be a reusable typed value;
a short, fixed computation remains straightforward as an ordinary async fn.

State across calls can live in a `FnMut` stage or a named stage that lends its
state to its future. A plain `async fn` can similarly accept mutable state as an
argument. See [Stateful pipelines](#stateful-pipelines) for the supported
patterns and the copied-capture pitfall.

Tower is the closest reusable abstraction with a different purpose. Its
services have a readiness protocol and address server/client middleware; it is
not `no_std`. A separate benchmark uses the same fallible `Ready` stages in a
three-stage `ServiceBuilder::and_then` stack and invokes it through Tower's
normal `ready().await.call()` path:

| Group | plain `async fn` | `skid-pipe` | Tower ready + call |
|---|---:|---:|---:|
| try async, 3 stages, success | 12.654 ns | 13.896 ns | 35.703 ns |

The Rust 1.98.1 remeasurement gives a 2.57x Tower/skid-pipe ratio in this
fixture. Tower provides a readiness and service contract that skid-pipe does
not implement. Its payload differs from the futures benchmark above, so
compare the implementations within each table. Use Tower for service
readiness and backpressure; use skid-pipe for a local, typed computation chain.

Task/channel pipeline crates (`async-pipes`, `pumps`, and `pipelines`),
type-keyed workflow kits (`pipeline-toolkit`), and scratchpad executors
(`pipexec`) are also adjacent rather than direct nanosecond competitors. Their
fair comparison is a multi-item throughput, p99 latency, memory, and
backpressure workload; [BENCHMARKS.md](BENCHMARKS.md) records the exact
boundary rather than presenting a misleading single-item ranking.

See [BENCHMARKS.md](BENCHMARKS.md) for the full comparison and its method.

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

CI checks the static core on stable Rust and the declared MSRV (Rust 1.98.1),
including representative targets:

- `wasm32-unknown-unknown`
- `wasm32v1-none`
- `thumbv6m-none-eabi`
- `thumbv7em-none-eabihf`
- `riscv32imac-unknown-none-elf`

The core stays ecosystem-neutral. Integrations that require a HAL, executor,
logging framework, or model runtime belong in separate adapter crates.

The portable sensor CI job additionally executes the same shared Rust computation
as a native binary and as `wasm32-wasip1` under Node's WASI host. It compares 77
samples across three fresh streams and checks an explicit expected result for
the default replay. The integer demo avoids floating-point tolerance differences.
Its core-only Wasm, ARM, and RISC-V checks establish compilation, not hardware
execution, real-time deadlines, or target memory use.

## Versioning and compatibility

The minimum supported Rust version (MSRV) is Rust 1.98.1. CI checks both the MSRV
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
cargo test --features wide
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.98.1 bench --bench composition -- --warm-up-time 1 --measurement-time 2 --sample-size 50
cargo check --target wasm32-unknown-unknown
cargo check --target wasm32v1-none
cargo check --target thumbv6m-none-eabi
cargo check --target thumbv7em-none-eabihf
cargo check --target riscv32imac-unknown-none-elf
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target wasm32v1-none
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target thumbv6m-none-eabi
cargo check --target thumbv6m-none-eabi --features wide
cargo +nightly-2026-09-10 miri test --test async_pipeline --test borrowed_send --test erasure --test try_async_pipeline --test hundred_stages
```
