# skid-pipe

> Reusable, state-capable, fully static computation pipelines for Rust `core`.

`skid-pipe` turns a chain of ordinary Rust functions into a reusable value.
Its shipped library uses only `core`:

- `no_std`
- zero library dependencies
- zero allocation
- static dispatch
- no runtime or executor
- no `unsafe`
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

The native Criterion benchmark compares equivalent three-stage direct calls
with `Pipe`, `TryPipe`, and `AsyncPipe` composition. Construction is outside
the measured loop, matching a reusable pipeline's normal use:

```sh
cargo bench --bench composition
```

Criterion is a benchmark-only development dependency; the published library's
normal dependency graph remains empty.

Treat the resulting nanoseconds as machine-local evidence, not a portable
performance promise. Flash size, stack use, and assembly require target-specific
measurement before making embedded optimization claims.

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
environment. `run` holds the mutable pipeline borrow until its future
completes, so a stateful pipeline instance cannot run concurrently. The core
crate does not depend on any executor.

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

`Step`, `TryStep`, and `AsyncStep` are public and open to hand-written
implementations for named stateful stages. The execution traits are `Sized`;
the core deliberately offers no type-erased, boxed, or runtime-configured
pipeline. That keeps every stage connection statically checked,
allocation-free, and free of dynamic dispatch.

## What this crate is not

`skid-pipe` is deliberately not:

- an HTTP middleware stack;
- an async runtime or executor;
- a parallel stream processor;
- a persistent workflow engine;
- a state-machine DSL;
- a plugin loader;
- a replacement for straightforward local variables and `?`.

If you need readiness, backpressure, timeout, retry, or network middleware, use
a service abstraction such as Tower. If the computation is local and one-shot,
ordinary procedural Rust is usually clearer.

## Platform validation

CI checks the static core on stable Rust and the declared MSRV (Rust 1.86), including
representative targets:

- `wasm32-unknown-unknown`
- `wasm32v1-none`
- `thumbv6m-none-eabi`
- `thumbv7em-none-eabihf`
- `riscv32imac-unknown-none-elf`

The core stays ecosystem-neutral. Integrations that require a HAL, executor,
logging framework, or model runtime belong in separate adapter crates.

## License

Licensed under the [MIT License](LICENSE-MIT).

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo bench --bench composition
cargo check --target wasm32-unknown-unknown
cargo check --target wasm32v1-none
cargo check --target thumbv6m-none-eabi
cargo check --target thumbv7em-none-eabihf
cargo check --target riscv32imac-unknown-none-elf
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target wasm32v1-none
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target thumbv6m-none-eabi
```
