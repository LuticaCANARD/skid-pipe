# skid-pipe

> Dependency-free typed function pipelines for `no_std`, with zero-cost
> static composition and opt-in type erasure.

`skid-pipe` turns a chain of ordinary Rust functions into a reusable value.
Its default build uses only `core`:

- `no_std`
- zero dependencies
- zero allocation
- static dispatch
- no runtime or executor
- no `unsafe`
- native, WebAssembly, and embedded-core compatible

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
- borrow behind `DynChain` without allocation;
- own behind `BoxedPipe` when type erasure is worth an allocation;
- reuse across native, Wasm, and `no_std` targets.

This crate does not replace Rust control flow. Branches remain ordinary
`if`/`match` expressions inside stages.

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

The default path allocates nothing and dynamically dispatches nothing.

## Examples

| Example | Demonstrates | Run |
|---|---|---|
| [`typed_sensor.rs`](examples/typed_sensor.rs) | Type-changing embedded-style processing | `cargo run --example typed_sensor` |
| [`fallible_protocol.rs`](examples/fallible_protocol.rs) | First-error short-circuiting | `cargo run --example fallible_protocol` |
| [`stateful_router.rs`](examples/stateful_router.rs) | Branching and state retained across runs | `cargo run --example stateful_router` |
| [`runtime_registry.rs`](examples/runtime_registry.rs) | Configuration-selected stages | `cargo run --example runtime_registry --features dynamic` |

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
environment. `AsyncChain` intentionally has no dyn-compatible variant because
its methods return `impl Future`.

## API boundaries and cost

A concrete pipeline type grows with every stage. Choose the cheapest boundary
that satisfies the caller:

| Boundary | Allocation | Dispatch cost | Purpose |
|---|---:|---:|---|
| concrete `Pipe` | none | static | Maximum transparency and optimization |
| `impl Chain` | none | static | Hide the concrete recursive type |
| `DynChain` | none | one indirect call per run | Borrow one of several completed pipelines |
| `BoxedPipe` | yes | indirect nested boundary | Own an erased pipeline |
| `RuntimePipe` | one box per step | indirect call per step | Select stages and order from configuration |

### Zero-cost opaque return

```rust
use skid_pipe::{Chain, Pipe};

fn build() -> impl Chain<u16, Output = bool> {
    Pipe::new(|value: u16| value as f32 / 4095.0)
        .then(|ratio: f32| ratio > 0.5)
}
```

### Allocation-free borrowed erasure

```rust
use skid_pipe::{DynChain, Pipe};

let mut double = Pipe::new(|value: i32| value * 2);
let mut negate = Pipe::new(|value: i32| -value);

let selected: DynChain<'_, i32, i32> =
    if true { &mut double } else { &mut negate };

assert_eq!(selected.run(4), 8);
```

`BoxedPipe` and `BoxedTryPipe` are available with the `alloc` feature.
Each extension adds another erased wrapper, so they are ownership tools rather
than heterogeneous workflow engines.

## Dynamic composition is opt-in

The `dynamic` feature is for the narrower case where configuration chooses
the kinds, count, and order of registered stages. It implies `alloc`.

```rust
use skid_pipe::RuntimePipe;

#[derive(Debug, PartialEq)]
enum Value {
    Raw(u8),
    Decoded(u16),
}

#[derive(Debug, PartialEq)]
enum Error {
    UnexpectedValue,
}

let mut pipeline = RuntimePipe::<Value, Error>::new();
pipeline.push(|value| match value {
    Value::Raw(raw) => Ok(Value::Decoded(u16::from(raw))),
    _ => Err(Error::UnexpectedValue),
});

assert_eq!(
    pipeline.run(Value::Raw(7)),
    Ok(Value::Decoded(7)),
);
```

Every runtime step has the common contract
`Value -> Result<Value, Error>`. A caller-defined enum preserves domain states
without `Any` or downcasting. In exchange, adjacency validation moves from
compile time to runtime, and every step allocates and dynamically dispatches.

Prefer the static core unless runtime configuration is an actual requirement.

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

## Features

- default — dependency-free, allocation-free static pipelines using only
  `core`;
- `alloc` — `BoxedPipe` and `BoxedTryPipe`;
- `dynamic` — `RuntimePipe`; implies `alloc`;
- `std` — currently implies `alloc`.

## Platform validation

CI checks the static core and opt-in features on stable Rust and the declared
MSRV, including representative targets:

- `wasm32-unknown-unknown`
- `wasm32v1-none`
- `thumbv6m-none-eabi`
- `thumbv7em-none-eabihf`
- `riscv32imac-unknown-none-elf`

The core stays ecosystem-neutral. Integrations that require a HAL, executor,
logging framework, or model runtime belong in separate adapter crates.
