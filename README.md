# skid-pipe

> A tiny `no_std`, allocation-free typed pipeline for composing ordinary Rust
> computations from left to right.

- `no_std`
- zero dependencies
- no allocation or dynamic dispatch added by the crate
- no runtime or executor
- native, WebAssembly, and embedded-core compatible

`skid-pipe` composes the computation itself into a reusable value. It does not
know about HTTP, Tokio, Embassy, JavaScript, model runtimes, or hardware.

## Synchronous composition

```rust
use skid_pipe::Pipe;

fn preprocess(value: u16) -> f32 {
    value as f32 / 4095.0
}

fn classify(value: f32) -> bool {
    value > 0.5
}

let mut pipeline = Pipe::new(preprocess).then(classify);

assert!(pipeline.run(3000));
assert!(!pipeline.run(100));
```

Each stage may change the value type. The compiler checks that each concrete
`run` call connects compatible input and output types. `run` takes `&mut self`
so pure functions and stateful `FnMut` closures use the same API.

## Fallible composition

`TryPipe` composes standard `Result<T, E>` functions and stops at the first
error. All stages use one error type; map individual domain errors to that type
explicitly before composing them.

```rust
use skid_pipe::TryPipe;

fn decode(value: u8) -> Result<u16, &'static str> {
    if value == 0 { Err("empty") } else { Ok(u16::from(value)) }
}

fn classify(value: u16) -> Result<bool, &'static str> {
    Ok(value > 10)
}

let mut pipeline = TryPipe::new(decode).try_then(classify);

assert_eq!(pipeline.run(12), Ok(true));
assert_eq!(pipeline.run(0), Err("empty"));
```

## Static branching

`then_branch` selects one sub-pipeline without allocating or dynamically
dispatching. The predicate borrows the intermediate value; exactly one branch
then consumes it. Both branches must produce the same type, after which normal
composition continues.

```rust
use skid_pipe::Pipe;

let mut pipeline = Pipe::new(|value: i32| value).then_branch(
    |value: &i32| *value >= 0,
    Pipe::new(|value: i32| value * 2),
    Pipe::new(|value: i32| -value),
);

assert_eq!(pipeline.run(4), 8);
assert_eq!(pipeline.run(-4), 4);
```

`AsyncPipe::then_branch` has the same contract. Predicate evaluation is
synchronous, while only the selected branch future is awaited.

## Asynchronous composition

```rust
use skid_pipe::AsyncPipe;

async fn fetch(value: u8) -> u8 {
    value + 1
}

async fn transform(value: u8) -> u16 {
    u16::from(value) * 2
}

# async fn example() {
let mut pipeline = AsyncPipe::new(fetch).then(transform);
let output = pipeline.run(4).await;

assert_eq!(output, 10);
# }
```

`AsyncPipe` returns a future but does not poll it. Use the executor owned by
your application, whether that is a browser/Wasm integration, Embassy, Tokio,
or another environment. The returned future holds the mutable pipeline borrow
until it completes, so stateful stages cannot be run concurrently. The core
crate does not depend on any executor.

## Type erasure

A pipeline's concrete type nests with every step
(`Pipe<F3, Pipe<F2, Pipe<F1, End>>>`). Three opt-in layers hide that name,
ordered by cost; the default build keeps the first two, which stay
allocation-free.

Return `impl Chain` from a builder function (zero cost), or borrow any
pipeline as `DynChain` / `DynTryChain` (no allocation, one indirect call per
run):

```rust
use skid_pipe::{Chain, DynChain, Pipe};

fn build() -> impl Chain<u16, Output = bool> {
    Pipe::new(|value: u16| value as f32 / 4095.0).then(|value: f32| value > 0.5)
}

let mut pipeline = build();
let erased: DynChain<'_, u16, bool> = &mut pipeline;
assert!(erased.run(3000));
```

With the `alloc` feature (or `std`, which implies it), `BoxedPipe` and
`BoxedTryPipe` own a fully erased pipeline and compose it at runtime:

```rust
use skid_pipe::{BoxedPipe, Pipe};

let offsets = vec![1, 2, 3];
let mut pipeline = BoxedPipe::new(Pipe::new(|value: i32| value));

for offset in offsets {
    pipeline = pipeline.then(move |value| value + offset);
}

assert_eq!(pipeline.run(10), 16);
```

`Step`, `TryStep`, and `AsyncStep` are public and open to hand-written
implementations for named stateful stages. `AsyncChain` supports only the
`impl AsyncChain` boundary layer: its `run` returns `impl Future`, so the
trait is not dyn-compatible and the crate offers no boxed asynchronous
pipeline.

## Features

- `alloc` — `BoxedPipe` and `BoxedTryPipe`; requires only the `alloc` crate,
  so it works on `no_std` targets with a heap allocator.
- `std` — currently just implies `alloc`.

The default feature set is empty and the core stays dependency- and
allocation-free.

## Embedded integrations

The core crate remains dependency-free for every target; there is intentionally
no `embedded` feature that changes its allocation or runtime model. If a future
integration needs `defmt`, a HAL, or another ecosystem dependency, it belongs
in an opt-in adapter crate rather than in this core API.

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --all-features
cargo check --target wasm32-unknown-unknown
cargo check --target wasm32v1-none
cargo check --target thumbv6m-none-eabi
cargo check --target thumbv7em-none-eabihf
cargo check --target riscv32imac-unknown-none-elf
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target wasm32v1-none
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target thumbv6m-none-eabi
```
