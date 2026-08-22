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

## Embedded integrations

The core crate remains dependency-free for every target; there is intentionally
no `embedded` feature that changes its allocation or runtime model. If a future
integration needs `defmt`, a HAL, or another ecosystem dependency, it belongs
in an opt-in adapter crate rather than in this core API.

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo check --target wasm32-unknown-unknown
cargo check --target wasm32v1-none
cargo check --target thumbv6m-none-eabi
cargo check --target thumbv7em-none-eabihf
cargo check --target riscv32imac-unknown-none-elf
cargo check --manifest-path tests/fixtures/no_std/Cargo.toml --target wasm32v1-none
```
