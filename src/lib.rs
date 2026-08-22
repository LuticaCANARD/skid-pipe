#![no_std]
#![forbid(unsafe_code)]

//! Portable, typed composition of ordinary Rust functions.
//!
//! `skid-pipe` is dependency-free. Its default feature set uses only [`core`]
//! and adds no allocator, dynamic dispatch, runtime, or executor requirement,
//! so the same pipeline types work on native targets, WebAssembly, and
//! `no_std` firmware. Allocation-backed layers are explicitly opt-in.
//!
//! # Synchronous pipeline
//!
//! ```
//! use skid_pipe::Pipe;
//!
//! let mut pipeline = Pipe::new(|value: u8| value + 1)
//!     .then(|value| value * 2);
//!
//! assert_eq!(pipeline.run(4), 10);
//! ```
//!
//! # Fallible pipeline
//!
//! ```
//! use skid_pipe::TryPipe;
//!
//! fn decode(value: u8) -> Result<u16, &'static str> {
//!     if value == 0 { Err("empty") } else { Ok(u16::from(value)) }
//! }
//!
//! fn classify(value: u16) -> Result<bool, &'static str> {
//!     Ok(value > 10)
//! }
//!
//! let mut pipeline = TryPipe::new(decode).try_then(classify);
//! assert_eq!(pipeline.run(12), Ok(true));
//! ```
//!
//! # Asynchronous pipeline
//!
//! [`AsyncPipe`] composes functions that return [`core::future::Future`]. It
//! does not poll the future itself or select an executor.
//!
//! ```
//! use skid_pipe::AsyncPipe;
//!
//! async fn increment(value: u8) -> u8 {
//!     value + 1
//! }
//!
//! async fn double(value: u8) -> u8 {
//!     value * 2
//! }
//!
//! # async fn example() {
//! let mut pipeline = AsyncPipe::new(increment).then(double);
//! assert_eq!(pipeline.run(4).await, 10);
//! # }
//! ```
//!
//! # Branching
//!
//! Branching is an ordinary `if` or `match` inside a stage, so this crate
//! adds no combinator for it. `match` dispatches over any number of arms,
//! and the compiler already requires every arm to produce the same type.
//!
//! ```
//! use skid_pipe::Pipe;
//!
//! let mut pipeline = Pipe::new(|value: i32| value)
//!     .then(|value: i32| match value.signum() {
//!         1 => value * 2,
//!         -1 => -value,
//!         _ => 0,
//!     })
//!     .then(|value: i32| value + 1);
//!
//! assert_eq!(pipeline.run(4), 9);
//! assert_eq!(pipeline.run(-4), 5);
//! ```
//!
//! The same holds for [`AsyncPipe`], where only the selected arm is awaited.
//! A stage closure is [`FnMut`], so a branch that keeps state across runs
//! holds it in a [`Cell`](core::cell::Cell) captured by shared reference
//! rather than moving it into the returned future.
//!
//! # Type erasure
//!
//! A pipeline's concrete type nests with every step
//! (`Pipe<F3, Pipe<F2, Pipe<F1, End>>>`). Three opt-in layers hide that
//! name, ordered by cost:
//!
//! - Return `impl Chain<Input, Output = O>` (or `impl TryChain` /
//!   `impl AsyncChain`) from a builder function — zero cost.
//! - Borrow any pipeline as [`DynChain`] / [`DynTryChain`] — no allocation,
//!   one indirect call per run, works on every `no_std` target.
//! - With the `alloc` feature (or `std`, which implies it), own a fully
//!   erased pipeline as [`BoxedPipe`] / [`BoxedTryPipe`] and compose it at
//!   runtime.
//!
//! ```
//! use skid_pipe::{Chain, DynChain, Pipe};
//!
//! fn build() -> impl Chain<u16, Output = bool> {
//!     Pipe::new(|value: u16| value as f32 / 4095.0).then(|value: f32| value > 0.5)
//! }
//!
//! let mut pipeline = build();
//! let erased: DynChain<'_, u16, bool> = &mut pipeline;
//! assert!(erased.run(3000));
//! ```
//!
//! [`AsyncChain`] supports only the first layer: its `run` returns
//! `impl Future`, so the trait is not dyn-compatible and no unboxed erasure
//! exists for asynchronous pipelines.
//!
//! # Dynamic composition
//!
//! The opt-in `dynamic` feature provides [`RuntimePipe`] for configurations
//! that select registered steps at runtime. It implies `alloc`; each step is
//! boxed and dynamically dispatched. A caller-defined carrier enum represents
//! heterogeneous logical values, and runtime connection failures use the
//! caller's error type. The default build remains fully static and
//! allocation-free.
//!
//! ```compile_fail
//! use skid_pipe::Pipe;
//!
//! fn decode(_: u8) -> u16 { 0 }
//! fn needs_boolean(_: bool) -> u32 { 0 }
//!
//! let mut pipeline = Pipe::new(decode).then(needs_boolean);
//! let _ = pipeline.run(1_u8);
//! ```
//!
//! ```compile_fail
//! use skid_pipe::TryPipe;
//!
//! let mut pipeline = TryPipe::new(|value: u8| Ok::<_, u8>(value))
//!     .try_then(|value| Ok::<_, bool>(value));
//! let _ = pipeline.run(1_u8);
//! ```
//!
//! ```compile_fail
//! use skid_pipe::AsyncPipe;
//!
//! async fn decode(_: u8) -> u16 { 0 }
//! async fn needs_boolean(_: bool) -> u32 { 0 }
//!
//! let mut pipeline = AsyncPipe::new(decode).then(needs_boolean);
//! let _ = pipeline.run(1_u8);
//! ```

#[cfg(feature = "alloc")]
extern crate alloc;

mod async_pipe;
#[cfg(feature = "alloc")]
mod boxed;
#[cfg(feature = "dynamic")]
mod dynamic;
mod pipe;
mod try_pipe;

pub use async_pipe::{AsyncChain, AsyncPipe, AsyncStep};
#[cfg(feature = "alloc")]
pub use boxed::{BoxedPipe, BoxedTryPipe};
#[cfg(feature = "dynamic")]
pub use dynamic::{RuntimePipe, RuntimeStep};
pub use pipe::{Chain, DynChain, End, Pipe, Step};
pub use try_pipe::{DynTryChain, TryChain, TryPipe, TryStep};
