#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]

//! Portable, typed composition of ordinary Rust functions.
//!
//! `skid-pipe`'s default feature set is dependency-free and uses only [`core`].
//! It adds no allocator, dynamic dispatch, runtime, or executor requirement,
//! so the same pipeline types work on native targets, WebAssembly, and
//! `no_std` firmware. The opt-in `tokio` feature adds task-spawning adapters
//! for Tokio applications without changing the default core.
//! The crate contains no `unsafe` code at all: async sequencing is ordinary
//! `async` blocks, so the compiler generates every state machine, its drop
//! glue, and its pin projection.
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
//! # Fallible asynchronous pipeline
//!
//! [`TryAsyncPipe`] composes asynchronous `Result`-returning functions and
//! stops at the first error, without selecting an executor or allocating.
//!
//! ```
//! use skid_pipe::TryAsyncPipe;
//!
//! async fn fetch(value: u8) -> Result<u16, &'static str> {
//!     Ok(u16::from(value))
//! }
//!
//! async fn validate(value: u16) -> Result<bool, &'static str> {
//!     Ok(value > 10)
//! }
//!
//! # async fn example() {
//! let mut pipeline = TryAsyncPipe::new(fetch).try_then(validate);
//! assert_eq!(pipeline.run(12).await, Ok(true));
//! # }
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
//! # Type names
//!
//! A pipeline's concrete type nests with every step
//! (`Pipe<F3, Pipe<F2, Pipe<F1, End>>>`). Builder functions can hide that
//! name without changing the static pipeline:
//!
//! Return `impl Chain<Input, Output = O>` (or `impl TryChain`,
//! `impl AsyncChain`, or `impl TryAsyncChain`) from a builder function. This
//! remains zero-cost and preserves compile-time validation of every
//! connection.
//!
//! ```
//! use skid_pipe::{Chain, Pipe};
//!
//! fn build() -> impl Chain<u16, Output = bool> {
//!     Pipe::new(|value: u16| value as f32 / 4095.0).then(|value: f32| value > 0.5)
//! }
//!
//! let mut pipeline = build();
//! assert!(pipeline.run(3000));
//! ```
//!
//! The crate deliberately has no type-erased or runtime-configured pipeline.
//! [`AsyncChain`] also returns a concrete future, so it remains allocation-free.
//!
//! # Type checking
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
//!
//! ```compile_fail
//! use skid_pipe::TryAsyncPipe;
//!
//! async fn decode(_: u8) -> Result<u16, ()> { Ok(0) }
//! async fn needs_boolean(_: bool) -> Result<u32, ()> { Ok(0) }
//!
//! let mut pipeline = TryAsyncPipe::new(decode).try_then(needs_boolean);
//! let _ = pipeline.run(1_u8);
//! ```

/// Walks `.tail` once per stage below the one being reached.
///
/// Shared by both ladders: it never names a pipeline type, only the `tail`
/// field both of them have.
macro_rules! chain_at {
    ($this:expr;) => { $this };
    ($this:expr; $s:ident $($rest:ident)*) => { chain_at!($this.tail; $($rest)*) };
}

mod async_pipe;
mod pipe;
#[cfg(feature = "tokio")]
mod tokio;
mod try_async_pipe;
mod try_pipe;

pub use async_pipe::{AsyncChain, AsyncChainSend, AsyncPipe, AsyncStep};
pub use pipe::{Chain, End, Pipe, Step};
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use tokio::{TokioAsyncChainExt, TokioTryAsyncChainExt};
pub use try_async_pipe::{TryAsyncChain, TryAsyncChainSend, TryAsyncPipe, TryAsyncStep};
pub use try_pipe::{TryChain, TryPipe, TryStep};
