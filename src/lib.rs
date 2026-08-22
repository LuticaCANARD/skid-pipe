#![no_std]
#![forbid(unsafe_code)]

//! Portable, typed composition of ordinary Rust functions.
//!
//! `skid-pipe` is dependency-free and uses only [`core`]. It adds no allocator,
//! dynamic dispatch, runtime, or executor requirement, so the same pipeline
//! types work on native targets, WebAssembly, and `no_std` firmware.
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
//! use skid_pipe::Pipe;
//!
//! let mut pipeline = Pipe::new(|value: u8| value).then_branch(
//!     |_| true,
//!     Pipe::new(|value: u8| u16::from(value)),
//!     Pipe::new(|value: u8| value > 0),
//! );
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

mod async_pipe;
mod pipe;

pub use async_pipe::AsyncPipe;
pub use pipe::{Branch, End, Pipe};
