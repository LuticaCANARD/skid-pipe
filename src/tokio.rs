//! Tokio task-spawning adapters.
//!
//! This module is available with the `tokio` feature. It intentionally owns a
//! pipeline for each task: a run future borrows its pipeline and therefore
//! cannot be passed directly to [`tokio::spawn`]. Moving the pipeline into the
//! task keeps Tokio's `'static` task boundary explicit without changing the
//! core pipeline's no-std, allocation-free contract.

use crate::{AsyncChain, AsyncChainSend, TryAsyncChain, TryAsyncChainSend};

/// Spawns an infallible async chain on a Tokio runtime.
///
/// Import this trait to call [`TokioAsyncChainExt::spawn`] or
/// [`TokioAsyncChainExt::spawn_local`] on an [`AsyncChain`]. Both methods
/// consume the pipeline because the spawned task must own it.
pub trait TokioAsyncChainExt<Input>: AsyncChain<Input> + Sized {
    /// Moves this pipeline and one input into a `Send` Tokio task.
    ///
    /// This is equivalent to `tokio::spawn(async move { ... pipeline.run(input)
    /// .await })`. It requires the pipeline, input, output, and composed run
    /// future to satisfy Tokio's `Send + 'static` boundary, which is what
    /// [`AsyncChainSend`](crate::AsyncChainSend) is for: the composed future is
    /// an unnameable `impl Future`, so the bound cannot be written directly.
    ///
    /// A stage that holds something non-`Send` is refused here. Use
    /// [`spawn_local`](Self::spawn_local) for those.
    ///
    /// ```compile_fail
    /// use std::rc::Rc;
    /// use skid_pipe::{AsyncPipe, TokioAsyncChainExt};
    ///
    /// let offset = Rc::new(1_u16);
    /// let pipeline = AsyncPipe::new(move |value: u16| {
    ///     let offset = Rc::clone(&offset);
    ///     async move { value + *offset }
    /// });
    /// let _ = pipeline.spawn(4);
    /// ```
    #[inline]
    fn spawn(self, input: Input) -> tokio::task::JoinHandle<Self::Output>
    where
        Self: AsyncChainSend<Input> + Send + 'static,
        Input: Send + 'static,
        Self::Output: Send + 'static,
    {
        tokio::spawn(async move {
            let mut pipeline = self;
            AsyncChainSend::run_send(&mut pipeline, input).await
        })
    }

    /// Moves this pipeline and one input into Tokio's current local task set.
    ///
    /// Unlike [`TokioAsyncChainExt::spawn`], this accepts non-`Send` stages.
    /// It panics if called outside a Tokio `LocalSet` or local runtime, matching
    /// [`tokio::task::spawn_local`].
    #[inline]
    fn spawn_local(self, input: Input) -> tokio::task::JoinHandle<Self::Output>
    where
        Self: 'static,
        Input: 'static,
        Self::Output: 'static,
    {
        tokio::task::spawn_local(async move {
            let mut pipeline = self;
            AsyncChain::run(&mut pipeline, input).await
        })
    }
}

impl<Pipeline, Input> TokioAsyncChainExt<Input> for Pipeline where Pipeline: AsyncChain<Input> {}

/// Spawns a fallible async chain on a Tokio runtime.
///
/// Import this trait to call [`TokioTryAsyncChainExt::spawn`] or
/// [`TokioTryAsyncChainExt::spawn_local`] on a [`TryAsyncChain`]. Both methods
/// consume the pipeline because the spawned task must own it.
pub trait TokioTryAsyncChainExt<Input, Error>: TryAsyncChain<Input, Error> + Sized {
    /// Moves this pipeline and one input into a `Send` Tokio task.
    ///
    /// The error type is part of the boundary, not only the stages: an error a
    /// task cannot carry across threads is refused the same way a stage is.
    ///
    /// ```compile_fail
    /// use std::rc::Rc;
    /// use skid_pipe::{TokioTryAsyncChainExt, TryAsyncPipe};
    ///
    /// let pipeline = TryAsyncPipe::new(|value: u16| {
    ///     core::future::ready(Ok::<_, Rc<u16>>(value + 1))
    /// });
    /// let _ = pipeline.spawn(4);
    /// ```
    #[inline]
    fn spawn(self, input: Input) -> tokio::task::JoinHandle<Result<Self::Output, Error>>
    where
        Self: TryAsyncChainSend<Input, Error> + Send + 'static,
        Input: Send + 'static,
        Error: Send + 'static,
        Self::Output: Send + 'static,
    {
        tokio::spawn(async move {
            let mut pipeline = self;
            TryAsyncChainSend::run_send(&mut pipeline, input).await
        })
    }

    /// Moves this pipeline and one input into Tokio's current local task set.
    ///
    /// This accepts non-`Send` stages but requires a Tokio `LocalSet` or local
    /// runtime, just like [`tokio::task::spawn_local`].
    #[inline]
    fn spawn_local(self, input: Input) -> tokio::task::JoinHandle<Result<Self::Output, Error>>
    where
        Self: 'static,
        Input: 'static,
        Error: 'static,
        Self::Output: 'static,
    {
        tokio::task::spawn_local(async move {
            let mut pipeline = self;
            TryAsyncChain::run(&mut pipeline, input).await
        })
    }
}

impl<Pipeline, Input, Error> TokioTryAsyncChainExt<Input, Error> for Pipeline where
    Pipeline: TryAsyncChain<Input, Error>
{
}
