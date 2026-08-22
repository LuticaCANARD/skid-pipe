//! Tokio task-spawning adapters.
//!
//! This module is available with the `tokio` feature. It intentionally owns a
//! pipeline for each task: a run future borrows its pipeline and therefore
//! cannot be passed directly to [`tokio::spawn`]. Moving the pipeline into the
//! task keeps Tokio's `'static` task boundary explicit without changing the
//! core pipeline's no-std, allocation-free contract.

use crate::{AsyncChain, TryAsyncChain};

/// Spawns an infallible async chain on a Tokio runtime.
///
/// Import this trait to call [`TokioAsyncChainExt::spawn`] or
/// [`TokioAsyncChainExt::spawn_local`] on an [`AsyncChain`]. Both methods
/// consume the pipeline because the spawned task must own it.
pub trait TokioAsyncChainExt<Input>: AsyncChain<Input> + Sized {
    /// Moves this pipeline and one input into a `Send` Tokio task.
    ///
    /// This is equivalent to `tokio::spawn(async move { ... pipeline.run(input)
    /// .await })`. It requires the pipeline, input, output, and concrete run
    /// future to satisfy Tokio's `Send + 'static` boundary.
    #[inline]
    fn spawn(self, input: Input) -> tokio::task::JoinHandle<Self::Output>
    where
        Self: Send + 'static,
        Input: Send + 'static,
        Self::Output: Send + 'static,
        for<'a> Self::Future<'a>: Send,
    {
        tokio::spawn(async move {
            let mut pipeline = self;
            AsyncChain::run(&mut pipeline, input).await
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
    #[inline]
    fn spawn(self, input: Input) -> tokio::task::JoinHandle<Result<Self::Output, Error>>
    where
        Self: Send + 'static,
        Input: Send + 'static,
        Error: Send + 'static,
        Self::Output: Send + 'static,
        for<'a> Self::Future<'a>: Send,
    {
        tokio::spawn(async move {
            let mut pipeline = self;
            TryAsyncChain::run(&mut pipeline, input).await
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
