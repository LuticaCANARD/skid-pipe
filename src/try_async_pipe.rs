use core::future::Future;

use crate::End;

/// A reusable, statically typed fallible asynchronous pipeline.
///
/// Each stage returns a [`Future`] resolving to a [`Result`]. The chain stops
/// at the first error and never enters a later stage.
pub struct TryAsyncPipe<Head, Tail = End> {
    pub(crate) head: Head,
    pub(crate) tail: Tail,
}

impl<Head> TryAsyncPipe<Head> {
    /// Starts a fallible asynchronous pipeline with its first step.
    #[inline(always)]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> TryAsyncPipe<Head, Tail> {
    /// Appends the next fallible asynchronous step to this pipeline.
    #[inline(always)]
    pub const fn try_then<Next>(self, next: Next) -> TryAsyncPipe<Next, Self> {
        TryAsyncPipe {
            head: next,
            tail: self,
        }
    }

    /// Returns a future that runs every stage from left to right, stopping at
    /// the first error.
    ///
    /// Creating the future is lazy: no stage runs until it is first polled.
    /// The future holds the mutable pipeline borrow, so one pipeline instance
    /// cannot have overlapping runs.
    ///
    /// ```compile_fail
    /// use skid_pipe::TryAsyncPipe;
    ///
    /// let mut pipeline =
    ///     TryAsyncPipe::new(|value: u8| core::future::ready(Ok::<_, ()>(value + 1)));
    /// let first = pipeline.run(1);
    /// let second = pipeline.run(2);
    /// drop((first, second));
    /// ```
    #[inline(always)]
    pub fn run<Input, Error>(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<<Self as TryAsyncChain<Input, Error>>::Output, Error>>
    where
        Self: TryAsyncChain<Input, Error>,
    {
        TryAsyncChain::run(self, input)
    }
}

/// A callable fallible asynchronous pipeline stage.
///
/// Every `FnMut(Input) -> Future<Output = Result<Output, Error>>` implements
/// this trait automatically, so callers normally pass plain functions or
/// closures to [`TryAsyncPipe::try_then`]. Implementing `TryAsyncStep` by hand
/// is supported for named stateful stages.
pub trait TryAsyncStep<Input, Error>: Sized {
    /// The success value emitted when the stage future resolves.
    type Output;

    /// The concrete future created by this stage.
    type Future<'a>: Future<Output = Result<Self::Output, Error>>
    where
        Self: 'a;

    /// Creates the stage future for one input value.
    fn call(&mut self, input: Input) -> Self::Future<'_>;
}

impl<Input, Output, Error, F, Fut> TryAsyncStep<Input, Error> for F
where
    F: FnMut(Input) -> Fut,
    Fut: Future<Output = Result<Output, Error>>,
{
    type Output = Output;
    type Future<'a>
        = Fut
    where
        Self: 'a;

    #[inline(always)]
    fn call(&mut self, input: Input) -> Self::Future<'_> {
        self(input)
    }
}

/// A complete fallible asynchronous pipeline, runnable for one input value.
///
/// [`TryAsyncPipe`] implements this trait as the recursive engine behind
/// [`TryAsyncPipe::run`]. The trait is public so builder functions can return
/// `impl TryAsyncChain<Input, Error, Output = O>` without naming the recursive
/// concrete pipeline type. External implementations must run stages from left
/// to right and stop after the first error.
///
/// `async fn` here cannot name an auto trait bound. [`TryAsyncChainSend`]
/// restates the same composition with `Send` promised, for callers that must
/// prove it.
#[allow(async_fn_in_trait)]
pub trait TryAsyncChain<Input, Error>: Sized {
    /// The success value emitted when the completed pipeline resolves.
    type Output;

    /// Creates the future that runs this chain.
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error>;
}

impl<S1, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S1, End>
where
    S1: TryAsyncStep<Input, Error>,
{
    type Output = <S1 as TryAsyncStep<Input, Error>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        self.head.call(input).await
    }
}

impl<S1, S2, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S2, TryAsyncPipe<S1, End>>
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
{
    type Output = <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.head.call(input).await?;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
    S3: TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >,
{
    type Output = <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.head.call(input).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
    S3: TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >,
    S4: TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        >,
{
    type Output = <S4 as TryAsyncStep<
        <S3 as TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >>::Output,
        Error,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.tail.head.call(input).await?;
        let carried = self.tail.tail.head.call(carried).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<
        S5,
        TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
    >
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
    S3: TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >,
    S4: TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        >,
    S5: TryAsyncStep<
            <S4 as TryAsyncStep<
                <S3 as TryAsyncStep<
                    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                    Error,
                >>::Output,
                Error,
            >>::Output,
            Error,
        >,
{
    type Output = <S5 as TryAsyncStep<
        <S4 as TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        >>::Output,
        Error,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.tail.tail.head.call(input).await?;
        let carried = self.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.head.call(carried).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, S6, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>>>
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
{
    type Output = <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.tail.tail.tail.head.call(input).await?;
        let carried = self.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.head.call(carried).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await

    }
}

impl<S1, S2, S3, S4, S5, S6, S7, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>>>>
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S7: TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
{
    type Output = <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.tail.tail.tail.tail.head.call(input).await?;
        let carried = self.tail.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.head.call(carried).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await

    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S8, TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>>>>>
where
    S1: TryAsyncStep<Input, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S7: TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S8: TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
{
    type Output = <S8 as TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.tail.tail.tail.tail.tail.head.call(input).await?;
        let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.head.call(carried).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await

    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, TailHead, TailTail, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S8, TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, TryAsyncPipe<TailHead, TailTail>>>>>>>>>
where
    TryAsyncPipe<TailHead, TailTail>: TryAsyncChain<Input, Error>,
    S1: TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>,
    S2: TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S7: TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
    S8: TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>,
{
    type Output = <S8 as TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let carried = self.tail.tail.tail.tail.tail.tail.tail.tail.run(input).await?;
        let carried = self.tail.tail.tail.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.tail.head.call(carried).await?;
        let carried = self.tail.tail.head.call(carried).await?;
        let carried = self.tail.head.call(carried).await?;
        self.head.call(carried).await

    }
}

/// The `Send` variant of [`TryAsyncChain`], for the same reason as
/// [`AsyncChainSend`](crate::AsyncChainSend): the composed future is
/// unnameable, so a `tokio::spawn` caller cannot bound it.
pub trait TryAsyncChainSend<Input, Error>: TryAsyncChain<Input, Error> {
    /// Creates the future that runs this chain, promising `Send`.
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send;
}

impl<S1, Input, Error> TryAsyncChainSend<Input, Error> for TryAsyncPipe<S1, End>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move { self.head.call(input).await }
    }
}

impl<S1, S2, Input, Error> TryAsyncChainSend<Input, Error>
    for TryAsyncPipe<S2, TryAsyncPipe<S1, End>>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>:
        Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.head.call(input).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, Input, Error> TryAsyncChainSend<Input, Error>
    for TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>:
        Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        > + Send,
    for<'a> <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Future<'a>: Send,
    <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.head.call(input).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, Input, Error> TryAsyncChainSend<Input, Error>
    for TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>:
        Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        > + Send,
    for<'a> <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Future<'a>: Send,
    <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Output: Send,
    S4: TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        > + Send,
    for<'a> <S4 as TryAsyncStep<
        <S3 as TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >>::Output,
        Error,
    >>::Future<'a>: Send,
    <S4 as TryAsyncStep<
        <S3 as TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >>::Output,
        Error,
    >>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.tail.head.call(input).await?;
            let carried = self.tail.tail.head.call(carried).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, Input, Error> TryAsyncChainSend<Input, Error>
    for TryAsyncPipe<
        S5,
        TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
    >
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>:
        Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        > + Send,
    for<'a> <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Future<'a>: Send,
    <S3 as TryAsyncStep<
        <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
        Error,
    >>::Output: Send,
    S4: TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        > + Send,
    for<'a> <S4 as TryAsyncStep<
        <S3 as TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >>::Output,
        Error,
    >>::Future<'a>: Send,
    <S4 as TryAsyncStep<
        <S3 as TryAsyncStep<
            <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
            Error,
        >>::Output,
        Error,
    >>::Output: Send,
    S5: TryAsyncStep<
            <S4 as TryAsyncStep<
                <S3 as TryAsyncStep<
                    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                    Error,
                >>::Output,
                Error,
            >>::Output,
            Error,
        > + Send,
    for<'a> <S5 as TryAsyncStep<
        <S4 as TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        >>::Output,
        Error,
    >>::Future<'a>: Send,
    <S5 as TryAsyncStep<
        <S4 as TryAsyncStep<
            <S3 as TryAsyncStep<
                <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output,
                Error,
            >>::Output,
            Error,
        >>::Output,
        Error,
    >>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.head.call(input).await?;
            let carried = self.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.head.call(carried).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, Input, Error> TryAsyncChainSend<Input, Error> for TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>>>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>: Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.head.call(input).await?;
            let carried = self.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.head.call(carried).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, Input, Error> TryAsyncChainSend<Input, Error> for TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>>>>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>: Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S7: TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.tail.head.call(input).await?;
            let carried = self.tail.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.head.call(carried).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, Input, Error> TryAsyncChainSend<Input, Error> for TryAsyncPipe<S8, TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>>>>>
where
    S1: TryAsyncStep<Input, Error> + Send,
    for<'a> <S1 as TryAsyncStep<Input, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<Input, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Future<'a>: Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S7: TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S8: TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S8 as TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S8 as TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.tail.tail.head.call(input).await?;
            let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.head.call(carried).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, TailHead, TailTail, Input, Error> TryAsyncChainSend<Input, Error> for TryAsyncPipe<S8, TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, TryAsyncPipe<TailHead, TailTail>>>>>>>>>
where
    TryAsyncPipe<TailHead, TailTail>: TryAsyncChainSend<Input, Error> + Send,
    <TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output: Send,
    S1: TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error> + Send,
    for<'a> <S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Future<'a>: Send,
    <S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output: Send,
    S2: TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S3: TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S4: TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S5: TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S6: TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S7: TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    S8: TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error> + Send,
    for<'a> <S8 as TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Future<'a>: Send,
    <S8 as TryAsyncStep<<S7 as TryAsyncStep<<S6 as TryAsyncStep<<S5 as TryAsyncStep<<S4 as TryAsyncStep<<S3 as TryAsyncStep<<S2 as TryAsyncStep<<S1 as TryAsyncStep<<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output, Error>>::Output: Send,
    Input: Send,
    Error: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Result<Self::Output, Error>> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.tail.tail.tail.run_send(input).await?;
            let carried = self.tail.tail.tail.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.tail.head.call(carried).await?;
            let carried = self.tail.tail.head.call(carried).await?;
            let carried = self.tail.head.call(carried).await?;
            self.head.call(carried).await
        }
    }
}
