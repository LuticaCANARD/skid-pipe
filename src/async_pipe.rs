use core::future::Future;

use crate::End;

/// A reusable, statically typed asynchronous function pipeline.
///
/// Each stage returns a [`Future`]. [`AsyncPipe::run`] returns the composed
/// future without allocating, polling it, or selecting an executor.
pub struct AsyncPipe<Head, Tail = End> {
    pub(crate) head: Head,
    pub(crate) tail: Tail,
}

impl<Head> AsyncPipe<Head> {
    /// Starts an asynchronous pipeline with its first step.
    #[inline(always)]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> AsyncPipe<Head, Tail> {
    /// Appends the next asynchronous step to this pipeline.
    #[inline(always)]
    pub const fn then<Next>(self, next: Next) -> AsyncPipe<Next, Self> {
        AsyncPipe {
            head: next,
            tail: self,
        }
    }

    /// Returns a future that runs every stage from left to right.
    ///
    /// The caller selects where and how that future is polled. Creating the
    /// future is lazy: no stage runs until the future is first polled. The
    /// returned future holds the mutable pipeline borrow until it is completed
    /// or dropped, so one pipeline instance cannot have overlapping runs.
    ///
    /// ```compile_fail
    /// use skid_pipe::AsyncPipe;
    ///
    /// let mut pipeline = AsyncPipe::new(|value: u8| core::future::ready(value + 1));
    /// let first = pipeline.run(1);
    /// let second = pipeline.run(2);
    /// drop((first, second));
    /// ```
    #[inline(always)]
    pub fn run<Input>(
        &mut self,
        input: Input,
    ) -> impl Future<Output = <Self as AsyncChain<Input>>::Output>
    where
        Self: AsyncChain<Input>,
    {
        AsyncChain::run(self, input)
    }
}

/// A callable asynchronous pipeline stage.
///
/// Functions and closures that return a [`Future`] implement it
/// automatically, so callers normally pass them straight to
/// [`AsyncPipe::then`]. Implementing `AsyncStep` by hand is supported for
/// named stateful stages.
pub trait AsyncStep<Input>: Sized {
    /// The value emitted when the stage future resolves.
    type Output;

    /// The concrete future created by this stage.
    type Future<'a>: Future<Output = Self::Output>
    where
        Self: 'a;

    /// Creates the stage future for one input value.
    fn call(&mut self, input: Input) -> Self::Future<'_>;
}

impl<Input, Output, F, Fut> AsyncStep<Input> for F
where
    F: FnMut(Input) -> Fut,
    Fut: Future<Output = Output>,
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

/// A complete asynchronous pipeline, runnable for one input value.
///
/// [`AsyncPipe`] implements this trait; it is the recursive engine behind
/// [`AsyncPipe::run`]. It is public so builder functions can return
/// `impl AsyncChain<Input, Output = O>` and hide the recursive concrete
/// pipeline type at zero cost.
///
/// `async fn` here cannot name an auto trait bound. [`AsyncChainSend`] restates
/// the same composition with `Send` promised, for callers that must prove it.
#[allow(async_fn_in_trait)]
pub trait AsyncChain<Input>: Sized {
    /// The value emitted when this chain's future resolves.
    type Output;

    /// Creates the future that runs this chain.
    async fn run(&mut self, input: Input) -> Self::Output;
}

impl<S1, Input> AsyncChain<Input> for AsyncPipe<S1, End>
where
    S1: AsyncStep<Input>,
{
    type Output = <S1 as AsyncStep<Input>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        self.head.call(input).await
    }
}

impl<S1, S2, Input> AsyncChain<Input> for AsyncPipe<S2, AsyncPipe<S1, End>>
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
{
    type Output = <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self.tail.head.call(input).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, Input> AsyncChain<Input> for AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>,
{
    type Output =
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self.tail.tail.head.call(input).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, Input> AsyncChain<Input>
    for AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>,
    S4: AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >,
{
    type Output = <S4 as AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self.tail.tail.tail.head.call(input).await;
        let carried = self.tail.tail.head.call(carried).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, Input> AsyncChain<Input>
    for AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>,
    S4: AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >,
    S5: AsyncStep<
        <S4 as AsyncStep<
            <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
        >>::Output,
    >,
{
    type Output = <S5 as AsyncStep<
        <S4 as AsyncStep<
            <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
        >>::Output,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self.tail.tail.tail.tail.head.call(input).await;
        let carried = self.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.head.call(carried).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, S6, Input> AsyncChain<Input>
    for AsyncPipe<
        S6,
        AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>,
    >
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>,
    S4: AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >,
    S5: AsyncStep<
        <S4 as AsyncStep<
            <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
        >>::Output,
    >,
    S6:
        AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >,
{
    type Output =
        <S6 as AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self.tail.tail.tail.tail.tail.head.call(input).await;
        let carried = self.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.head.call(carried).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, Input> AsyncChain<Input>
    for AsyncPipe<
        S7,
        AsyncPipe<
            S6,
            AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>,
        >,
    >
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>,
    S4: AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >,
    S5: AsyncStep<
        <S4 as AsyncStep<
            <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
        >>::Output,
    >,
    S6:
        AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >,
    S7: AsyncStep<
        <S6 as AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
{
    type Output = <S7 as AsyncStep<
        <S6 as AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self.tail.tail.tail.tail.tail.tail.head.call(input).await;
        let carried = self.tail.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.head.call(carried).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, Input> AsyncChain<Input>
    for AsyncPipe<
        S8,
        AsyncPipe<
            S7,
            AsyncPipe<
                S6,
                AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>,
            >,
        >,
    >
where
    S1: AsyncStep<Input>,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output>,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>,
    S4: AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >,
    S5: AsyncStep<
        <S4 as AsyncStep<
            <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
        >>::Output,
    >,
    S6:
        AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >,
    S7: AsyncStep<
        <S6 as AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
    S8: AsyncStep<
        <S7 as AsyncStep<
            <S6 as AsyncStep<
                <S5 as AsyncStep<
                    <S4 as AsyncStep<
                        <S3 as AsyncStep<
                            <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                        >>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
{
    type Output = <S8 as AsyncStep<
        <S7 as AsyncStep<
            <S6 as AsyncStep<
                <S5 as AsyncStep<
                    <S4 as AsyncStep<
                        <S3 as AsyncStep<
                            <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output,
                        >>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .head
            .call(input)
            .await;
        let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.head.call(carried).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, TailHead, TailTail, Input> AsyncChain<Input>
    for AsyncPipe<
        S8,
        AsyncPipe<
            S7,
            AsyncPipe<
                S6,
                AsyncPipe<
                    S5,
                    AsyncPipe<
                        S4,
                        AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, AsyncPipe<TailHead, TailTail>>>>,
                    >,
                >,
            >,
        >,
    >
where
    AsyncPipe<TailHead, TailTail>: AsyncChain<Input>,
    S1: AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>,
    S2: AsyncStep<
        <S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output,
    >,
    S3: AsyncStep<
        <S2 as AsyncStep<
            <S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output,
        >>::Output,
    >,
    S4:
        AsyncStep<
            <S3 as AsyncStep<
                <S2 as AsyncStep<
                    <S1 as AsyncStep<
                        <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >,
    S5: AsyncStep<
        <S4 as AsyncStep<
            <S3 as AsyncStep<
                <S2 as AsyncStep<
                    <S1 as AsyncStep<
                        <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
    S6: AsyncStep<
        <S5 as AsyncStep<
            <S4 as AsyncStep<
                <S3 as AsyncStep<
                    <S2 as AsyncStep<
                        <S1 as AsyncStep<
                            <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output,
                        >>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
    S7: AsyncStep<
        <S6 as AsyncStep<
            <S5 as AsyncStep<
                <S4 as AsyncStep<
                    <S3 as AsyncStep<
                        <S2 as AsyncStep<
                            <S1 as AsyncStep<
                                <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output,
                            >>::Output,
                        >>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
    S8: AsyncStep<
        <S7 as AsyncStep<
            <S6 as AsyncStep<
                <S5 as AsyncStep<
                    <S4 as AsyncStep<
                        <S3 as AsyncStep<
                            <S2 as AsyncStep<
                                <S1 as AsyncStep<
                                    <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output,
                                >>::Output,
                            >>::Output,
                        >>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >,
{
    type Output = <S8 as AsyncStep<
        <S7 as AsyncStep<
            <S6 as AsyncStep<
                <S5 as AsyncStep<
                    <S4 as AsyncStep<
                        <S3 as AsyncStep<
                            <S2 as AsyncStep<
                                <S1 as AsyncStep<
                                    <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output,
                                >>::Output,
                            >>::Output,
                        >>::Output,
                    >>::Output,
                >>::Output,
            >>::Output,
        >>::Output,
    >>::Output;

    #[inline(always)]
    async fn run(&mut self, input: Input) -> Self::Output {
        let carried = self
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .run(input)
            .await;
        let carried = self
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .tail
            .head
            .call(carried)
            .await;
        let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.tail.head.call(carried).await;
        let carried = self.tail.tail.head.call(carried).await;
        let carried = self.tail.head.call(carried).await;
        self.head.call(carried).await
    }
}

/// The `Send` variant of [`AsyncChain`].
///
/// [`AsyncChain::run`] returns an unnameable `impl Future`, so a caller that
/// must prove the composed future is `Send` (a `tokio::spawn` boundary) cannot
/// write that bound. This trait restates the same composition with `Send`
/// promised in the return type. A stage future stays nameable through
/// [`AsyncStep::Future`], so its bound is expressible here.
pub trait AsyncChainSend<Input>: AsyncChain<Input> {
    /// Creates the future that runs this chain, promising `Send`.
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send;
}

impl<S1, Input> AsyncChainSend<Input> for AsyncPipe<S1, End>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move { self.head.call(input).await }
    }
}

impl<S1, S2, Input> AsyncChainSend<Input> for AsyncPipe<S2, AsyncPipe<S1, End>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.head.call(input).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, Input> AsyncChainSend<Input> for AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Future<'a>:
        Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.head.call(input).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, Input> AsyncChainSend<Input>
    for AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Future<'a>:
        Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output: Send,
    S4: AsyncStep<
            <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
        > + Send,
    for<'a> <S4 as AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >>::Future<'a>: Send,
    <S4 as AsyncStep<
        <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output,
    >>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.tail.head.call(input).await;
            let carried = self.tail.tail.head.call(carried).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, Input> AsyncChainSend<Input> for AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Future<'a>: Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output: Send,
    S4: AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output> + Send,
    for<'a> <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output: Send,
    S5: AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.head.call(input).await;
            let carried = self.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.head.call(carried).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, Input> AsyncChainSend<Input> for AsyncPipe<S6, AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Future<'a>: Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output: Send,
    S4: AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output> + Send,
    for<'a> <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output: Send,
    S5: AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S6: AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.head.call(input).await;
            let carried = self.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.head.call(carried).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, Input> AsyncChainSend<Input> for AsyncPipe<S7, AsyncPipe<S6, AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Future<'a>: Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output: Send,
    S4: AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output> + Send,
    for<'a> <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output: Send,
    S5: AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S6: AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S7: AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.tail.head.call(input).await;
            let carried = self.tail.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.head.call(carried).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, Input> AsyncChainSend<Input> for AsyncPipe<S8, AsyncPipe<S7, AsyncPipe<S6, AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>>>>
where
    S1: AsyncStep<Input> + Send,
    for<'a> <S1 as AsyncStep<Input>>::Future<'a>: Send,
    <S1 as AsyncStep<Input>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<Input>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Future<'a>: Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output: Send,
    S4: AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output> + Send,
    for<'a> <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output: Send,
    S5: AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S6: AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S7: AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S8: AsyncStep<<S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S8 as AsyncStep<<S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S8 as AsyncStep<<S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.tail.tail.head.call(input).await;
            let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.head.call(carried).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, TailHead, TailTail, Input> AsyncChainSend<Input> for AsyncPipe<S8, AsyncPipe<S7, AsyncPipe<S6, AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, AsyncPipe<TailHead, TailTail>>>>>>>>>
where
    AsyncPipe<TailHead, TailTail>: AsyncChainSend<Input> + Send,
    <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output: Send,
    S1: AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output> + Send,
    for<'a> <S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Future<'a>: Send,
    <S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output: Send,
    S2: AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output> + Send,
    for<'a> <S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Future<'a>: Send,
    <S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output: Send,
    S3: AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output> + Send,
    for<'a> <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output: Send,
    S4: AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S5: AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S6: AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S7: AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    S8: AsyncStep<<S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output> + Send,
    for<'a> <S8 as AsyncStep<<S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Future<'a>: Send,
    <S8 as AsyncStep<<S7 as AsyncStep<<S6 as AsyncStep<<S5 as AsyncStep<<S4 as AsyncStep<<S3 as AsyncStep<<S2 as AsyncStep<<S1 as AsyncStep<<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output>>::Output: Send,
    Input: Send,
{
    // `async fn` cannot promise `+ Send` in its return type, which is the
    // entire reason this trait exists beside the plain one.
    #[allow(clippy::manual_async_fn)]
    #[inline(always)]
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send {
        async move {
            let carried = self.tail.tail.tail.tail.tail.tail.tail.tail.run_send(input).await;
            let carried = self.tail.tail.tail.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.tail.head.call(carried).await;
            let carried = self.tail.tail.head.call(carried).await;
            let carried = self.tail.head.call(carried).await;
            self.head.call(carried).await
        }
    }
}
