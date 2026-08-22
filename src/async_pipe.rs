use core::future::Future;

use crate::{
    AsyncStart, End, FirstStageFuture, ThenFuture, ThenOctFuture, ThenPairFuture, ThenQuadFuture,
};

type StepOutput<Step, Input> = <Step as AsyncStep<Input>>::Output;
type ChainOutput<Chain, Input> = <Chain as AsyncChain<Input>>::Output;

/// A reusable, statically typed asynchronous function pipeline.
///
/// Each stage returns a [`Future`]. [`AsyncPipe::run`] returns the composed
/// future without allocating, polling it, or selecting an executor.
pub struct AsyncPipe<Head, Tail = End> {
    // `crate::future` projects these to reach one stage at a time from a
    // single stored pipeline pointer.
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
    /// future is lazy: no stage runs until the future is first polled.
    /// Construction is still not free, because it writes one pipeline pointer
    /// and one state tag per group of eight stages, so an unpolled or
    /// immediately dropped run costs `O(stages / 8)` stores. The mutable
    /// receiver permits `FnMut` stages to retain state between completed runs.
    /// The returned future holds the mutable pipeline borrow until it is
    /// completed or dropped, so one pipeline instance cannot have overlapping
    /// runs. Dropping an incomplete future cancels that run and releases the
    /// borrow; state changes already made by a polled stage are not rolled back.
    /// To satisfy a `tokio::spawn`-style `Send + 'static` boundary, move the
    /// pipeline into an `async move` task and call `run` inside that task.
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
    pub fn run<Input>(&mut self, input: Input) -> <Self as AsyncChain<Input>>::Future<'_>
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
/// [`AsyncPipe::run`]. It is public so builder
/// functions can return `impl AsyncChain<Input, Output = O>` and hide the
/// recursive concrete pipeline type at zero cost.
///
/// `run` returns the concrete associated [`AsyncChain::Future`], keeping
/// execution allocation-free.
pub trait AsyncChain<Input>: Sized {
    /// The value emitted when this chain's future resolves.
    type Output;

    /// The concrete future created by this chain.
    type Future<'a>: Future<Output = Self::Output>
    where
        Self: 'a;

    /// Creates the future that runs this chain.
    fn run(&mut self, input: Input) -> Self::Future<'_>;
}

impl<Head, Input> AsyncChain<Input> for AsyncPipe<Head, End>
where
    Head: AsyncStep<Input>,
{
    type Output = Head::Output;
    type Future<'a>
        = FirstStageFuture<'a, AsyncStart, Head, Input, Head::Future<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        FirstStageFuture::new(&mut self.head, input)
    }
}

impl<S1, S2, Input> AsyncChain<Input> for AsyncPipe<S2, AsyncPipe<S1, End>>
where
    AsyncPipe<S1, End>: AsyncChain<Input>,
    S2: AsyncStep<ChainOutput<AsyncPipe<S1, End>, Input>>,
{
    type Output = StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>;
    type Future<'a>
        = ThenFuture<
        'a,
        Self,
        Input,
        <AsyncPipe<S1, End> as AsyncChain<Input>>::Future<'a>,
        <S2 as AsyncStep<ChainOutput<AsyncPipe<S1, End>, Input>>>::Future<'a>,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenFuture::new(self, input)
    }
}

impl<S1, S2, S3, Input> AsyncChain<Input> for AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>
where
    AsyncPipe<S1, End>: AsyncChain<Input>,
    S2: AsyncStep<ChainOutput<AsyncPipe<S1, End>, Input>>,
    S3: AsyncStep<StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>,
{
    type Output = StepOutput<S3, StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>;
    type Future<'a>
        = ThenPairFuture<
        'a,
        Self,
        Input,
        <AsyncPipe<S1, End> as AsyncChain<Input>>::Future<'a>,
        <S2 as AsyncStep<ChainOutput<AsyncPipe<S1, End>, Input>>>::Future<'a>,
        <S3 as AsyncStep<StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>>::Future<'a>,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenPairFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, Input> AsyncChain<Input>
    for AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>
where
    AsyncPipe<S2, AsyncPipe<S1, End>>: AsyncChain<Input>,
    S3: AsyncStep<ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>,
    S4: AsyncStep<StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>,
{
    type Output =
        StepOutput<S4, StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>;
    type Future<'a>
        =
        ThenPairFuture<
            'a,
            Self,
            Input,
            <AsyncPipe<S2, AsyncPipe<S1, End>> as AsyncChain<Input>>::Future<'a>,
            <S3 as AsyncStep<ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>::Future<'a>,
            <S4 as AsyncStep<
                StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>,
            >>::Future<'a>,
        >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenPairFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, Input> AsyncChain<Input>
    for AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>
where
    AsyncPipe<S1, End>: AsyncChain<Input>,
    S2: AsyncStep<ChainOutput<AsyncPipe<S1, End>, Input>>,
    S3: AsyncStep<StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>,
    S4: AsyncStep<StepOutput<S3, StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>>,
    S5: AsyncStep<
        StepOutput<S4, StepOutput<S3, StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>>,
    >,
{
    type Output = StepOutput<
        S5,
        StepOutput<S4, StepOutput<S3, StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>>,
    >;
    type Future<'a>
        =
        ThenQuadFuture<
            'a,
            Self,
            Input,
            <AsyncPipe<S1, End> as AsyncChain<Input>>::Future<'a>,
            <S2 as AsyncStep<ChainOutput<AsyncPipe<S1, End>, Input>>>::Future<'a>,
            <S3 as AsyncStep<StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>>::Future<'a>,
            <S4 as AsyncStep<
                StepOutput<S3, StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>,
            >>::Future<'a>,
            <S5 as AsyncStep<
                StepOutput<
                    S4,
                    StepOutput<S3, StepOutput<S2, ChainOutput<AsyncPipe<S1, End>, Input>>>,
                >,
            >>::Future<'a>,
        >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenQuadFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, S6, Input> AsyncChain<Input>
    for AsyncPipe<
        S6,
        AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>>,
    >
where
    AsyncPipe<S2, AsyncPipe<S1, End>>: AsyncChain<Input>,
    S3: AsyncStep<ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>,
    S4: AsyncStep<StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>,
    S5: AsyncStep<
        StepOutput<S4, StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>,
    >,
    S6: AsyncStep<
        StepOutput<
            S5,
            StepOutput<S4, StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>,
        >,
    >,
{
    type Output = StepOutput<
        S6,
        StepOutput<
            S5,
            StepOutput<S4, StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>,
        >,
    >;
    type Future<'a>
        =
        ThenQuadFuture<
            'a,
            Self,
            Input,
            <AsyncPipe<S2, AsyncPipe<S1, End>> as AsyncChain<Input>>::Future<'a>,
            <S3 as AsyncStep<ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>>::Future<'a>,
            <S4 as AsyncStep<
                StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>,
            >>::Future<'a>,
            <S5 as AsyncStep<
                StepOutput<
                    S4,
                    StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>,
                >,
            >>::Future<'a>,
            <S6 as AsyncStep<
                StepOutput<
                    S5,
                    StepOutput<
                        S4,
                        StepOutput<S3, ChainOutput<AsyncPipe<S2, AsyncPipe<S1, End>>, Input>>,
                    >,
                >,
            >>::Future<'a>,
        >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenQuadFuture::new(self, input)
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
    AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>: AsyncChain<Input>,
    S4: AsyncStep<ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>>,
    S5: AsyncStep<
        StepOutput<S4, ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>>,
    >,
    S6: AsyncStep<
        StepOutput<
            S5,
            StepOutput<S4, ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>>,
        >,
    >,
    S7: AsyncStep<
        StepOutput<
            S6,
            StepOutput<
                S5,
                StepOutput<
                    S4,
                    ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>,
                >,
            >,
        >,
    >,
{
    type Output = StepOutput<
        S7,
        StepOutput<
            S6,
            StepOutput<
                S5,
                StepOutput<
                    S4,
                    ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>,
                >,
            >,
        >,
    >;
    type Future<'a>
        =
        ThenQuadFuture<
            'a,
            Self,
            Input,
            <AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>> as AsyncChain<Input>>::Future<'a>,
            <S4 as AsyncStep<
                ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>,
            >>::Future<'a>,
            <S5 as AsyncStep<
                StepOutput<
                    S4,
                    ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>,
                >,
            >>::Future<'a>,
            <S6 as AsyncStep<
                StepOutput<
                    S5,
                    StepOutput<
                        S4,
                        ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>,
                    >,
                >,
            >>::Future<'a>,
            <S7 as AsyncStep<
                StepOutput<
                    S6,
                    StepOutput<
                        S5,
                        StepOutput<
                            S4,
                            ChainOutput<AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>, Input>,
                        >,
                    >,
                >,
            >>::Future<'a>,
        >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenQuadFuture::new(self, input)
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
    AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>: AsyncChain<Input>,
    S5: AsyncStep<
        ChainOutput<AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>, Input>,
    >,
    S6: AsyncStep<
        StepOutput<
            S5,
            ChainOutput<AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>, Input>,
        >,
    >,
    S7: AsyncStep<
        StepOutput<
            S6,
            StepOutput<
                S5,
                ChainOutput<AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>, Input>,
            >,
        >,
    >,
    S8: AsyncStep<
        StepOutput<
            S7,
            StepOutput<
                S6,
                StepOutput<
                    S5,
                    ChainOutput<
                        AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>,
                        Input,
                    >,
                >,
            >,
        >,
    >,
{
    type Output = StepOutput<
        S8,
        StepOutput<
            S7,
            StepOutput<
                S6,
                StepOutput<
                    S5,
                    ChainOutput<
                        AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>,
                        Input,
                    >,
                >,
            >,
        >,
    >;
    type Future<'a>
        =
        ThenQuadFuture<
            'a,
            Self,
            Input,
            <AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>> as AsyncChain<
                Input,
            >>::Future<'a>,
            <S5 as AsyncStep<
                ChainOutput<AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>, Input>,
            >>::Future<'a>,
            <S6 as AsyncStep<
                StepOutput<
                    S5,
                    ChainOutput<
                        AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>,
                        Input,
                    >,
                >,
            >>::Future<'a>,
            <S7 as AsyncStep<
                StepOutput<
                    S6,
                    StepOutput<
                        S5,
                        ChainOutput<
                            AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>,
                            Input,
                        >,
                    >,
                >,
            >>::Future<'a>,
            <S8 as AsyncStep<
                StepOutput<
                    S7,
                    StepOutput<
                        S6,
                        StepOutput<
                            S5,
                            ChainOutput<
                                AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, End>>>>,
                                Input,
                            >,
                        >,
                    >,
                >,
            >>::Future<'a>,
        >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenQuadFuture::new(self, input)
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
    S1: AsyncStep<ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>,
    S2: AsyncStep<StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>,
    S3: AsyncStep<
        StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>,
    >,
    S4: AsyncStep<
        StepOutput<
            S3,
            StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>,
        >,
    >,
    S5: AsyncStep<
        StepOutput<
            S4,
            StepOutput<
                S3,
                StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>,
            >,
        >,
    >,
    S6: AsyncStep<
        StepOutput<
            S5,
            StepOutput<
                S4,
                StepOutput<
                    S3,
                    StepOutput<
                        S2,
                        StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>,
                    >,
                >,
            >,
        >,
    >,
    S7: AsyncStep<
        StepOutput<
            S6,
            StepOutput<
                S5,
                StepOutput<
                    S4,
                    StepOutput<
                        S3,
                        StepOutput<
                            S2,
                            StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>,
                        >,
                    >,
                >,
            >,
        >,
    >,
    S8: AsyncStep<
        StepOutput<
            S7,
            StepOutput<
                S6,
                StepOutput<
                    S5,
                    StepOutput<
                        S4,
                        StepOutput<
                            S3,
                            StepOutput<
                                S2,
                                StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>,
                            >,
                        >,
                    >,
                >,
            >,
        >,
    >,
{
    type Output = StepOutput<
        S8,
        StepOutput<
            S7,
            StepOutput<
                S6,
                StepOutput<
                    S5,
                    StepOutput<
                        S4,
                        StepOutput<
                            S3,
                            StepOutput<
                                S2,
                                StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>,
                            >,
                        >,
                    >,
                >,
            >,
        >,
    >;
    type Future<'a>
        = ThenOctFuture<'a, Self, Input, <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Future<'a>, <S1 as AsyncStep<ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>::Future<'a>, <S2 as AsyncStep<StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>::Future<'a>, <S3 as AsyncStep<StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>>::Future<'a>, <S4 as AsyncStep<StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>>>::Future<'a>, <S5 as AsyncStep<StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>>>>::Future<'a>, <S6 as AsyncStep<StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>>>>>::Future<'a>, <S7 as AsyncStep<StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>>>>>>::Future<'a>, <S8 as AsyncStep<StepOutput<S7, StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<AsyncPipe<TailHead, TailTail>, Input>>>>>>>>>>::Future<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        ThenOctFuture::new(self, input)
    }
}
