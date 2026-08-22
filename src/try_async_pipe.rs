use core::future::Future;

use crate::{
    End, FirstStageFuture, TryStart, TryThenFuture, TryThenOctFuture, TryThenPairFuture,
    TryThenQuadFuture,
};

type TryStepOutput<Step, Input, Error> = <Step as TryAsyncStep<Input, Error>>::Output;
type TryChainOutput<Chain, Input, Error> = <Chain as TryAsyncChain<Input, Error>>::Output;

/// A reusable, statically typed asynchronous pipeline of fallible functions.
///
/// Every stage returns a [`Future`] whose output is
/// `Result<Output, Error>`. A pipeline stops at the first error and never
/// calls subsequent stages. The caller selects the executor and error type;
/// the pipeline itself allocates nothing and performs no dynamic dispatch.
pub struct TryAsyncPipe<Head, Tail = End> {
    // `crate::future` projects these to reach one stage at a time from a
    // single stored pipeline pointer.
    pub(crate) head: Head,
    pub(crate) tail: Tail,
}

impl<Head> TryAsyncPipe<Head> {
    /// Starts a fallible asynchronous pipeline with its first stage.
    #[inline(always)]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> TryAsyncPipe<Head, Tail> {
    /// Appends the next fallible asynchronous stage.
    #[inline(always)]
    pub const fn try_then<Next>(self, next: Next) -> TryAsyncPipe<Next, Self> {
        TryAsyncPipe {
            head: next,
            tail: self,
        }
    }

    /// Returns a future that runs stages from left to right until one fails.
    ///
    /// The caller selects where and how the future is polled. Creating the
    /// future is lazy: no stage runs until the future is first polled.
    /// Construction is still not free, because it writes one pipeline pointer
    /// and one state tag per group of eight stages, so an unpolled run, or one
    /// that short-circuits on the first stage's error, costs `O(stages / 8)`
    /// stores. The mutable receiver permits `FnMut` stages to retain state
    /// between completed runs. The returned future holds the pipeline's mutable borrow until it
    /// completes or is dropped, so another run cannot start while it is live.
    /// Dropping a pending future releases that borrow but does not roll back
    /// state changes made by stages that were already polled.
    /// To satisfy a `tokio::spawn`-style `Send + 'static` boundary, move the
    /// pipeline into an `async move` task and call `run` inside that task.
    ///
    /// ```compile_fail
    /// use skid_pipe::TryAsyncPipe;
    ///
    /// async fn step(value: u8) -> Result<u8, ()> {
    ///     Ok(value + 1)
    /// }
    ///
    /// # async fn overlapping_runs() {
    /// let mut pipeline = TryAsyncPipe::new(step);
    /// let first = pipeline.run(1);
    /// let second = pipeline.run(2); // `first` still borrows `pipeline`
    /// let _ = (first.await, second.await);
    /// # }
    /// ```
    #[inline(always)]
    pub fn run<Input, Error>(
        &mut self,
        input: Input,
    ) -> <Self as TryAsyncChain<Input, Error>>::Future<'_>
    where
        Self: TryAsyncChain<Input, Error>,
    {
        TryAsyncChain::run(self, input)
    }
}

/// A callable asynchronous `Result`-returning pipeline stage.
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
/// concrete pipeline type.
///
/// `run` returns a concrete future and does not require allocation or dynamic
/// dispatch. External implementations must run stages from left to right and
/// stop after the first error.
pub trait TryAsyncChain<Input, Error>: Sized {
    /// The success value emitted when the completed pipeline resolves.
    type Output;

    /// The concrete future created by this chain.
    type Future<'a>: Future<Output = Result<Self::Output, Error>>
    where
        Self: 'a;

    /// Creates the future that runs this chain until success or failure.
    fn run(&mut self, input: Input) -> Self::Future<'_>;
}

impl<Head, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<Head, End>
where
    Head: TryAsyncStep<Input, Error>,
{
    type Output = Head::Output;
    type Future<'a>
        = FirstStageFuture<'a, TryStart<Error>, Head, Input, Head::Future<'a>>
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        FirstStageFuture::new(&mut self.head, input)
    }
}

impl<S1, S2, Input, Error> TryAsyncChain<Input, Error> for TryAsyncPipe<S2, TryAsyncPipe<S1, End>>
where
    TryAsyncPipe<S1, End>: TryAsyncChain<Input, Error>,
    S2: TryAsyncStep<TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
{
    type Output = TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>;
    type Future<'a>
        = TryThenFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<S1, End> as TryAsyncChain<Input, Error>>::Future<'a>,
        <S2 as TryAsyncStep<TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>>::Future<
            'a,
        >,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenFuture::new(self, input)
    }
}

impl<S1, S2, S3, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>
where
    TryAsyncPipe<S1, End>: TryAsyncChain<Input, Error>,
    S2: TryAsyncStep<TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
    S3: TryAsyncStep<
            TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
            Error,
        >,
{
    type Output = TryStepOutput<
        S3,
        TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
        Error,
    >;
    type Future<'a>
        = TryThenPairFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<S1, End> as TryAsyncChain<Input, Error>>::Future<'a>,
        <S2 as TryAsyncStep<TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>>::Future<
            'a,
        >,
        <S3 as TryAsyncStep<
            TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
            Error,
        >>::Future<'a>,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenPairFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>
where
    TryAsyncPipe<S2, TryAsyncPipe<S1, End>>: TryAsyncChain<Input, Error>,
    S3: TryAsyncStep<TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>, Error>,
    S4: TryAsyncStep<
            TryStepOutput<
                S3,
                TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                Error,
            >,
            Error,
        >,
{
    type Output = TryStepOutput<
        S4,
        TryStepOutput<
            S3,
            TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
            Error,
        >,
        Error,
    >;
    type Future<'a>
        = TryThenPairFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<S2, TryAsyncPipe<S1, End>> as TryAsyncChain<Input, Error>>::Future<'a>,
        <S3 as TryAsyncStep<
            TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
            Error,
        >>::Future<'a>,
        <S4 as TryAsyncStep<
            TryStepOutput<
                S3,
                TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                Error,
            >,
            Error,
        >>::Future<'a>,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenPairFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<
        S5,
        TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
    >
where
    TryAsyncPipe<S1, End>: TryAsyncChain<Input, Error>,
    S2: TryAsyncStep<TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
    S3: TryAsyncStep<
            TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
            Error,
        >,
    S4: TryAsyncStep<
            TryStepOutput<
                S3,
                TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
                Error,
            >,
            Error,
        >,
    S5: TryAsyncStep<
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
{
    type Output = TryStepOutput<
        S5,
        TryStepOutput<
            S4,
            TryStepOutput<
                S3,
                TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
                Error,
            >,
            Error,
        >,
        Error,
    >;
    type Future<'a>
        = TryThenQuadFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<S1, End> as TryAsyncChain<Input, Error>>::Future<'a>,
        <S2 as TryAsyncStep<TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>>::Future<
            'a,
        >,
        <S3 as TryAsyncStep<
            TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
            Error,
        >>::Future<'a>,
        <S4 as TryAsyncStep<
            TryStepOutput<
                S3,
                TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S5 as TryAsyncStep<
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryStepOutput<S2, TryChainOutput<TryAsyncPipe<S1, End>, Input, Error>, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenQuadFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, S6, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<
        S6,
        TryAsyncPipe<
            S5,
            TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
        >,
    >
where
    TryAsyncPipe<S2, TryAsyncPipe<S1, End>>: TryAsyncChain<Input, Error>,
    S3: TryAsyncStep<TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>, Error>,
    S4: TryAsyncStep<
            TryStepOutput<
                S3,
                TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                Error,
            >,
            Error,
        >,
    S5: TryAsyncStep<
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S6: TryAsyncStep<
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryStepOutput<
                        S3,
                        TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
{
    type Output = TryStepOutput<
        S6,
        TryStepOutput<
            S5,
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
        Error,
    >;
    type Future<'a>
        = TryThenQuadFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<S2, TryAsyncPipe<S1, End>> as TryAsyncChain<Input, Error>>::Future<'a>,
        <S3 as TryAsyncStep<
            TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
            Error,
        >>::Future<'a>,
        <S4 as TryAsyncStep<
            TryStepOutput<
                S3,
                TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S5 as TryAsyncStep<
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S6 as TryAsyncStep<
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryStepOutput<
                        S3,
                        TryChainOutput<TryAsyncPipe<S2, TryAsyncPipe<S1, End>>, Input, Error>,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenQuadFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<
        S7,
        TryAsyncPipe<
            S6,
            TryAsyncPipe<
                S5,
                TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
            >,
        >,
    >
where
    TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>: TryAsyncChain<Input, Error>,
    S4: TryAsyncStep<
            TryChainOutput<TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>, Input, Error>,
            Error,
        >,
    S5: TryAsyncStep<
            TryStepOutput<
                S4,
                TryChainOutput<
                    TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                    Input,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S6: TryAsyncStep<
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryChainOutput<
                        TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                        Input,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S7: TryAsyncStep<
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryStepOutput<
                        S4,
                        TryChainOutput<
                            TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                            Input,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
{
    type Output = TryStepOutput<
        S7,
        TryStepOutput<
            S6,
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryChainOutput<
                        TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                        Input,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
        Error,
    >;
    type Future<'a>
        = TryThenQuadFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>> as TryAsyncChain<
            Input,
            Error,
        >>::Future<'a>,
        <S4 as TryAsyncStep<
            TryChainOutput<TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>, Input, Error>,
            Error,
        >>::Future<'a>,
        <S5 as TryAsyncStep<
            TryStepOutput<
                S4,
                TryChainOutput<
                    TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                    Input,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S6 as TryAsyncStep<
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryChainOutput<
                        TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                        Input,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S7 as TryAsyncStep<
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryStepOutput<
                        S4,
                        TryChainOutput<
                            TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                            Input,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenQuadFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<
        S8,
        TryAsyncPipe<
            S7,
            TryAsyncPipe<
                S6,
                TryAsyncPipe<
                    S5,
                    TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
                >,
            >,
        >,
    >
where
    TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>:
        TryAsyncChain<Input, Error>,
    S5: TryAsyncStep<
            TryChainOutput<
                TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
                Input,
                Error,
            >,
            Error,
        >,
    S6: TryAsyncStep<
            TryStepOutput<
                S5,
                TryChainOutput<
                    TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
                    Input,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S7: TryAsyncStep<
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryChainOutput<
                        TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
                        Input,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S8: TryAsyncStep<
            TryStepOutput<
                S7,
                TryStepOutput<
                    S6,
                    TryStepOutput<
                        S5,
                        TryChainOutput<
                            TryAsyncPipe<
                                S4,
                                TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>,
                            >,
                            Input,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
{
    type Output = TryStepOutput<
        S8,
        TryStepOutput<
            S7,
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryChainOutput<
                        TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>,
                        Input,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
        Error,
    >;
    type Future<'a>
        = TryThenQuadFuture<'a, Self, Input, <TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>> as TryAsyncChain<Input, Error>>::Future<'a>, <S5 as TryAsyncStep<TryChainOutput<TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>, Input, Error>, Error>>::Future<'a>, <S6 as TryAsyncStep<TryStepOutput<S5, TryChainOutput<TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>, Input, Error>, Error>, Error>>::Future<'a>, <S7 as TryAsyncStep<TryStepOutput<S6, TryStepOutput<S5, TryChainOutput<TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>, Input, Error>, Error>, Error>, Error>>::Future<'a>, <S8 as TryAsyncStep<TryStepOutput<S7, TryStepOutput<S6, TryStepOutput<S5, TryChainOutput<TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, End>>>>, Input, Error>, Error>, Error>, Error>, Error>>::Future<'a>, Error>
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenQuadFuture::new(self, input)
    }
}

impl<S1, S2, S3, S4, S5, S6, S7, S8, TailHead, TailTail, Input, Error> TryAsyncChain<Input, Error>
    for TryAsyncPipe<
        S8,
        TryAsyncPipe<
            S7,
            TryAsyncPipe<
                S6,
                TryAsyncPipe<
                    S5,
                    TryAsyncPipe<
                        S4,
                        TryAsyncPipe<
                            S3,
                            TryAsyncPipe<S2, TryAsyncPipe<S1, TryAsyncPipe<TailHead, TailTail>>>,
                        >,
                    >,
                >,
            >,
        >,
    >
where
    TryAsyncPipe<TailHead, TailTail>: TryAsyncChain<Input, Error>,
    S1: TryAsyncStep<TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>, Error>,
    S2: TryAsyncStep<
            TryStepOutput<
                S1,
                TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                Error,
            >,
            Error,
        >,
    S3: TryAsyncStep<
            TryStepOutput<
                S2,
                TryStepOutput<
                    S1,
                    TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S4: TryAsyncStep<
            TryStepOutput<
                S3,
                TryStepOutput<
                    S2,
                    TryStepOutput<
                        S1,
                        TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S5: TryAsyncStep<
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryStepOutput<
                        S2,
                        TryStepOutput<
                            S1,
                            TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S6: TryAsyncStep<
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryStepOutput<
                        S3,
                        TryStepOutput<
                            S2,
                            TryStepOutput<
                                S1,
                                TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S7: TryAsyncStep<
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryStepOutput<
                        S4,
                        TryStepOutput<
                            S3,
                            TryStepOutput<
                                S2,
                                TryStepOutput<
                                    S1,
                                    TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                                    Error,
                                >,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
    S8: TryAsyncStep<
            TryStepOutput<
                S7,
                TryStepOutput<
                    S6,
                    TryStepOutput<
                        S5,
                        TryStepOutput<
                            S4,
                            TryStepOutput<
                                S3,
                                TryStepOutput<
                                    S2,
                                    TryStepOutput<
                                        S1,
                                        TryChainOutput<
                                            TryAsyncPipe<TailHead, TailTail>,
                                            Input,
                                            Error,
                                        >,
                                        Error,
                                    >,
                                    Error,
                                >,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
{
    type Output = TryStepOutput<
        S8,
        TryStepOutput<
            S7,
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryStepOutput<
                        S4,
                        TryStepOutput<
                            S3,
                            TryStepOutput<
                                S2,
                                TryStepOutput<
                                    S1,
                                    TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                                    Error,
                                >,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >,
        Error,
    >;
    type Future<'a>
        = TryThenOctFuture<
        'a,
        Self,
        Input,
        <TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Future<'a>,
        <S1 as TryAsyncStep<
            TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
            Error,
        >>::Future<'a>,
        <S2 as TryAsyncStep<
            TryStepOutput<
                S1,
                TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S3 as TryAsyncStep<
            TryStepOutput<
                S2,
                TryStepOutput<
                    S1,
                    TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S4 as TryAsyncStep<
            TryStepOutput<
                S3,
                TryStepOutput<
                    S2,
                    TryStepOutput<
                        S1,
                        TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S5 as TryAsyncStep<
            TryStepOutput<
                S4,
                TryStepOutput<
                    S3,
                    TryStepOutput<
                        S2,
                        TryStepOutput<
                            S1,
                            TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S6 as TryAsyncStep<
            TryStepOutput<
                S5,
                TryStepOutput<
                    S4,
                    TryStepOutput<
                        S3,
                        TryStepOutput<
                            S2,
                            TryStepOutput<
                                S1,
                                TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S7 as TryAsyncStep<
            TryStepOutput<
                S6,
                TryStepOutput<
                    S5,
                    TryStepOutput<
                        S4,
                        TryStepOutput<
                            S3,
                            TryStepOutput<
                                S2,
                                TryStepOutput<
                                    S1,
                                    TryChainOutput<TryAsyncPipe<TailHead, TailTail>, Input, Error>,
                                    Error,
                                >,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        <S8 as TryAsyncStep<
            TryStepOutput<
                S7,
                TryStepOutput<
                    S6,
                    TryStepOutput<
                        S5,
                        TryStepOutput<
                            S4,
                            TryStepOutput<
                                S3,
                                TryStepOutput<
                                    S2,
                                    TryStepOutput<
                                        S1,
                                        TryChainOutput<
                                            TryAsyncPipe<TailHead, TailTail>,
                                            Input,
                                            Error,
                                        >,
                                        Error,
                                    >,
                                    Error,
                                >,
                                Error,
                            >,
                            Error,
                        >,
                        Error,
                    >,
                    Error,
                >,
                Error,
            >,
            Error,
        >>::Future<'a>,
        Error,
    >
    where
        Self: 'a;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Future<'_> {
        TryThenOctFuture::new(self, input)
    }
}
