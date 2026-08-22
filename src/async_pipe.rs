use core::future::Future;

use crate::{Branch, End};

/// A reusable, statically typed asynchronous function pipeline.
///
/// Each stage returns a [`Future`]. [`AsyncPipe::run`] returns the composed
/// future without allocating, polling it, or selecting an executor.
pub struct AsyncPipe<Head, Tail = End> {
    head: Head,
    tail: Tail,
}

impl<Head> AsyncPipe<Head> {
    /// Starts an asynchronous pipeline with its first step.
    #[inline]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> AsyncPipe<Head, Tail> {
    /// Appends the next asynchronous step to this pipeline.
    #[inline]
    pub const fn then<Next>(self, next: Next) -> AsyncPipe<Next, Self> {
        AsyncPipe {
            head: next,
            tail: self,
        }
    }

    /// Appends two alternative asynchronous pipelines selected by a predicate.
    ///
    /// The predicate is evaluated synchronously from a borrow of the preceding
    /// value. Only the selected branch is polled, and both branches must
    /// resolve to the same output type when this pipeline is run.
    #[inline]
    pub const fn then_branch<Input, Predicate, WhenTrue, WhenFalse>(
        self,
        predicate: Predicate,
        when_true: WhenTrue,
        when_false: WhenFalse,
    ) -> AsyncPipe<Branch<Predicate, WhenTrue, WhenFalse>, Self>
    where
        Predicate: FnMut(&Input) -> bool,
        WhenTrue: AsyncChain<Input>,
        WhenFalse: AsyncChain<Input, Output = <WhenTrue as AsyncChain<Input>>::Output>,
    {
        AsyncPipe {
            head: Branch {
                predicate,
                when_true,
                when_false,
            },
            tail: self,
        }
    }

    /// Returns a future that runs every stage from left to right.
    ///
    /// The caller selects where and how that future is polled. The mutable
    /// receiver permits `FnMut` stages to retain state between completed runs.
    #[inline]
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
pub trait AsyncStep<Input> {
    /// The value emitted when the stage future resolves.
    type Output;

    /// Creates the stage future for one input value.
    fn call(&mut self, input: Input) -> impl Future<Output = Self::Output>;
}

impl<Input, Output, F, Fut> AsyncStep<Input> for F
where
    F: FnMut(Input) -> Fut,
    Fut: Future<Output = Output>,
{
    type Output = Output;

    #[inline]
    fn call(&mut self, input: Input) -> impl Future<Output = Self::Output> {
        self(input)
    }
}

/// A complete asynchronous pipeline, runnable for one input value.
///
/// [`AsyncPipe`] and [`End`](crate::End) implement this trait; it is the
/// recursive engine behind [`AsyncPipe::run`]. It is public so builder
/// functions can return `impl AsyncChain<Input, Output = O>` and hide the
/// recursive concrete pipeline type at zero cost.
///
/// Unlike [`Chain`](crate::Chain), this trait is **not dyn-compatible**:
/// `run` returns `impl Future`, so there is no `&mut dyn AsyncChain`
/// counterpart to [`DynChain`](crate::DynChain). Erasing an asynchronous
/// pipeline requires boxing each returned future, which this crate does not
/// do; use `impl AsyncChain` at API boundaries instead.
pub trait AsyncChain<Input> {
    /// The value emitted when this chain's future resolves.
    type Output;

    /// Creates the future that runs this chain.
    fn run(&mut self, input: Input) -> impl Future<Output = Self::Output>;
}

impl<Input> AsyncChain<Input> for End {
    type Output = Input;

    #[inline]
    fn run(&mut self, input: Input) -> impl Future<Output = Self::Output> {
        core::future::ready(input)
    }
}

impl<Head, Tail, Input> AsyncChain<Input> for AsyncPipe<Head, Tail>
where
    Tail: AsyncChain<Input>,
    Head: AsyncStep<Tail::Output>,
{
    type Output = Head::Output;

    #[inline]
    async fn run(&mut self, input: Input) -> Self::Output {
        let intermediate = AsyncChain::run(&mut self.tail, input).await;
        AsyncStep::call(&mut self.head, intermediate).await
    }
}

impl<Predicate, WhenTrue, WhenFalse, Input> AsyncStep<Input>
    for Branch<Predicate, WhenTrue, WhenFalse>
where
    Predicate: FnMut(&Input) -> bool,
    WhenTrue: AsyncChain<Input>,
    WhenFalse: AsyncChain<Input, Output = <WhenTrue as AsyncChain<Input>>::Output>,
{
    type Output = <WhenTrue as AsyncChain<Input>>::Output;

    #[inline]
    fn call(&mut self, input: Input) -> impl Future<Output = Self::Output> {
        let select_true = (self.predicate)(&input);

        async move {
            if select_true {
                AsyncChain::run(&mut self.when_true, input).await
            } else {
                AsyncChain::run(&mut self.when_false, input).await
            }
        }
    }
}
