use crate::End;

/// A reusable, statically typed pipeline of fallible functions.
///
/// Every stage returns the standard `Result<Output, Error>` type. A pipeline
/// stops at the first error and never runs subsequent stages. All stages use a
/// single caller-selected error type; callers can map domain-specific errors
/// before passing a function to [`TryPipe::try_then`].
pub struct TryPipe<Head, Tail = End> {
    head: Head,
    tail: Tail,
}

impl<Head> TryPipe<Head> {
    /// Starts a fallible pipeline with its first stage.
    #[inline]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> TryPipe<Head, Tail> {
    /// Appends the next fallible stage.
    #[inline]
    pub const fn try_then<Next>(self, next: Next) -> TryPipe<Next, Self> {
        TryPipe {
            head: next,
            tail: self,
        }
    }

    /// Runs this pipeline until it succeeds or a stage returns an error.
    ///
    /// The mutable receiver permits stateful `FnMut` stages. No error type is
    /// synthesized or converted by the pipeline itself.
    #[inline]
    pub fn run<Input, Error>(
        &mut self,
        input: Input,
    ) -> Result<<Self as TryChain<Input, Error>>::Output, Error>
    where
        Self: TryChain<Input, Error>,
    {
        TryChain::run(self, input)
    }
}

/// A callable `Result`-returning pipeline stage.
///
/// Every `FnMut(Input) -> Result<Output, Error>` implements this trait
/// automatically, so callers normally pass plain functions or closures to
/// [`TryPipe::try_then`]. Implementing `TryStep` by hand is supported for
/// named stateful stages.
pub trait TryStep<Input, Error>: Sized {
    /// The success value emitted by this stage.
    type Output;

    /// Applies this stage to one input value.
    fn call(&mut self, input: Input) -> Result<Self::Output, Error>;
}

impl<Input, Output, Error, F> TryStep<Input, Error> for F
where
    F: FnMut(Input) -> Result<Output, Error>,
{
    type Output = Output;

    #[inline]
    fn call(&mut self, input: Input) -> Result<Self::Output, Error> {
        self(input)
    }
}

/// A complete fallible pipeline, runnable for one input value.
///
/// [`TryPipe`] and [`End`] implement this trait; it is the recursive engine
/// behind [`TryPipe::run`]. Like [`Chain`](crate::Chain), it is public so a
/// fallible pipeline can be handled without naming its recursive concrete
/// type: return `impl TryChain<Input, Error, Output = O>` from a builder
/// function.
///
/// External implementations are allowed. An implementation must run its
/// stages from left to right and stop at the first error.
pub trait TryChain<Input, Error>: Sized {
    /// The success value emitted by the completed pipeline.
    type Output;

    /// Runs this chain from left to right.
    fn run(&mut self, input: Input) -> Result<Self::Output, Error>;
}

impl<Input, Error> TryChain<Input, Error> for End {
    type Output = Input;

    #[inline]
    fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        Ok(input)
    }
}

impl<Head, Input, Error> TryChain<Input, Error> for TryPipe<Head, End>
where
    Head: TryStep<Input, Error>,
{
    type Output = Head::Output;

    #[inline]
    fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        TryStep::call(&mut self.head, input)
    }
}

impl<Head, TailHead, TailTail, Input, Error> TryChain<Input, Error>
    for TryPipe<Head, TryPipe<TailHead, TailTail>>
where
    TryPipe<TailHead, TailTail>: TryChain<Input, Error>,
    Head: TryStep<<TryPipe<TailHead, TailTail> as TryChain<Input, Error>>::Output, Error>,
{
    type Output = Head::Output;

    #[inline]
    fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let intermediate = TryChain::run(&mut self.tail, input)?;
        TryStep::call(&mut self.head, intermediate)
    }
}
