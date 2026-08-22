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
#[doc(hidden)]
pub trait TryStep<Input, Error> {
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

/// Recursive implementation detail behind [`TryPipe::run`].
#[doc(hidden)]
pub trait TryChain<Input, Error> {
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

impl<Head, Tail, Input, Error> TryChain<Input, Error> for TryPipe<Head, Tail>
where
    Tail: TryChain<Input, Error>,
    Head: TryStep<Tail::Output, Error>,
{
    type Output = Head::Output;

    #[inline]
    fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        let intermediate = TryChain::run(&mut self.tail, input)?;
        TryStep::call(&mut self.head, intermediate)
    }
}
