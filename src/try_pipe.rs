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

/// A complete fallible pipeline, runnable for one input value.
///
/// [`TryPipe`] and [`End`] implement this trait; it is the recursive engine
/// behind [`TryPipe::run`]. Like [`Chain`](crate::Chain), it is public so a
/// fallible pipeline can be handled without naming its recursive concrete
/// type: return `impl TryChain<Input, Error, Output = O>` from a builder
/// function, or borrow the pipeline as [`DynTryChain`].
///
/// External implementations are allowed. An implementation must run its
/// stages from left to right and stop at the first error.
pub trait TryChain<Input, Error> {
    /// The success value emitted by the completed pipeline.
    type Output;

    /// Runs this chain from left to right.
    fn run(&mut self, input: Input) -> Result<Self::Output, Error>;
}

/// A mutable borrow of a type-erased fallible pipeline.
///
/// The parameters mirror `Result`: input, then success output, then error.
/// Like [`DynChain`](crate::DynChain) this adds no allocation and costs one
/// indirect call per `run`.
///
/// ```
/// use skid_pipe::{DynTryChain, TryPipe};
///
/// let mut pipeline = TryPipe::new(|value: u8| {
///     if value == 0 { Err("empty") } else { Ok(u16::from(value)) }
/// });
///
/// let erased: DynTryChain<'_, u8, u16, &'static str> = &mut pipeline;
/// assert_eq!(erased.run(7), Ok(7));
/// ```
pub type DynTryChain<'a, Input, Output, Error> =
    &'a mut dyn TryChain<Input, Error, Output = Output>;

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
