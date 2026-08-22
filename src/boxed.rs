use alloc::boxed::Box;

use crate::{Chain, Pipe, TryChain, TryPipe};

/// An owned, fully type-erased synchronous pipeline.
///
/// Available with the `alloc` feature. The pipeline is named by its input and
/// output types only, so it can be stored in fields, collected, and extended
/// at runtime. Construction allocates once; each `run` costs one indirect
/// call, while the stages captured inside remain statically dispatched.
///
/// ```
/// use skid_pipe::{BoxedPipe, Pipe};
///
/// let mut pipeline = BoxedPipe::new(Pipe::new(|value: u8| value + 1));
/// assert_eq!(pipeline.run(4), 5);
/// ```
///
/// [`BoxedPipe::then`] enables composition whose shape is decided at runtime:
///
/// ```
/// use skid_pipe::{BoxedPipe, Pipe};
///
/// let offsets = vec![1, 2, 3];
/// let mut pipeline = BoxedPipe::new(Pipe::new(|value: i32| value));
///
/// for offset in offsets {
///     pipeline = pipeline.then(move |value| value + offset);
/// }
///
/// assert_eq!(pipeline.run(10), 16);
/// ```
pub struct BoxedPipe<Input, Output> {
    inner: Box<dyn Chain<Input, Output = Output>>,
}

impl<Input, Output> BoxedPipe<Input, Output> {
    /// Boxes a complete pipeline behind its input and output types.
    pub fn new<Pipeline>(pipeline: Pipeline) -> Self
    where
        Pipeline: Chain<Input, Output = Output> + 'static,
    {
        Self {
            inner: Box::new(pipeline),
        }
    }

    /// Appends the next step, boxing the extended pipeline.
    ///
    /// Unlike [`Pipe::then`], this reallocates per call because the combined
    /// pipeline is erased again; in exchange the number of steps can be
    /// decided at runtime.
    pub fn then<Next, NextOutput>(mut self, mut next: Next) -> BoxedPipe<Input, NextOutput>
    where
        Input: 'static,
        Output: 'static,
        Next: FnMut(Output) -> NextOutput + 'static,
    {
        BoxedPipe::new(Pipe::new(move |input: Input| next(self.inner.run(input))))
    }

    /// Runs the pipeline for one input value.
    pub fn run(&mut self, input: Input) -> Output {
        self.inner.run(input)
    }
}

impl<Input, Output> Chain<Input> for BoxedPipe<Input, Output> {
    type Output = Output;

    fn run(&mut self, input: Input) -> Self::Output {
        self.inner.run(input)
    }
}

/// An owned, fully type-erased fallible pipeline.
///
/// Available with the `alloc` feature. The type parameters mirror `Result`:
/// input, then success output, then error. The cost model matches
/// [`BoxedPipe`]: one allocation to construct, one indirect call per `run`.
///
/// ```
/// use skid_pipe::{BoxedTryPipe, TryPipe};
///
/// let mut pipeline = BoxedTryPipe::new(TryPipe::new(|value: u8| {
///     if value == 0 { Err("empty") } else { Ok(u16::from(value)) }
/// }))
/// .try_then(|value| Ok(value > 10));
///
/// assert_eq!(pipeline.run(12), Ok(true));
/// assert_eq!(pipeline.run(0), Err("empty"));
/// ```
pub struct BoxedTryPipe<Input, Output, Error> {
    inner: Box<dyn TryChain<Input, Error, Output = Output>>,
}

impl<Input, Output, Error> BoxedTryPipe<Input, Output, Error> {
    /// Boxes a complete fallible pipeline behind its input, output, and error
    /// types.
    pub fn new<Pipeline>(pipeline: Pipeline) -> Self
    where
        Pipeline: TryChain<Input, Error, Output = Output> + 'static,
    {
        Self {
            inner: Box::new(pipeline),
        }
    }

    /// Appends the next fallible step, boxing the extended pipeline.
    pub fn try_then<Next, NextOutput>(
        mut self,
        mut next: Next,
    ) -> BoxedTryPipe<Input, NextOutput, Error>
    where
        Input: 'static,
        Output: 'static,
        Error: 'static,
        Next: FnMut(Output) -> Result<NextOutput, Error> + 'static,
    {
        BoxedTryPipe::new(TryPipe::new(move |input: Input| {
            let intermediate = self.inner.run(input)?;
            next(intermediate)
        }))
    }

    /// Runs this pipeline until it succeeds or a stage returns an error.
    pub fn run(&mut self, input: Input) -> Result<Output, Error> {
        self.inner.run(input)
    }
}

impl<Input, Output, Error> TryChain<Input, Error> for BoxedTryPipe<Input, Output, Error> {
    type Output = Output;

    fn run(&mut self, input: Input) -> Result<Self::Output, Error> {
        self.inner.run(input)
    }
}
