/// Recursive terminator shared by synchronous and asynchronous pipelines.
#[derive(Clone, Copy, Debug, Default)]
pub struct End;

/// A reusable, statically typed synchronous function pipeline.
///
/// `Pipe::new(first).then(second).then(third)` stores the newest step at the
/// head of a recursive structure, but [`Pipe::run`] evaluates the tail first.
/// Therefore, values flow through steps in the same left-to-right order in
/// which they appear in source.
pub struct Pipe<Head, Tail = End> {
    head: Head,
    tail: Tail,
}

impl<Head> Pipe<Head> {
    /// Starts a pipeline with its first step.
    #[inline(always)]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> Pipe<Head, Tail> {
    /// Appends the next step to this pipeline.
    #[inline(always)]
    pub const fn then<Next>(self, next: Next) -> Pipe<Next, Self> {
        Pipe {
            head: next,
            tail: self,
        }
    }

    /// Runs the pipeline for one input value.
    ///
    /// The mutable receiver allows any stage to be an ordinary `FnMut` closure
    /// and retain state between runs. Pure functions and `Fn` closures work as
    /// `FnMut` stages too.
    #[inline(always)]
    pub fn run<Input>(&mut self, input: Input) -> <Self as Chain<Input>>::Output
    where
        Self: Chain<Input>,
    {
        Chain::run(self, input)
    }
}

/// A callable synchronous pipeline stage.
///
/// Every `FnMut(Input) -> Output` implements this trait automatically, so
/// callers normally pass plain functions or closures to [`Pipe::then`].
/// Implementing `Step` by hand is supported and is useful for named stateful
/// stages that cannot be expressed as a closure.
pub trait Step<Input>: Sized {
    /// The value emitted by this stage.
    type Output;

    /// Applies this stage to one input value.
    fn call(&mut self, input: Input) -> Self::Output;
}

impl<Input, Output, F> Step<Input> for F
where
    F: FnMut(Input) -> Output,
{
    type Output = Output;

    #[inline(always)]
    fn call(&mut self, input: Input) -> Self::Output {
        self(input)
    }
}

/// A complete synchronous pipeline, runnable for one input value.
///
/// [`Pipe`] and [`End`] implement this trait; it is the recursive engine
/// behind [`Pipe::run`]. It is public so pipelines can be handled without
/// naming their recursive concrete type:
///
/// - Return `impl Chain<Input, Output = O>` from a builder function to hide
///   the concrete pipeline type at zero cost.
///
/// External implementations are allowed. An implementation must run its
/// stages from left to right exactly once per `run` call.
///
/// ```
/// use skid_pipe::{Chain, Pipe};
///
/// fn build() -> impl Chain<u8, Output = u8> {
///     Pipe::new(|value: u8| value + 1).then(|value: u8| value * 2)
/// }
///
/// let mut pipeline = build();
/// assert_eq!(pipeline.run(4), 10);
/// ```
pub trait Chain<Input>: Sized {
    /// The value emitted by the completed pipeline.
    type Output;

    /// Runs this chain from left to right.
    fn run(&mut self, input: Input) -> Self::Output;
}

impl<Input> Chain<Input> for End {
    type Output = Input;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Output {
        input
    }
}

impl<Head, Tail, Input> Chain<Input> for Pipe<Head, Tail>
where
    Tail: Chain<Input>,
    Head: Step<Tail::Output>,
{
    type Output = Head::Output;

    #[inline(always)]
    fn run(&mut self, input: Input) -> Self::Output {
        let intermediate = Chain::run(&mut self.tail, input);
        Step::call(&mut self.head, intermediate)
    }
}
