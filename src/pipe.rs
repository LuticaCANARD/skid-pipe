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

/// A statically typed conditional branch between two pipelines.
///
/// The predicate observes an input by reference. Exactly one branch then
/// consumes the input, and both branches must emit the same output type.
/// [`Pipe::then_branch`] constructs this stage in the usual case.
pub struct Branch<Predicate, WhenTrue, WhenFalse> {
    pub(crate) predicate: Predicate,
    pub(crate) when_true: WhenTrue,
    pub(crate) when_false: WhenFalse,
}

impl<Head> Pipe<Head> {
    /// Starts a pipeline with its first step.
    #[inline]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> Pipe<Head, Tail> {
    /// Appends the next step to this pipeline.
    #[inline]
    pub const fn then<Next>(self, next: Next) -> Pipe<Next, Self> {
        Pipe {
            head: next,
            tail: self,
        }
    }

    /// Appends two alternative pipelines selected by a predicate.
    ///
    /// The predicate borrows the value produced by the preceding pipeline. The
    /// selected branch consumes that value; the other branch is not run. Both
    /// branches must produce the same output type when this pipeline is run.
    #[inline]
    pub const fn then_branch<Input, Predicate, WhenTrue, WhenFalse>(
        self,
        predicate: Predicate,
        when_true: WhenTrue,
        when_false: WhenFalse,
    ) -> Pipe<Branch<Predicate, WhenTrue, WhenFalse>, Self>
    where
        Predicate: FnMut(&Input) -> bool,
        WhenTrue: Chain<Input>,
        WhenFalse: Chain<Input, Output = <WhenTrue as Chain<Input>>::Output>,
    {
        Pipe {
            head: Branch {
                predicate,
                when_true,
                when_false,
            },
            tail: self,
        }
    }

    /// Runs the pipeline for one input value.
    ///
    /// The mutable receiver allows any stage to be an ordinary `FnMut` closure
    /// and retain state between runs. Pure functions and `Fn` closures work as
    /// `FnMut` stages too.
    #[inline]
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
pub trait Step<Input> {
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

    #[inline]
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
/// - Borrow any pipeline as [`DynChain`] to store or pass it as a single
///   nameable type without allocation.
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
pub trait Chain<Input> {
    /// The value emitted by the completed pipeline.
    type Output;

    /// Runs this chain from left to right.
    fn run(&mut self, input: Input) -> Self::Output;
}

/// A mutable borrow of a type-erased synchronous pipeline.
///
/// [`Chain`] is dyn-compatible, so any pipeline can be borrowed as a trait
/// object. This names the pipeline by its input and output types only, adds
/// no allocation, and costs one indirect call per `run` — the stages inside
/// remain statically dispatched.
///
/// ```
/// use skid_pipe::{DynChain, Pipe};
///
/// let mut double = Pipe::new(|value: i32| value * 2);
/// let mut negate = Pipe::new(|value: i32| -value);
///
/// let selected: DynChain<'_, i32, i32> = if true { &mut double } else { &mut negate };
/// assert_eq!(selected.run(4), 8);
/// ```
pub type DynChain<'a, Input, Output> = &'a mut dyn Chain<Input, Output = Output>;

impl<Input> Chain<Input> for End {
    type Output = Input;

    #[inline]
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

    #[inline]
    fn run(&mut self, input: Input) -> Self::Output {
        let intermediate = Chain::run(&mut self.tail, input);
        Step::call(&mut self.head, intermediate)
    }
}

impl<Predicate, WhenTrue, WhenFalse, Input> Step<Input> for Branch<Predicate, WhenTrue, WhenFalse>
where
    Predicate: FnMut(&Input) -> bool,
    WhenTrue: Chain<Input>,
    WhenFalse: Chain<Input, Output = <WhenTrue as Chain<Input>>::Output>,
{
    type Output = <WhenTrue as Chain<Input>>::Output;

    #[inline]
    fn call(&mut self, input: Input) -> Self::Output {
        if (self.predicate)(&input) {
            Chain::run(&mut self.when_true, input)
        } else {
            Chain::run(&mut self.when_false, input)
        }
    }
}
