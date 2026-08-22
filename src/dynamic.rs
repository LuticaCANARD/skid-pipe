use alloc::{boxed::Box, vec::Vec};

use crate::TryChain;

/// One dynamically dispatched step in a [`RuntimePipe`].
///
/// Every step accepts and returns the same caller-defined carrier type. An
/// enum is a natural carrier when the logical stages use different domain
/// types. Returning `Result` is mandatory because a runtime-selected sequence
/// cannot prove that adjacent variants are compatible at compile time.
pub trait RuntimeStep<Value, Error> {
    /// Applies this step to one carrier value.
    fn call(&mut self, input: Value) -> Result<Value, Error>;
}

impl<Value, Error, F> RuntimeStep<Value, Error> for F
where
    F: FnMut(Value) -> Result<Value, Error>,
{
    fn call(&mut self, input: Value) -> Result<Value, Error> {
        self(input)
    }
}

/// An owned pipeline whose steps and order can be selected at runtime.
///
/// Available with the `dynamic` feature, which implies `alloc`. Unlike
/// [`Pipe`](crate::Pipe), this pipeline deliberately trades static adjacency
/// checks for runtime configurability. Each step is boxed, each call is
/// dynamically dispatched, and the caller supplies a common carrier and error
/// type.
///
/// This type is intended for registered steps selected by configuration. It
/// does not load native plugins or erase arbitrary values with `Any`.
///
/// ```
/// use skid_pipe::RuntimePipe;
///
/// #[derive(Debug, PartialEq)]
/// enum Value {
///     Raw(u8),
///     Decoded(u16),
/// }
///
/// #[derive(Debug, PartialEq)]
/// enum Error {
///     UnexpectedValue,
/// }
///
/// let mut pipeline = RuntimePipe::<Value, Error>::new();
/// pipeline.push(|value| match value {
///     Value::Raw(raw) => Ok(Value::Decoded(u16::from(raw))),
///     _ => Err(Error::UnexpectedValue),
/// });
///
/// assert_eq!(pipeline.run(Value::Raw(7)), Ok(Value::Decoded(7)));
/// ```
pub struct RuntimePipe<Value, Error> {
    steps: Vec<Box<dyn RuntimeStep<Value, Error>>>,
}

impl<Value, Error> RuntimePipe<Value, Error> {
    /// Creates an empty runtime pipeline.
    #[must_use]
    pub const fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Creates an empty runtime pipeline with space for at least `capacity`
    /// steps.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            steps: Vec::with_capacity(capacity),
        }
    }

    /// Appends one concrete step and returns this pipeline for chained setup.
    pub fn push<Step>(&mut self, step: Step) -> &mut Self
    where
        Step: RuntimeStep<Value, Error> + 'static,
    {
        self.steps.push(Box::new(step));
        self
    }

    /// Appends an already-erased step, such as one returned by a registry.
    pub fn push_boxed(&mut self, step: Box<dyn RuntimeStep<Value, Error>>) -> &mut Self {
        self.steps.push(step);
        self
    }

    /// Returns the number of configured steps.
    #[must_use]
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Returns whether this pipeline contains no steps.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Runs all configured steps from left to right.
    ///
    /// The first error is returned immediately and later steps are not called.
    pub fn run(&mut self, mut value: Value) -> Result<Value, Error> {
        for step in &mut self.steps {
            value = step.call(value)?;
        }

        Ok(value)
    }
}

impl<Value, Error> Default for RuntimePipe<Value, Error> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value, Error> TryChain<Value, Error> for RuntimePipe<Value, Error> {
    type Output = Value;

    fn run(&mut self, input: Value) -> Result<Self::Output, Error> {
        RuntimePipe::run(self, input)
    }
}
