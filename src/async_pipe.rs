use core::future::Future;

use crate::End;

/// A reusable, statically typed asynchronous function pipeline.
///
/// Each stage returns a [`Future`]. [`AsyncPipe::run`] returns the composed
/// future without allocating, polling it, or selecting an executor.
pub struct AsyncPipe<Head, Tail = End> {
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
    /// future is lazy: no stage runs until the future is first polled. The
    /// returned future holds the mutable pipeline borrow until it is completed
    /// or dropped, so one pipeline instance cannot have overlapping runs.
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
/// [`AsyncPipe::run`]. It is public so builder functions can return
/// `impl AsyncChain<Input, Output = O>` and hide the recursive concrete
/// pipeline type at zero cost.
/// `run` deliberately returns an `async` block rather than being an `async fn`.
/// Clippy's `manual_async_fn` asks for the shorter spelling, but on this crate's
/// 100-stage footprint example the `async fn` form measures 320 B against the
/// block form's 216 B, so the lint is allowed at each `run` instead.
pub trait AsyncChain<Input>: Sized {
    /// The value emitted when this chain's future resolves.
    type Output;

    /// Creates the future that runs this chain.
    fn run(&mut self, input: Input) -> impl Future<Output = Self::Output>;
}

// The ladder below is written by the shared accumulators in `ladder.rs`.
// Arity is this crate's main performance lever — a group's stages share one
// `async` block and rustc overlaps their futures into a single slot — so
// widening is adding invocation lines.

ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(not(feature = "wide"))]
ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  rest [AsyncPipe<TailHead, TailTail>: AsyncChain<Input>,] [<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(feature = "wide")]
const _: () = {
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
    ladder_impl!([AsyncPipe] [AsyncChain<Input>] [run] [Self::Output] [Input] [owned] [] [AsyncStep] []  rest [AsyncPipe<TailHead, TailTail>: AsyncChain<Input>,] [<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
};

/// The `Send` variant of [`AsyncChain`].
///
/// [`AsyncChain::run`] returns an unnameable `impl Future`, so a caller that
/// must prove the composed future is `Send` (a `tokio::spawn` boundary) cannot
/// write that bound. This trait restates the same composition with `Send`
/// promised in the return type. A stage future stays nameable through
/// [`AsyncStep::Future`], so its bound is expressible here.
pub trait AsyncChainSend<Input>: AsyncChain<Input> {
    /// Creates the future that runs this chain, promising `Send`.
    fn run_send(&mut self, input: Input) -> impl Future<Output = Self::Output> + Send;
}

ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(not(feature = "wide"))]
ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  rest [AsyncPipe<TailHead, TailTail>: AsyncChainSend<Input> + Send, <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output: Send,] [<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(feature = "wide")]
const _: () = {
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
    ladder_send_impl!([AsyncPipe] [AsyncChainSend<Input>] [run_send] [Self::Output] [Input] [inherited] [+ Send] [AsyncStep] [] []  rest [AsyncPipe<TailHead, TailTail>: AsyncChainSend<Input> + Send, <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output: Send,] [<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
};
