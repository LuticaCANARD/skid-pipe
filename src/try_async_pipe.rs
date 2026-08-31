use core::future::Future;

use crate::End;

/// A reusable, statically typed fallible asynchronous pipeline.
///
/// Each stage returns a [`Future`] resolving to a [`Result`]. The chain stops
/// at the first error and never enters a later stage.
pub struct TryAsyncPipe<Head, Tail = End> {
    pub(crate) head: Head,
    pub(crate) tail: Tail,
}

impl<Head> TryAsyncPipe<Head> {
    /// Starts a fallible asynchronous pipeline with its first step.
    #[inline(always)]
    pub const fn new(head: Head) -> Self {
        Self { head, tail: End }
    }
}

impl<Head, Tail> TryAsyncPipe<Head, Tail> {
    /// Appends the next fallible asynchronous step to this pipeline.
    #[inline(always)]
    pub const fn try_then<Next>(self, next: Next) -> TryAsyncPipe<Next, Self> {
        TryAsyncPipe {
            head: next,
            tail: self,
        }
    }

    /// Returns a future that runs every stage from left to right, stopping at
    /// the first error.
    ///
    /// Creating the future is lazy: no stage runs until it is first polled.
    /// The future holds the mutable pipeline borrow, so one pipeline instance
    /// cannot have overlapping runs.
    ///
    /// ```compile_fail
    /// use skid_pipe::TryAsyncPipe;
    ///
    /// let mut pipeline =
    ///     TryAsyncPipe::new(|value: u8| core::future::ready(Ok::<_, ()>(value + 1)));
    /// let first = pipeline.run(1);
    /// let second = pipeline.run(2);
    /// drop((first, second));
    /// ```
    #[inline(always)]
    pub fn run<Input, Error>(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<<Self as TryAsyncChain<Input, Error>>::Output, Error>>
    where
        Self: TryAsyncChain<Input, Error>,
    {
        TryAsyncChain::run(self, input)
    }
}

/// A callable fallible asynchronous pipeline stage.
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
/// concrete pipeline type. External implementations must run stages from left
/// to right and stop after the first error.
/// `run` deliberately returns an `async` block rather than being an `async fn`.
/// Clippy's `manual_async_fn` asks for the shorter spelling, but on this crate's
/// 100-stage footprint example the `async fn` form measures 320 B against the
/// block form's 216 B, so the lint is allowed at each `run` instead.
pub trait TryAsyncChain<Input, Error>: Sized {
    /// The success value emitted when the completed pipeline resolves.
    type Output;

    /// Creates the future that runs this chain.
    fn run(&mut self, input: Input) -> impl Future<Output = Result<Self::Output, Error>>;
}

// The ladder below is written by the shared accumulators in `ladder.rs`.
// Arity is this crate's main performance lever — a group's stages share one
// `async` block and rustc overlaps their futures into a single slot — so
// widening is adding invocation lines.

ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(not(feature = "wide"))]
ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  rest [TryAsyncPipe<TailHead, TailTail>: TryAsyncChain<Input, Error>,] [<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(feature = "wide")]
const _: () = {
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
    ladder_impl!([TryAsyncPipe] [TryAsyncChain<Input, Error>] [run] [Result<Self::Output, Error>] [Input, Error] [owned] [] [TryAsyncStep, Error] [?]  rest [TryAsyncPipe<TailHead, TailTail>: TryAsyncChain<Input, Error>,] [<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
};

/// The `Send` variant of [`TryAsyncChain`], for the same reason as
/// [`AsyncChainSend`](crate::AsyncChainSend): the composed future is
/// unnameable, so a `tokio::spawn` caller cannot bound it.
pub trait TryAsyncChainSend<Input, Error>: TryAsyncChain<Input, Error> {
    /// Creates the future that runs this chain, promising `Send`.
    fn run_send(
        &mut self,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, Error>> + Send;
}

ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(not(feature = "wide"))]
ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  rest [TryAsyncPipe<TailHead, TailTail>: TryAsyncChainSend<Input, Error> + Send, <TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output: Send,] [<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

#[cfg(feature = "wide")]
const _: () = {
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  end S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
    ladder_send_impl!([TryAsyncPipe] [TryAsyncChainSend<Input, Error>] [run_send] [Result<Self::Output, Error>] [Input, Error] [inherited] [+ Send] [TryAsyncStep, Error] [?] [Error: Send,]  rest [TryAsyncPipe<TailHead, TailTail>: TryAsyncChainSend<Input, Error> + Send, <TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output: Send,] [<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output] S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
};
