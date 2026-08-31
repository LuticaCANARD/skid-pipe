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

// The impl ladders below are mechanical: one impl per arity up to 16, then one
// that folds 16 stages over any shorter chain. They are macros because arity is
// this crate's main performance lever — a group's stages share one `async`
// block and rustc overlaps their futures into a single slot, so a wider group
// means a flatter, smaller future. Widening is adding invocation lines.

/// Folds a stage list into the nested pipeline type it names.
macro_rules! try_async_chain_ty {
    ($bottom:ty;) => { $bottom };
    ($bottom:ty; $s:ident $($rest:ident)*) => { try_async_chain_ty!(TryAsyncPipe<$s, $bottom>; $($rest)*) };
}

/// Emits one `TryAsyncChain` impl per invocation.
///
/// One pass accumulates everything the impl needs: `$cur` is the input type the
/// next stage sees, so the bounds fall out in order; `$fwd` keeps the stages
/// innermost-first for the type; `$body` collects the run body as statements.
/// The body is accumulated rather than recursed into so every `let` lands in a
/// single expansion — one hygiene context, so each stage's future is a
/// temporary that dies at its own statement and rustc overlaps them into one
/// slot, and an early `?` returns from one scope rather than 16.
macro_rules! try_async_chain_impls {
    (rest $($s:ident)+) => {
        try_async_chain_impls!(@rest
            [<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output]
            [TryAsyncPipe<TailHead, TailTail>: TryAsyncChain<Input, Error>,]
            []
            this input carried
            [let carried = chain_at!(this; $($s)*).run(input).await?;]
            $($s)+);
    };
    ($($s:ident)+) => {
        try_async_chain_impls!(@end [Input] [] [] this input carried
            [let carried = input;] $($s)+);
    };

    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        try_async_chain_impls!(@end
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = chain_at!($this; $($rest)+).head.call($car).await?;]
            $($rest)+);
    };
    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        try_async_chain_impls!(@end
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@end [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* Input, Error> TryAsyncChain<Input, Error> for try_async_chain_ty!(End; $($fwd)*)
        where
            $($b)*
        {
            type Output = $out;

            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run(&mut self, $inp: Input) -> impl Future<Output = Result<Self::Output, Error>> {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        try_async_chain_impls!(@rest
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = chain_at!($this; $($rest)+).head.call($car).await?;]
            $($rest)+);
    };
    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        try_async_chain_impls!(@rest
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@rest [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* TailHead, TailTail, Input, Error> TryAsyncChain<Input, Error> for try_async_chain_ty!(TryAsyncPipe<TailHead, TailTail>; $($fwd)*)
        where
            $($b)*
        {
            type Output = $out;

            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run(&mut self, $inp: Input) -> impl Future<Output = Result<Self::Output, Error>> {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

}

try_async_chain_impls!(S1);
try_async_chain_impls!(S1 S2);
try_async_chain_impls!(S1 S2 S3);
try_async_chain_impls!(S1 S2 S3 S4);
try_async_chain_impls!(S1 S2 S3 S4 S5);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

// Without `wide`, groups of 16 fold over anything longer.
#[cfg(not(feature = "wide"))]
try_async_chain_impls!(rest S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

// With it the ladder runs to 32 and folds 32 at a time: a 100-stage chain
// costs 364.11 ns rather than 385, its run future 72 B rather than 120 B, at
// roughly five times the crate's own compile time. Turning it on anywhere
// turns it on for everyone, which is what keeps the feature additive.
#[cfg(feature = "wide")]
const _: () = {
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31);
    try_async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
    try_async_chain_impls!(rest S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
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

/// Emits one `TryAsyncChainSend` impl per invocation.
///
/// One pass accumulates everything the impl needs: `$cur` is the input type the
/// next stage sees, so the bounds fall out in order; `$fwd` keeps the stages
/// innermost-first for the type; `$body` collects the run body as statements.
/// The body is accumulated rather than recursed into so every `let` lands in a
/// single expansion — one hygiene context, so each stage's future is a
/// temporary that dies at its own statement and rustc overlaps them into one
/// slot, and an early `?` returns from one scope rather than 16.
macro_rules! try_async_chain_send_impls {
    (rest $($s:ident)+) => {
        try_async_chain_send_impls!(@rest
            [<TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output]
            [Input: Send, Error: Send, TryAsyncPipe<TailHead, TailTail>: TryAsyncChainSend<Input, Error> + Send, <TryAsyncPipe<TailHead, TailTail> as TryAsyncChain<Input, Error>>::Output: Send,]
            []
            this input carried
            [let carried = chain_at!(this; $($s)*).run_send(input).await?;]
            $($s)+);
    };
    ($($s:ident)+) => {
        try_async_chain_send_impls!(@end [Input] [Input: Send, Error: Send,] [] this input carried
            [let carried = input;] $($s)+);
    };

    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        try_async_chain_send_impls!(@end
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error> + Send, for<'a> <$s as TryAsyncStep<$cur, Error>>::Future<'a>: Send, <$s as TryAsyncStep<$cur, Error>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = chain_at!($this; $($rest)+).head.call($car).await?;]
            $($rest)+);
    };
    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        try_async_chain_send_impls!(@end
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error> + Send, for<'a> <$s as TryAsyncStep<$cur, Error>>::Future<'a>: Send, <$s as TryAsyncStep<$cur, Error>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@end [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* Input, Error> TryAsyncChainSend<Input, Error> for try_async_chain_ty!(End; $($fwd)*)
        where
            $($b)*
        {
            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run_send(&mut self, $inp: Input) -> impl Future<Output = Result<Self::Output, Error>> + Send {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        try_async_chain_send_impls!(@rest
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error> + Send, for<'a> <$s as TryAsyncStep<$cur, Error>>::Future<'a>: Send, <$s as TryAsyncStep<$cur, Error>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = chain_at!($this; $($rest)+).head.call($car).await?;]
            $($rest)+);
    };
    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        try_async_chain_send_impls!(@rest
            [<$s as TryAsyncStep<$cur, Error>>::Output]
            [$($b)* $s: TryAsyncStep<$cur, Error> + Send, for<'a> <$s as TryAsyncStep<$cur, Error>>::Future<'a>: Send, <$s as TryAsyncStep<$cur, Error>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@rest [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* TailHead, TailTail, Input, Error> TryAsyncChainSend<Input, Error> for try_async_chain_ty!(TryAsyncPipe<TailHead, TailTail>; $($fwd)*)
        where
            $($b)*
        {
            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run_send(&mut self, $inp: Input) -> impl Future<Output = Result<Self::Output, Error>> + Send {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

}

try_async_chain_send_impls!(S1);
try_async_chain_send_impls!(S1 S2);
try_async_chain_send_impls!(S1 S2 S3);
try_async_chain_send_impls!(S1 S2 S3 S4);
try_async_chain_send_impls!(S1 S2 S3 S4 S5);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

// Without `wide`, groups of 16 fold over anything longer.
#[cfg(not(feature = "wide"))]
try_async_chain_send_impls!(rest S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

// With it the ladder runs to 32 and folds 32 at a time: a 100-stage chain
// costs 364.11 ns rather than 385, its run future 72 B rather than 120 B, at
// roughly five times the crate's own compile time. Turning it on anywhere
// turns it on for everyone, which is what keeps the feature additive.
#[cfg(feature = "wide")]
const _: () = {
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31);
    try_async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
    try_async_chain_send_impls!(rest S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32);
};
