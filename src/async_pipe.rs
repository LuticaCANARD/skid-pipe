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

// The impl ladders below are mechanical: one impl per arity up to 16, then one
// that folds 16 stages over any shorter chain. They are macros because arity is
// this crate's main performance lever — a group's stages share one `async`
// block and rustc overlaps their futures into a single slot, so a wider group
// means a flatter, smaller future. Widening is adding invocation lines.

/// Folds a stage list into the nested pipeline type it names.
macro_rules! async_chain_ty {
    ($bottom:ty;) => { $bottom };
    ($bottom:ty; $s:ident $($rest:ident)*) => { async_chain_ty!(AsyncPipe<$s, $bottom>; $($rest)*) };
}

/// Walks `.tail` once per stage below the one being reached.
macro_rules! async_chain_at {
    ($this:expr;) => { $this };
    ($this:expr; $s:ident $($rest:ident)*) => { async_chain_at!($this.tail; $($rest)*) };
}

/// Emits one `AsyncChain` impl per invocation.
///
/// One pass accumulates everything the impl needs: `$cur` is the input type the
/// next stage sees, so the bounds fall out in order; `$fwd` keeps the stages
/// innermost-first for the type; `$body` collects the run body as statements.
/// The body is accumulated rather than recursed into so every `let` lands in a
/// single expansion — one hygiene context, so each stage's future is a
/// temporary that dies at its own statement and rustc overlaps them into one
/// slot, and an early `?` returns from one scope rather than 16.
macro_rules! async_chain_impls {
    (rest $($s:ident)+) => {
        async_chain_impls!(@rest
            [<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output]
            [AsyncPipe<TailHead, TailTail>: AsyncChain<Input>,]
            []
            this input carried
            [let carried = async_chain_at!(this; $($s)*).run(input).await;]
            $($s)+);
    };
    ($($s:ident)+) => {
        async_chain_impls!(@end [Input] [] [] this input carried
            [let carried = input;] $($s)+);
    };

    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        async_chain_impls!(@end
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = async_chain_at!($this; $($rest)+).head.call($car).await;]
            $($rest)+);
    };
    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        async_chain_impls!(@end
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@end [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* Input> AsyncChain<Input> for async_chain_ty!(End; $($fwd)*)
        where
            $($b)*
        {
            type Output = $out;

            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run(&mut self, $inp: Input) -> impl Future<Output = Self::Output> {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        async_chain_impls!(@rest
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = async_chain_at!($this; $($rest)+).head.call($car).await;]
            $($rest)+);
    };
    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        async_chain_impls!(@rest
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur>,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@rest [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* TailHead, TailTail, Input> AsyncChain<Input> for async_chain_ty!(AsyncPipe<TailHead, TailTail>; $($fwd)*)
        where
            $($b)*
        {
            type Output = $out;

            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run(&mut self, $inp: Input) -> impl Future<Output = Self::Output> {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

}

async_chain_impls!(S1);
async_chain_impls!(S1 S2);
async_chain_impls!(S1 S2 S3);
async_chain_impls!(S1 S2 S3 S4);
async_chain_impls!(S1 S2 S3 S4 S5);
async_chain_impls!(S1 S2 S3 S4 S5 S6);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
async_chain_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);
async_chain_impls!(rest S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);

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

/// Emits one `AsyncChainSend` impl per invocation.
///
/// One pass accumulates everything the impl needs: `$cur` is the input type the
/// next stage sees, so the bounds fall out in order; `$fwd` keeps the stages
/// innermost-first for the type; `$body` collects the run body as statements.
/// The body is accumulated rather than recursed into so every `let` lands in a
/// single expansion — one hygiene context, so each stage's future is a
/// temporary that dies at its own statement and rustc overlaps them into one
/// slot, and an early `?` returns from one scope rather than 16.
macro_rules! async_chain_send_impls {
    (rest $($s:ident)+) => {
        async_chain_send_impls!(@rest
            [<AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output]
            [Input: Send, AsyncPipe<TailHead, TailTail>: AsyncChainSend<Input> + Send, <AsyncPipe<TailHead, TailTail> as AsyncChain<Input>>::Output: Send,]
            []
            this input carried
            [let carried = async_chain_at!(this; $($s)*).run_send(input).await;]
            $($s)+);
    };
    ($($s:ident)+) => {
        async_chain_send_impls!(@end [Input] [Input: Send,] [] this input carried
            [let carried = input;] $($s)+);
    };

    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        async_chain_send_impls!(@end
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur> + Send, for<'a> <$s as AsyncStep<$cur>>::Future<'a>: Send, <$s as AsyncStep<$cur>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = async_chain_at!($this; $($rest)+).head.call($car).await;]
            $($rest)+);
    };
    (@end [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        async_chain_send_impls!(@end
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur> + Send, for<'a> <$s as AsyncStep<$cur>>::Future<'a>: Send, <$s as AsyncStep<$cur>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@end [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* Input> AsyncChainSend<Input> for async_chain_ty!(End; $($fwd)*)
        where
            $($b)*
        {
            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run_send(&mut self, $inp: Input) -> impl Future<Output = Self::Output> + Send {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        async_chain_send_impls!(@rest
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur> + Send, for<'a> <$s as AsyncStep<$cur>>::Future<'a>: Send, <$s as AsyncStep<$cur>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = async_chain_at!($this; $($rest)+).head.call($car).await;]
            $($rest)+);
    };
    (@rest [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        async_chain_send_impls!(@rest
            [<$s as AsyncStep<$cur>>::Output]
            [$($b)* $s: AsyncStep<$cur> + Send, for<'a> <$s as AsyncStep<$cur>>::Future<'a>: Send, <$s as AsyncStep<$cur>>::Output: Send,]
            [$($fwd)* $s]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@rest [$out:ty] [$($b:tt)*] [$($fwd:ident)*] $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        impl<$($fwd,)* TailHead, TailTail, Input> AsyncChainSend<Input> for async_chain_ty!(AsyncPipe<TailHead, TailTail>; $($fwd)*)
        where
            $($b)*
        {
            #[inline(always)]
            #[allow(clippy::manual_async_fn)]
            fn run_send(&mut self, $inp: Input) -> impl Future<Output = Self::Output> + Send {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };

}

async_chain_send_impls!(S1);
async_chain_send_impls!(S1 S2);
async_chain_send_impls!(S1 S2 S3);
async_chain_send_impls!(S1 S2 S3 S4);
async_chain_send_impls!(S1 S2 S3 S4 S5);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15);
async_chain_send_impls!(S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);
async_chain_send_impls!(rest S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 S15 S16);
