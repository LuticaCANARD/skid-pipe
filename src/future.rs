//! Internal allocation-free async state machines.
//!
//! # Safety invariant
//!
//! Each future's state tag names the only initialized `ManuallyDrop` union
//! member. The containing future is `!Unpin`; polling projects only the active
//! member as pinned, never moves it, drops it in place exactly once, marks the
//! slot empty, and only then initializes the next member. `Drop` consults the
//! same tag. No unsafe operation escapes this module.

use core::{
    future::Future,
    marker::{PhantomData, PhantomPinned},
    mem::ManuallyDrop,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    async_pipe::{AsyncChain, AsyncPipe, AsyncStep},
    try_async_pipe::{TryAsyncChain, TryAsyncPipe, TryAsyncStep},
};

type StepOutput<Step, Input> = <Step as AsyncStep<Input>>::Output;
type TryStepOutput<Step, Input, Error> = <Step as TryAsyncStep<Input, Error>>::Output;
type ChainOutput<Chain, Input> = <Chain as AsyncChain<Input>>::Output;
type TryChainOutput<Chain, Input, Error> = <Chain as TryAsyncChain<Input, Error>>::Output;

/// Marker for an infallible first-stage future.
#[doc(hidden)]
pub struct AsyncStart;

/// Marker for a fallible first-stage future.
#[doc(hidden)]
pub struct TryStart<Error>(PhantomData<fn() -> Error>);

// These public-but-hidden types must be nameable because public chain GATs
// expose them. Sealing the adapter trait keeps them implementation details:
// downstream crates can use the associated future types but cannot add
// competing first-stage adapters.
mod sealed {
    use super::{AsyncStart, TryStart};
    use crate::{AsyncStep, TryAsyncStep};

    pub trait Sealed<Mode, Input> {}

    impl<Head, Input> Sealed<AsyncStart, Input> for Head where Head: AsyncStep<Input> {}

    impl<Head, Input, Error> Sealed<TryStart<Error>, Input> for Head where
        Head: TryAsyncStep<Input, Error>
    {
    }
}

#[doc(hidden)]
pub trait StartStep<Mode, Input>: sealed::Sealed<Mode, Input> + Sized {
    type Output;
    type Future<'a>: Future<Output = Self::Output>
    where
        Self: 'a;

    fn start(&mut self, input: Input) -> Self::Future<'_>;
}

impl<Head, Input> StartStep<AsyncStart, Input> for Head
where
    Head: AsyncStep<Input>,
{
    type Output = Head::Output;
    type Future<'a>
        = Head::Future<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn start(&mut self, input: Input) -> Self::Future<'_> {
        AsyncStep::call(self, input)
    }
}

impl<Head, Input, Error> StartStep<TryStart<Error>, Input> for Head
where
    Head: TryAsyncStep<Input, Error>,
{
    type Output = Result<Head::Output, Error>;
    type Future<'a>
        = Head::Future<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn start(&mut self, input: Input) -> Self::Future<'_> {
        TryAsyncStep::call(self, input)
    }
}

/// A `&'a mut T` held by value.
///
/// Two things need this. A machine used to park each step in an
/// `Option<&mut Step>` and `take` it when that step started; `Option::take`
/// writes `None` back, and that store is redundant because the state tag
/// already records which steps are consumed. And a machine now derives several
/// disjoint references from one stored borrow across separate `poll` calls,
/// which a stored `&mut` cannot do: re-deriving through the pinned future would
/// invalidate the reference the running stage still holds. Keeping the pointer
/// by value preserves the original borrow's provenance instead.
struct Borrowed<'a, T> {
    pointer: *mut T,
    _borrow: PhantomData<&'a mut T>,
}

// SAFETY: `Borrowed` is exactly a `&'a mut T` moved into a field, so it carries
// the same aliasing guarantee and therefore the same auto traits.
unsafe impl<T: Send> Send for Borrowed<'_, T> {}
// SAFETY: as above; shared access to a `&mut T` is shared access to `T`.
unsafe impl<T: Sync> Sync for Borrowed<'_, T> {}

impl<'a, T> Borrowed<'a, T> {
    #[inline(always)]
    fn new(target: &'a mut T) -> Self {
        Self {
            pointer: core::ptr::from_mut(target),
            _borrow: PhantomData,
        }
    }

    /// The borrow as a raw pointer, valid for all of `'a`.
    ///
    /// Dereferencing it is the caller's obligation: only one reference derived
    /// from it, or from a disjoint part of it, may be live at a time.
    #[inline(always)]
    fn as_ptr(&self) -> *mut T {
        self.pointer
    }

    /// Reborrows the whole target for `'a`.
    ///
    /// # Safety
    ///
    /// The caller must call this at most once, because the result aliases every
    /// other reference this would produce.
    #[inline(always)]
    unsafe fn get(&self) -> &'a mut T {
        // SAFETY: `pointer` is a copy of the `&'a mut T` handed to `new`, so it
        // keeps that reference's provenance and is valid for all of `'a`. The
        // caller guarantees this is the only derivation.
        unsafe { &mut *self.pointer }
    }
}

#[derive(Clone, Copy)]
enum StageState {
    Input,
    Future,
    Done,
}

union StageSlot<Input, StageFuture> {
    input: ManuallyDrop<Input>,
    future: ManuallyDrop<StageFuture>,
}

/// Lazy future for the first stage in an async chain.
///
/// It owns the input and retains the pipeline borrow before its first poll, so
/// creating or dropping the future does not call the stage and overlapping
/// runs remain rejected even when the stage returns an independently owned
/// future such as [`core::future::Ready`].
#[doc(hidden)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct FirstStageFuture<'a, Mode, Head, Input, HeadFuture> {
    head: Borrowed<'a, Head>,
    slot: StageSlot<Input, HeadFuture>,
    state: StageState,
    _mode: PhantomData<Mode>,
    _pin: PhantomPinned,
}

impl<'a, Mode, Head, Input, HeadFuture> FirstStageFuture<'a, Mode, Head, Input, HeadFuture> {
    #[inline(always)]
    pub(crate) fn new(head: &'a mut Head, input: Input) -> Self {
        Self {
            head: Borrowed::new(head),
            slot: StageSlot {
                input: ManuallyDrop::new(input),
            },
            state: StageState::Input,
            _mode: PhantomData,
            _pin: PhantomPinned,
        }
    }
}

impl<'a, Mode, Head, Input, HeadFuture> Future
    for FirstStageFuture<'a, Mode, Head, Input, HeadFuture>
where
    Head: StartStep<Mode, Input, Future<'a> = HeadFuture> + 'a,
    HeadFuture: Future<Output = <Head as StartStep<Mode, Input>>::Output>,
{
    type Output = <Head as StartStep<Mode, Input>>::Output;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            // SAFETY: the outer future is pinned; the active union member is
            // projected as pinned and never moved.
            let this = unsafe { self.as_mut().get_unchecked_mut() };

            match this.state {
                StageState::Input => {
                    this.state = StageState::Done;
                    // SAFETY: `Input` identified the initialized member. The
                    // state was cleared before moving the owned input out.
                    let input = unsafe { ManuallyDrop::take(&mut this.slot.input) };
                    // SAFETY: the `Input` state is entered at most once,
                    // because the tag advances to `Future` and never returns,
                    // so this is the only derivation of `head`.
                    let head = unsafe { this.head.get() };
                    let future = StartStep::start(head, input);
                    this.slot.future = ManuallyDrop::new(future);
                    this.state = StageState::Future;
                }
                StageState::Future => {
                    // SAFETY: `Future` identifies the initialized pinned member.
                    let poll = unsafe { Pin::new_unchecked(&mut *this.slot.future) }.poll(context);
                    match poll {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(output) => {
                            this.state = StageState::Done;
                            // SAFETY: the completed future is initialized and is
                            // dropped once after clearing the state tag.
                            unsafe { ManuallyDrop::drop(&mut this.slot.future) };
                            return Poll::Ready(output);
                        }
                    }
                }
                StageState::Done => panic!("pipeline future polled after completion"),
            }
        }
    }
}

impl<Mode, Head, Input, HeadFuture> Drop for FirstStageFuture<'_, Mode, Head, Input, HeadFuture> {
    fn drop(&mut self) {
        let state = core::mem::replace(&mut self.state, StageState::Done);
        // SAFETY: `state` identifies the single initialized union member. It
        // is replaced first so a panicking destructor cannot run twice.
        unsafe {
            match state {
                StageState::Input => ManuallyDrop::drop(&mut self.slot.input),
                StageState::Future => ManuallyDrop::drop(&mut self.slot.future),
                StageState::Done => {}
            }
        }
    }
}

/// Unwraps one stage outcome, short-circuiting a fallible machine's first `Err`.
macro_rules! short_circuit {
    (plain, $outcome:ident) => {
        $outcome
    };
    (fallible, $outcome:ident) => {
        match $outcome {
            Ok(value) => value,
            Err(error) => return Poll::Ready(Err(error)),
        }
    };
}

/// Generates one sequential stage machine.
///
/// The eight link futures below differ only in how many links they peel, whether
/// stage outputs are `Result`s, and which adapter traits they call. Their unsafe
/// protocol is identical, so it is written exactly once here: poll the active
/// union member, and on completion clear the state tag *before* dropping that
/// member in place, then initialize the next member and only then record its
/// tag. A fix to that ordering lands in every machine at once.
///
/// Every machine stores its sub-pipeline as one [`Borrowed`] pointer and
/// projects a single stage out of it when that stage starts, so a chain holds
/// one reference per group of links instead of one per stage. That is what
/// makes creating a run future cheap: a 100-stage chain writes one pointer and
/// one tag per group rather than one pointer per stage.
///
/// Parameters:
/// - `state` / `slot`: the private state tag and union shared by the machines
///   of one arity.
/// - `call` / `run`: the adapter entry points, [`AsyncStep::call`] and
///   [`AsyncChain::run`] or their fallible counterparts.
/// - `mode`: `plain` or `fallible`, selecting the [`short_circuit!`] arm. It
///   duplicates what `error` already says because a `$( )?` repetition over an
///   outer meta-variable cannot appear inside the per-step repetition.
/// - `error`: empty for the infallible machines; the fallible ones pass their
///   trailing `Error` parameter.
/// - `tail`: the field path from this machine's pipeline to the tail chain.
/// - `steps`: one entry per link, as `(polled state, its union member) ->
///   (next state, that member, the field path to the step, its future type)`.
/// - `last`: the final state and the union member it polls, whose output is the
///   machine's own output.
/// - `generics` / `pipeline` / `bounds` / `output`: the `Future` impl's header.
macro_rules! stage_machine {
    (
        $(#[$meta:meta])*
        $name:ident,
        state: $state:ident,
        slot: $slot:ident,
        call: $call:path,
        run: $run:path,
        mode: $mode:ident,
        error: [$($error:ident)?],
        tail: [$($tail_seg:ident).+],
        steps: [
            $(
                ($from:ident, $from_member:ident)
                    -> ($variant:ident, $member:ident, [$($seg:ident).+], $stepfut:ident)
            ),+ $(,)?
        ],
        last: ($last:ident, $last_member:ident),
        generics: [$($generic:tt)*],
        pipeline: [$($pipeline:tt)*],
        bounds: { $($bounds:tt)* },
        output: $output:ty,
    ) => {
        $(#[$meta])*
        #[doc(hidden)]
        #[must_use = "futures do nothing unless you `.await` or poll them"]
        pub struct $name<'a, Pipeline, Input, TailFuture, $($stepfut,)* $($error)?> {
            pipeline: Borrowed<'a, Pipeline>,
            slot: $slot<Input, TailFuture, $($stepfut),*>,
            state: $state,
            $(_error: PhantomData<fn() -> $error>,)?
            _pin: PhantomPinned,
        }

        impl<$($generic)*> $name<'a, $($pipeline)*, Input, TailFuture, $($stepfut,)* $($error)?>
        where
            $($bounds)*
        {
            #[cfg(not(feature = "lazy-construction"))]
            #[inline(always)]
            pub(crate) fn new(pipeline: &'a mut $($pipeline)*, input: Input) -> Self {
                let pipeline = Borrowed::new(pipeline);
                // SAFETY: `pipeline` was just built from a `&'a mut` to this
                // layer's sub-pipeline and nothing has been derived from it yet,
                // so this field projection is the only live derivation. Asking
                // the tail chain for its future runs no stage: every chain's own
                // future defers its first stage to the first poll.
                let rest = unsafe { &mut (*pipeline.as_ptr()) $(.$tail_seg)* };
                Self {
                    pipeline,
                    slot: $slot {
                        tail: ManuallyDrop::new($run(rest, input)),
                    },
                    state: $state::Tail,
                    $(_error: PhantomData::<fn() -> $error>,)?
                    _pin: PhantomPinned,
                }
            }

            /// Parks the input and leaves the tail chain's future to the first
            /// poll, so creating a run future is one layer's work rather than
            /// the whole nest's.
            #[cfg(feature = "lazy-construction")]
            #[inline(always)]
            pub(crate) fn new(pipeline: &'a mut $($pipeline)*, input: Input) -> Self {
                Self {
                    pipeline: Borrowed::new(pipeline),
                    slot: $slot {
                        input: ManuallyDrop::new(input),
                    },
                    state: $state::Input,
                    $(_error: PhantomData::<fn() -> $error>,)?
                    _pin: PhantomPinned,
                }
            }
        }

        impl<$($generic)*> Future
            for $name<'a, $($pipeline)*, Input, TailFuture, $($stepfut,)* $($error)?>
        where
            $($bounds)*
        {
            type Output = $output;

            #[inline(always)]
            fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
                loop {
                    // SAFETY: the outer future is pinned and `!Unpin`; `this` only
                    // ever projects the active union member as pinned and never
                    // moves any member out of the slot.
                    let this = unsafe { self.as_mut().get_unchecked_mut() };

                    match this.state {
                        #[cfg(feature = "lazy-construction")]
                        $state::Input => {
                            // SAFETY: `Input` identified the initialized member.
                            // The tag is cleared before the owned input moves
                            // out, so no later drop repeats it.
                            this.state = $state::Done;
                            let input =
                                unsafe { ManuallyDrop::take(&mut this.slot.input) };
                            // SAFETY: a field projection of the same `&'a mut`
                            // sub-pipeline. The slot holds no future yet, so this
                            // is the only live derivation, and this state is
                            // entered at most once because the tag only advances.
                            // Asking the tail chain for its future runs no stage.
                            let rest = unsafe {
                                &mut (*this.pipeline.as_ptr()) $(.$tail_seg)*
                            };
                            this.slot.tail = ManuallyDrop::new($run(rest, input));
                            this.state = $state::Tail;
                        }
                        $(
                            $state::$from => {
                                // SAFETY: the state tag names the initialized
                                // member, which the pin guarantees cannot move.
                                let poll = unsafe {
                                    Pin::new_unchecked(&mut *this.slot.$from_member)
                                }
                                .poll(context);
                                match poll {
                                    Poll::Pending => return Poll::Pending,
                                    Poll::Ready(outcome) => {
                                        // SAFETY: the completed member is
                                        // initialized and is dropped exactly
                                        // once; the tag is cleared first so no
                                        // later drop repeats it, and the next
                                        // member is written only afterwards.
                                        this.state = $state::Done;
                                        unsafe {
                                            ManuallyDrop::drop(&mut this.slot.$from_member)
                                        };

                                        let value = short_circuit!($mode, outcome);
                                        // SAFETY: a disjoint field projection of
                                        // the same `&'a mut` sub-pipeline. The
                                        // member just dropped held the only other
                                        // live derivation, so this is now the
                                        // single one, and each state is reached at
                                        // most once because the tag only advances.
                                        let step = unsafe {
                                            &mut (*this.pipeline.as_ptr()) $(.$seg)*
                                        };
                                        this.slot.$member =
                                            ManuallyDrop::new($call(step, value));
                                        this.state = $state::$variant;
                                    }
                                }
                            }
                        )+
                        $state::$last => {
                            // SAFETY: the state tag names the initialized member,
                            // which the pin guarantees cannot move.
                            let poll = unsafe {
                                Pin::new_unchecked(&mut *this.slot.$last_member)
                            }
                            .poll(context);
                            match poll {
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(output) => {
                                    // SAFETY: the completed member is initialized
                                    // and is dropped exactly once after clearing
                                    // the state tag.
                                    this.state = $state::Done;
                                    unsafe {
                                        ManuallyDrop::drop(&mut this.slot.$last_member)
                                    };
                                    return Poll::Ready(output);
                                }
                            }
                        }
                        $state::Done => panic!("pipeline future polled after completion"),
                    }
                }
            }
        }

        impl<Pipeline, Input, TailFuture, $($stepfut,)* $($error)?> Drop
            for $name<'_, Pipeline, Input, TailFuture, $($stepfut,)* $($error)?>
        {
            fn drop(&mut self) {
                let state = core::mem::replace(&mut self.state, $state::Done);
                // SAFETY: `state` identifies the single initialized union member.
                // It is replaced first so a panicking destructor cannot run twice.
                unsafe {
                    match state {
                        #[cfg(feature = "lazy-construction")]
                        $state::Input => ManuallyDrop::drop(&mut self.slot.input),
                        $($state::$from => ManuallyDrop::drop(&mut self.slot.$from_member),)+
                        $state::$last => ManuallyDrop::drop(&mut self.slot.$last_member),
                        $state::Done => {}
                    }
                }
            }
        }
    };
}

#[derive(Clone, Copy)]
enum State1 {
    #[cfg(feature = "lazy-construction")]
    Input,
    Tail,
    Step1,
    Done,
}

union Slot1<Input, TailFuture, F1> {
    // Only `lazy-construction` parks an input here. The member costs
    // no layout either way: `TailFuture` bottoms out in a
    // `FirstStageFuture` that holds this same `Input`, so the union is
    // at least this wide already.
    #[cfg_attr(not(feature = "lazy-construction"), allow(dead_code))]
    input: ManuallyDrop<Input>,
    tail: ManuallyDrop<TailFuture>,
    s1: ManuallyDrop<F1>,
}

stage_machine! {
    /// Future for one adjacent infallible pipeline link.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    ThenFuture,
    state: State1,
    slot: Slot1,
    call: AsyncStep::call,
    run: AsyncChain::run,
    mode: plain,
    error: [],
    tail: [tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [head], F1)
    ],
    last: (Step1, s1),
    generics: ['a, S1, Rest, Input, TailFuture, F1],
    pipeline: [AsyncPipe<S1, Rest>],
    bounds: {
        Rest: AsyncChain<Input, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = ChainOutput<Rest, Input>>,
        S1: AsyncStep<ChainOutput<Rest, Input>, Future<'a> = F1> + 'a,
        F1: Future<Output = StepOutput<S1, ChainOutput<Rest, Input>>>,
    },
    output: StepOutput<S1, ChainOutput<Rest, Input>>,
}

stage_machine! {
    /// Future for one adjacent fallible pipeline link.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    TryThenFuture,
    state: State1,
    slot: Slot1,
    call: TryAsyncStep::call,
    run: TryAsyncChain::run,
    mode: fallible,
    error: [Error],
    tail: [tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [head], F1)
    ],
    last: (Step1, s1),
    generics: ['a, S1, Rest, Input, TailFuture, F1, Error],
    pipeline: [TryAsyncPipe<S1, Rest>],
    bounds: {
        Rest: TryAsyncChain<Input, Error, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = Result<TryChainOutput<Rest, Input, Error>, Error>>,
        S1: TryAsyncStep<TryChainOutput<Rest, Input, Error>, Error, Future<'a> = F1> + 'a,
        F1: Future<Output = Result<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>>,
    },
    output: Result<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>,
}

#[derive(Clone, Copy)]
enum State2 {
    #[cfg(feature = "lazy-construction")]
    Input,
    Tail,
    Step1,
    Step2,
    Done,
}

union Slot2<Input, TailFuture, F1, F2> {
    // Only `lazy-construction` parks an input here. The member costs
    // no layout either way: `TailFuture` bottoms out in a
    // `FirstStageFuture` that holds this same `Input`, so the union is
    // at least this wide already.
    #[cfg_attr(not(feature = "lazy-construction"), allow(dead_code))]
    input: ManuallyDrop<Input>,
    tail: ManuallyDrop<TailFuture>,
    s1: ManuallyDrop<F1>,
    s2: ManuallyDrop<F2>,
}

stage_machine! {
    /// Future for two adjacent infallible pipeline links.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    ThenPairFuture,
    state: State2,
    slot: Slot2,
    call: AsyncStep::call,
    run: AsyncChain::run,
    mode: plain,
    error: [],
    tail: [tail.tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [tail.head], F1),
        (Step1, s1) -> (Step2, s2, [head], F2)
    ],
    last: (Step2, s2),
    generics: ['a, S1, S2, Rest, Input, TailFuture, F1, F2],
    pipeline: [AsyncPipe<S2, AsyncPipe<S1, Rest>>],
    bounds: {
        Rest: AsyncChain<Input, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = ChainOutput<Rest, Input>>,
        S1: AsyncStep<ChainOutput<Rest, Input>, Future<'a> = F1> + 'a,
        F1: Future<Output = StepOutput<S1, ChainOutput<Rest, Input>>>,
        S2: AsyncStep<StepOutput<S1, ChainOutput<Rest, Input>>, Future<'a> = F2> + 'a,
        F2: Future<Output = StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>,
    },
    output: StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>,
}

stage_machine! {
    /// Future for two adjacent fallible pipeline links.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    TryThenPairFuture,
    state: State2,
    slot: Slot2,
    call: TryAsyncStep::call,
    run: TryAsyncChain::run,
    mode: fallible,
    error: [Error],
    tail: [tail.tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [tail.head], F1),
        (Step1, s1) -> (Step2, s2, [head], F2)
    ],
    last: (Step2, s2),
    generics: ['a, S1, S2, Rest, Input, TailFuture, F1, F2, Error],
    pipeline: [TryAsyncPipe<S2, TryAsyncPipe<S1, Rest>>],
    bounds: {
        Rest: TryAsyncChain<Input, Error, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = Result<TryChainOutput<Rest, Input, Error>, Error>>,
        S1: TryAsyncStep<TryChainOutput<Rest, Input, Error>, Error, Future<'a> = F1> + 'a,
        F1: Future<Output = Result<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>>,
        S2: TryAsyncStep<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error, Future<'a> = F2> + 'a,
        F2: Future<Output = Result<TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>>,
    },
    output: Result<TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>,
}

#[derive(Clone, Copy)]
enum State4 {
    #[cfg(feature = "lazy-construction")]
    Input,
    Tail,
    Step1,
    Step2,
    Step3,
    Step4,
    Done,
}

union Slot4<Input, TailFuture, F1, F2, F3, F4> {
    // Only `lazy-construction` parks an input here. The member costs
    // no layout either way: `TailFuture` bottoms out in a
    // `FirstStageFuture` that holds this same `Input`, so the union is
    // at least this wide already.
    #[cfg_attr(not(feature = "lazy-construction"), allow(dead_code))]
    input: ManuallyDrop<Input>,
    tail: ManuallyDrop<TailFuture>,
    s1: ManuallyDrop<F1>,
    s2: ManuallyDrop<F2>,
    s3: ManuallyDrop<F3>,
    s4: ManuallyDrop<F4>,
}

stage_machine! {
    /// Future for four adjacent infallible pipeline links.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    ThenQuadFuture,
    state: State4,
    slot: Slot4,
    call: AsyncStep::call,
    run: AsyncChain::run,
    mode: plain,
    error: [],
    tail: [tail.tail.tail.tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [tail.tail.tail.head], F1),
        (Step1, s1) -> (Step2, s2, [tail.tail.head], F2),
        (Step2, s2) -> (Step3, s3, [tail.head], F3),
        (Step3, s3) -> (Step4, s4, [head], F4)
    ],
    last: (Step4, s4),
    generics: ['a, S1, S2, S3, S4, Rest, Input, TailFuture, F1, F2, F3, F4],
    pipeline: [AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, Rest>>>>],
    bounds: {
        Rest: AsyncChain<Input, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = ChainOutput<Rest, Input>>,
        S1: AsyncStep<ChainOutput<Rest, Input>, Future<'a> = F1> + 'a,
        F1: Future<Output = StepOutput<S1, ChainOutput<Rest, Input>>>,
        S2: AsyncStep<StepOutput<S1, ChainOutput<Rest, Input>>, Future<'a> = F2> + 'a,
        F2: Future<Output = StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>,
        S3: AsyncStep<StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>, Future<'a> = F3> + 'a,
        F3: Future<Output = StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>,
        S4: AsyncStep<StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>, Future<'a> = F4> + 'a,
        F4: Future<Output = StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>,
    },
    output: StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>,
}

stage_machine! {
    /// Future for four adjacent fallible pipeline links.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    TryThenQuadFuture,
    state: State4,
    slot: Slot4,
    call: TryAsyncStep::call,
    run: TryAsyncChain::run,
    mode: fallible,
    error: [Error],
    tail: [tail.tail.tail.tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [tail.tail.tail.head], F1),
        (Step1, s1) -> (Step2, s2, [tail.tail.head], F2),
        (Step2, s2) -> (Step3, s3, [tail.head], F3),
        (Step3, s3) -> (Step4, s4, [head], F4)
    ],
    last: (Step4, s4),
    generics: ['a, S1, S2, S3, S4, Rest, Input, TailFuture, F1, F2, F3, F4, Error],
    pipeline: [TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, Rest>>>>],
    bounds: {
        Rest: TryAsyncChain<Input, Error, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = Result<TryChainOutput<Rest, Input, Error>, Error>>,
        S1: TryAsyncStep<TryChainOutput<Rest, Input, Error>, Error, Future<'a> = F1> + 'a,
        F1: Future<Output = Result<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>>,
        S2: TryAsyncStep<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error, Future<'a> = F2> + 'a,
        F2: Future<Output = Result<TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>>,
        S3: TryAsyncStep<TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error, Future<'a> = F3> + 'a,
        F3: Future<Output = Result<TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>>,
        S4: TryAsyncStep<TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error, Future<'a> = F4> + 'a,
        F4: Future<Output = Result<TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>>,
    },
    output: Result<TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>,
}

#[derive(Clone, Copy)]
enum State8 {
    #[cfg(feature = "lazy-construction")]
    Input,
    Tail,
    Step1,
    Step2,
    Step3,
    Step4,
    Step5,
    Step6,
    Step7,
    Step8,
    Done,
}

union Slot8<Input, TailFuture, F1, F2, F3, F4, F5, F6, F7, F8> {
    // Only `lazy-construction` parks an input here. The member costs
    // no layout either way: `TailFuture` bottoms out in a
    // `FirstStageFuture` that holds this same `Input`, so the union is
    // at least this wide already.
    #[cfg_attr(not(feature = "lazy-construction"), allow(dead_code))]
    input: ManuallyDrop<Input>,
    tail: ManuallyDrop<TailFuture>,
    s1: ManuallyDrop<F1>,
    s2: ManuallyDrop<F2>,
    s3: ManuallyDrop<F3>,
    s4: ManuallyDrop<F4>,
    s5: ManuallyDrop<F5>,
    s6: ManuallyDrop<F6>,
    s7: ManuallyDrop<F7>,
    s8: ManuallyDrop<F8>,
}

stage_machine! {
    /// Future for eight adjacent infallible pipeline links.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    ThenOctFuture,
    state: State8,
    slot: Slot8,
    call: AsyncStep::call,
    run: AsyncChain::run,
    mode: plain,
    error: [],
    tail: [tail.tail.tail.tail.tail.tail.tail.tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [tail.tail.tail.tail.tail.tail.tail.head], F1),
        (Step1, s1) -> (Step2, s2, [tail.tail.tail.tail.tail.tail.head], F2),
        (Step2, s2) -> (Step3, s3, [tail.tail.tail.tail.tail.head], F3),
        (Step3, s3) -> (Step4, s4, [tail.tail.tail.tail.head], F4),
        (Step4, s4) -> (Step5, s5, [tail.tail.tail.head], F5),
        (Step5, s5) -> (Step6, s6, [tail.tail.head], F6),
        (Step6, s6) -> (Step7, s7, [tail.head], F7),
        (Step7, s7) -> (Step8, s8, [head], F8)
    ],
    last: (Step8, s8),
    generics: ['a, S1, S2, S3, S4, S5, S6, S7, S8, Rest, Input, TailFuture, F1, F2, F3, F4, F5, F6, F7, F8],
    pipeline: [AsyncPipe<S8, AsyncPipe<S7, AsyncPipe<S6, AsyncPipe<S5, AsyncPipe<S4, AsyncPipe<S3, AsyncPipe<S2, AsyncPipe<S1, Rest>>>>>>>>],
    bounds: {
        Rest: AsyncChain<Input, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = ChainOutput<Rest, Input>>,
        S1: AsyncStep<ChainOutput<Rest, Input>, Future<'a> = F1> + 'a,
        F1: Future<Output = StepOutput<S1, ChainOutput<Rest, Input>>>,
        S2: AsyncStep<StepOutput<S1, ChainOutput<Rest, Input>>, Future<'a> = F2> + 'a,
        F2: Future<Output = StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>,
        S3: AsyncStep<StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>, Future<'a> = F3> + 'a,
        F3: Future<Output = StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>,
        S4: AsyncStep<StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>, Future<'a> = F4> + 'a,
        F4: Future<Output = StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>,
        S5: AsyncStep<StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>, Future<'a> = F5> + 'a,
        F5: Future<Output = StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>,
        S6: AsyncStep<StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>, Future<'a> = F6> + 'a,
        F6: Future<Output = StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>>,
        S7: AsyncStep<StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>, Future<'a> = F7> + 'a,
        F7: Future<Output = StepOutput<S7, StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>>>,
        S8: AsyncStep<StepOutput<S7, StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>>, Future<'a> = F8> + 'a,
        F8: Future<Output = StepOutput<S8, StepOutput<S7, StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>>>>,
    },
    output: StepOutput<S8, StepOutput<S7, StepOutput<S6, StepOutput<S5, StepOutput<S4, StepOutput<S3, StepOutput<S2, StepOutput<S1, ChainOutput<Rest, Input>>>>>>>>>,
}

stage_machine! {
    /// Future for eight adjacent fallible pipeline links.
    ///
    /// It borrows the sub-pipeline it drives as a single pointer and projects one
    /// stage out of it at a time, so the future stores one reference per group of
    /// links rather than one per stage. The tail chain's future is built on the
    /// first poll, not when the run future is created.
    TryThenOctFuture,
    state: State8,
    slot: Slot8,
    call: TryAsyncStep::call,
    run: TryAsyncChain::run,
    mode: fallible,
    error: [Error],
    tail: [tail.tail.tail.tail.tail.tail.tail.tail],
    steps: [
        (Tail, tail) -> (Step1, s1, [tail.tail.tail.tail.tail.tail.tail.head], F1),
        (Step1, s1) -> (Step2, s2, [tail.tail.tail.tail.tail.tail.head], F2),
        (Step2, s2) -> (Step3, s3, [tail.tail.tail.tail.tail.head], F3),
        (Step3, s3) -> (Step4, s4, [tail.tail.tail.tail.head], F4),
        (Step4, s4) -> (Step5, s5, [tail.tail.tail.head], F5),
        (Step5, s5) -> (Step6, s6, [tail.tail.head], F6),
        (Step6, s6) -> (Step7, s7, [tail.head], F7),
        (Step7, s7) -> (Step8, s8, [head], F8)
    ],
    last: (Step8, s8),
    generics: ['a, S1, S2, S3, S4, S5, S6, S7, S8, Rest, Input, TailFuture, F1, F2, F3, F4, F5, F6, F7, F8, Error],
    pipeline: [TryAsyncPipe<S8, TryAsyncPipe<S7, TryAsyncPipe<S6, TryAsyncPipe<S5, TryAsyncPipe<S4, TryAsyncPipe<S3, TryAsyncPipe<S2, TryAsyncPipe<S1, Rest>>>>>>>>],
    bounds: {
        Rest: TryAsyncChain<Input, Error, Future<'a> = TailFuture> + 'a,
        TailFuture: Future<Output = Result<TryChainOutput<Rest, Input, Error>, Error>>,
        S1: TryAsyncStep<TryChainOutput<Rest, Input, Error>, Error, Future<'a> = F1> + 'a,
        F1: Future<Output = Result<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>>,
        S2: TryAsyncStep<TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error, Future<'a> = F2> + 'a,
        F2: Future<Output = Result<TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>>,
        S3: TryAsyncStep<TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error, Future<'a> = F3> + 'a,
        F3: Future<Output = Result<TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>>,
        S4: TryAsyncStep<TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error, Future<'a> = F4> + 'a,
        F4: Future<Output = Result<TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>>,
        S5: TryAsyncStep<TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error, Future<'a> = F5> + 'a,
        F5: Future<Output = Result<TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>>,
        S6: TryAsyncStep<TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error, Future<'a> = F6> + 'a,
        F6: Future<Output = Result<TryStepOutput<S6, TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>>,
        S7: TryAsyncStep<TryStepOutput<S6, TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error, Future<'a> = F7> + 'a,
        F7: Future<Output = Result<TryStepOutput<S7, TryStepOutput<S6, TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>>,
        S8: TryAsyncStep<TryStepOutput<S7, TryStepOutput<S6, TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error, Future<'a> = F8> + 'a,
        F8: Future<Output = Result<TryStepOutput<S8, TryStepOutput<S7, TryStepOutput<S6, TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>>,
    },
    output: Result<TryStepOutput<S8, TryStepOutput<S7, TryStepOutput<S6, TryStepOutput<S5, TryStepOutput<S4, TryStepOutput<S3, TryStepOutput<S2, TryStepOutput<S1, TryChainOutput<Rest, Input, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>, Error>,
}
