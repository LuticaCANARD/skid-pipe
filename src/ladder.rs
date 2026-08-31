//! The impl ladders, written once.
//!
//! Both pipelines compose the same way — arities up to the group width
//! terminate on [`End`](crate::End), longer chains fold a whole group over a
//! shorter chain — and the fallible one differs only in carrying an `Error`, a
//! `?` between stages, and a `Result` return. Rather than four copies of that
//! shape (plain and `Send`, times infallible and fallible), the pieces that
//! differ are threaded in as tokens and the shape lives here once.
//!
//! One pass accumulates everything an impl needs: `$cur` is the input type the
//! next stage sees, so the bounds fall out in order; `$fwd` keeps the stages
//! innermost-first for the type; `$body` collects the run body as statements.
//!
//! Two properties of the emitted body are load-bearing and were measured:
//!
//! - The stages are separate `let` statements. Written as one nested
//!   expression, every stage's future stays alive to the end of the statement
//!   and the run future grows with the group — 912 B against 120 B at width
//!   sixteen.
//! - They land in a single expansion, so one hygiene context. Recursing into
//!   nested blocks instead makes an early `?` unwind one scope per stage:
//!   36.663 ns against 21.755 ns on the 100-stage first error. That is why
//!   `this`, `input` and `carried` are threaded as metavariables — created
//!   once at an entry rule, they name the same tokens everywhere after.

/// Folds a stage list into the nested pipeline type it names.
macro_rules! ladder_ty {
    ($pipe:ident, $bottom:ty;) => { $bottom };
    ($pipe:ident, $bottom:ty; $s:ident $($rest:ident)*) => {
        ladder_ty!($pipe, $pipe<$s, $bottom>; $($rest)*)
    };
}

/// Walks `.tail` once per stage below the one being reached.
///
/// Names no pipeline type: it only walks the `tail` field both of them have.
macro_rules! ladder_at {
    ($this:expr;) => { $this };
    ($this:expr; $s:ident $($rest:ident)*) => { ladder_at!($this.tail; $($rest)*) };
}

/// `type Output` on the plain traits; nothing on the `Send` ones, which
/// inherit it.
macro_rules! ladder_assoc {
    (owned, $out:ty) => {
        type Output = $out;
    };
    (inherited, $out:ty) => {};
}

/// Writes the impl every accumulator arrives at.
macro_rules! ladder_emit {
    (
        [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*]
        [$($gen:tt)*] [$assoc:ident] [$($send:tt)*]
        [$out:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
        $this:ident $inp:ident $car:ident [$($body:tt)*]
    ) => {
        impl<$($fwd,)* $($extra,)* $($gen)*> $($chain)* for ladder_ty!($pipe, $bottom; $($fwd)*)
        where
            $($b)*
        {
            ladder_assoc!($assoc, $out);

            #[inline(always)]
            // Clippy asks for `async fn`; the two do not lay out the same. On
            // the 100-stage footprint example the `async fn` form measures
            // 320 B against this one's 216 B, so the lint is refused here.
            #[allow(clippy::manual_async_fn)]
            fn $method(&mut self, $inp: Input) -> impl ::core::future::Future<Output = $($ret)*> $($send)* {
                let $this = self;
                async move {
                    $($body)*
                    $car
                }
            }
        }
    };
}

/// Accumulates one impl of a plain chain trait.
///
/// `[$step $($eargs)*]` spells the step bound — `AsyncStep` alone, or
/// `TryAsyncStep , Error` — and `[$($q)*]` is the `?` a fallible chain puts
/// between stages.
macro_rules! ladder_impl {
    ([$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     end $($s:ident)+) => {
        ladder_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [Input] [] [] [] [End]
            this input carried [let carried = input;] $($s)+);
    };
    ([$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     rest [$($tailbound:tt)*] [$($tailout:tt)*] $($s:ident)+) => {
        ladder_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [$($tailout)*] [$($tailbound)*] [] [TailHead TailTail]
            [$pipe<TailHead, TailTail>]
            this input carried
            [let carried = ladder_at!(this; $($s)*).$method(input).await $($q)*;]
            $($s)+);
    };

    (@go [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
     $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        ladder_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [<$s as $step<$cur $($eargs)*>>::Output]
            [$($b)* $s: $step<$cur $($eargs)*>,]
            [$($fwd)* $s] [$($extra)*] [$bottom]
            $this $inp $car
            [$($body)* let $car = ladder_at!($this; $($rest)+).head.call($car).await $($q)*;]
            $($rest)+);
    };
    (@go [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
     $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        ladder_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [<$s as $step<$cur $($eargs)*>>::Output]
            [$($b)* $s: $step<$cur $($eargs)*>,]
            [$($fwd)* $s] [$($extra)*] [$bottom]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@go [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$out:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
     $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        ladder_emit!([$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*]
            [$out] [$($b)*] [$($fwd)*] [$($extra)*] [$bottom]
            $this $inp $car [$($body)*]);
    };
}

/// Accumulates one impl of a `Send` chain trait.
///
/// The same walk as [`ladder_impl`], with three bounds per stage instead of
/// one: the stage itself, the future it hands back, and the value it carries
/// must all be `Send` for the composed `async` block to be.
macro_rules! ladder_send_impl {
    ([$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$($sendgen:tt)*] end $($s:ident)+) => {
        ladder_send_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [Input] [Input: Send, $($sendgen)*] [] [] [End]
            this input carried [let carried = input;] $($s)+);
    };
    ([$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$($sendgen:tt)*] rest [$($tailbound:tt)*] [$($tailout:tt)*] $($s:ident)+) => {
        ladder_send_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [$($tailout)*] [Input: Send, $($sendgen)* $($tailbound)*] [] [TailHead TailTail]
            [$pipe<TailHead, TailTail>]
            this input carried
            [let carried = ladder_at!(this; $($s)*).$method(input).await $($q)*;]
            $($s)+);
    };

    (@go [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
     $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident $($rest:ident)+) => {
        ladder_send_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [<$s as $step<$cur $($eargs)*>>::Output]
            [$($b)* $s: $step<$cur $($eargs)*> + Send,
             for<'a> <$s as $step<$cur $($eargs)*>>::Future<'a>: Send,
             <$s as $step<$cur $($eargs)*>>::Output: Send,]
            [$($fwd)* $s] [$($extra)*] [$bottom]
            $this $inp $car
            [$($body)* let $car = ladder_at!($this; $($rest)+).head.call($car).await $($q)*;]
            $($rest)+);
    };
    (@go [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$cur:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
     $this:ident $inp:ident $car:ident [$($body:tt)*] $s:ident) => {
        ladder_send_impl!(@go [$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*] [$step $($eargs)*] [$($q)*]
            [<$s as $step<$cur $($eargs)*>>::Output]
            [$($b)* $s: $step<$cur $($eargs)*> + Send,
             for<'a> <$s as $step<$cur $($eargs)*>>::Future<'a>: Send,
             <$s as $step<$cur $($eargs)*>>::Output: Send,]
            [$($fwd)* $s] [$($extra)*] [$bottom]
            $this $inp $car
            [$($body)* let $car = $this.head.call($car).await;]);
    };
    (@go [$pipe:ident] [$($chain:tt)*] [$method:ident] [$($ret:tt)*] [$($gen:tt)*]
     [$($assoc:tt)*] [$($send:tt)*] [$step:ident $($eargs:tt)*] [$($q:tt)*]
     [$out:ty] [$($b:tt)*] [$($fwd:ident)*] [$($extra:ident)*] [$bottom:ty]
     $this:ident $inp:ident $car:ident [$($body:tt)*]) => {
        ladder_emit!([$pipe] [$($chain)*] [$method] [$($ret)*] [$($gen)*]
            [$($assoc)*] [$($send)*]
            [$out] [$($b)*] [$($fwd)*] [$($extra)*] [$bottom]
            $this $inp $car [$($body)*]);
    };
}
