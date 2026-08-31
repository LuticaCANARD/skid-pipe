//! Ordering and short-circuit position across a group fold.
//!
//! A group's stages share one `async` block; a chain longer than the group
//! folds, and the `rest` arms run the shorter chain first and then the group's
//! own stages over its result. `preserves_order_across_multiple_quad_groups`
//! used to cover that with nine stages, back when a group was eight wide. It
//! no longer does — nine stages land in a single group now — and
//! `hundred_stages.rs` cannot take its place, because a hundred identical
//! `+1` stages sum the same in any order.
//!
//! These chains are long enough to fold at either width. Note what they can and
//! cannot add: the stage types already pin most of the fold, since S2 takes
//! S1's output and nothing else, so a reordered or repeated stage inside the
//! macro fails to compile rather than fails a test. I checked that by breaking
//! the `rest` arms on purpose — the crate stopped building. What is left for a
//! test is that a fold boundary is crossed at all, which nothing did after the
//! group widened to sixteen, and where a fallible chain stops.

use core::{cell::Cell, future::ready};

use skid_pipe::{AsyncPipe, TryAsyncPipe};

#[path = "common/mod.rs"]
mod common;

use common::poll_to_completion;

/// Every stage of a folded chain runs once, in order.
///
/// Each stage asserts how many predecessors it saw, and the counter catches a
/// group skipped or run twice. Forty stages folds under both widths: sixteen
/// over sixteen over eight, or thirty-two over eight.
#[test]
fn runs_a_folded_chain_in_order() {
    let seen = Cell::new(0_u32);
    let stage = |index: u32| {
        let seen = &seen;
        move |value: u32| {
            assert_eq!(
                value, index,
                "stage {index} ran after {value} predecessors, not {index}"
            );
            seen.set(seen.get() + 1);
            ready(value + 1)
        }
    };

    let pipeline = AsyncPipe::new(stage(0));
    let mut pipeline = append_thirty_nine!(pipeline, then, stage);

    assert_eq!(poll_to_completion(pipeline.run(0_u32)), 40);
    assert_eq!(seen.get(), 40);
}

/// A folded chain stops at the first error, wherever the fold puts it.
///
/// The failing stage sits inside a non-terminal group under both widths, so the
/// error has to cross a group boundary to reach the caller. This is the part
/// the stage types do not pin: they would accept a chain that ran the next
/// group anyway and discarded its result.
#[test]
fn stops_at_the_first_error_inside_a_folded_chain() {
    const FAILS_AT: u32 = 20;

    let ran = Cell::new(0_u32);
    let stage = |index: u32| {
        let ran = &ran;
        move |value: u32| {
            ran.set(ran.get() + 1);
            ready(if index == FAILS_AT {
                Err(index)
            } else {
                Ok(value + 1)
            })
        }
    };

    let pipeline = TryAsyncPipe::new(stage(0));
    let mut pipeline = append_thirty_nine!(pipeline, try_then, stage);

    assert_eq!(poll_to_completion(pipeline.run(0_u32)), Err(FAILS_AT));
    assert_eq!(
        ran.get(),
        FAILS_AT + 1,
        "stages after the failing one must not run"
    );
}

/// Appends stages 1 through 39, each carrying its own index.
macro_rules! append_thirty_nine {
    ($pipeline:expr, $method:ident, $stage:expr) => {{
        let mut index = 0_u32;
        let mut next = || {
            index += 1;
            index
        };
        $pipeline
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
            .$method($stage(next()))
    }};
}

use append_thirty_nine;
