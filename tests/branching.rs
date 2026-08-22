//! Branching is an ordinary `if`/`match` inside a stage. These tests pin the
//! properties a dedicated branch combinator used to provide: only the selected
//! arm runs, stage state survives across runs, and composition continues
//! normally afterwards.

use core::cell::Cell;

use skid_pipe::Pipe;

#[test]
fn runs_only_the_selected_arm_then_continues_the_pipeline() {
    let true_calls = Cell::new(0_u8);
    let false_calls = Cell::new(0_u8);

    let mut pipeline = Pipe::new(|value: i32| value)
        .then(|value: i32| {
            if value >= 0 {
                true_calls.set(true_calls.get() + 1);
                value * 2
            } else {
                false_calls.set(false_calls.get() + 1);
                -value
            }
        })
        .then(|value| value + 1);

    assert_eq!(pipeline.run(4), 9);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 0);

    assert_eq!(pipeline.run(-4), 5);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 1);
}

#[test]
fn match_dispatches_over_more_than_two_arms() {
    enum Grade {
        Low,
        Mid,
        High,
    }

    let mut pipeline = Pipe::new(|value: i32| {
        if value < 0 {
            Grade::Low
        } else if value < 100 {
            Grade::Mid
        } else {
            Grade::High
        }
    })
    .then(|grade: Grade| match grade {
        Grade::Low => "low",
        Grade::Mid => "mid",
        Grade::High => "high",
    });

    assert_eq!(pipeline.run(-1), "low");
    assert_eq!(pipeline.run(50), "mid");
    assert_eq!(pipeline.run(500), "high");
}

#[test]
fn a_branch_arm_can_run_a_stateful_sub_pipeline() {
    let mut hits = 0_i32;
    let mut counted = Pipe::new(move |value: i32| {
        hits += 1;
        value + hits
    });
    let mut negated = Pipe::new(|value: i32| -value);

    let mut pipeline = Pipe::new(|value: i32| value).then(move |value: i32| {
        if value >= 0 {
            counted.run(value)
        } else {
            negated.run(value)
        }
    });

    assert_eq!(pipeline.run(10), 11);
    assert_eq!(pipeline.run(10), 12);
    assert_eq!(pipeline.run(-10), 10);
    assert_eq!(pipeline.run(10), 13);
}
