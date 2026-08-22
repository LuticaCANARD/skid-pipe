use core::cell::Cell;

use skid_pipe::Pipe;

#[test]
fn runs_only_the_selected_branch_then_continues_the_pipeline() {
    let true_calls = Cell::new(0_u8);
    let false_calls = Cell::new(0_u8);
    let when_true = |value: i32| {
        true_calls.set(true_calls.get() + 1);
        value * 2
    };
    let when_false = |value: i32| {
        false_calls.set(false_calls.get() + 1);
        -value
    };
    let mut pipeline = Pipe::new(|value: i32| value)
        .then_branch(
            |value| *value >= 0,
            Pipe::new(when_true),
            Pipe::new(when_false),
        )
        .then(|value| value + 1);

    assert_eq!(pipeline.run(4), 9);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 0);

    assert_eq!(pipeline.run(-4), 5);
    assert_eq!(true_calls.get(), 1);
    assert_eq!(false_calls.get(), 1);
}
