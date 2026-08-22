use core::cell::Cell;

use skid_pipe::TryPipe;

#[derive(Debug, PartialEq)]
struct Frame(u16);

#[derive(Debug, PartialEq)]
struct Score(u16);

#[derive(Debug, PartialEq)]
enum PipelineError {
    Empty,
    BelowThreshold,
}

fn decode(raw: u8) -> Result<Frame, PipelineError> {
    if raw == 0 {
        Err(PipelineError::Empty)
    } else {
        Ok(Frame(u16::from(raw)))
    }
}

fn score(frame: Frame) -> Result<Score, PipelineError> {
    Ok(Score(frame.0 * 2))
}

fn classify(score: Score) -> Result<bool, PipelineError> {
    if score.0 > 20 {
        Ok(true)
    } else {
        Err(PipelineError::BelowThreshold)
    }
}

#[test]
fn composes_fallible_steps_with_type_changes() {
    let mut pipeline = TryPipe::new(decode).try_then(score).try_then(classify);

    assert_eq!(pipeline.run(16), Ok(true));
    assert_eq!(pipeline.run(0), Err(PipelineError::Empty));
}

#[test]
fn stops_after_the_first_error() {
    let first_calls = Cell::new(0_u8);
    let rejected_calls = Cell::new(0_u8);
    let skipped_calls = Cell::new(0_u8);
    let first = |value: u8| {
        first_calls.set(first_calls.get() + 1);
        Ok::<_, &'static str>(value + 1)
    };
    let reject = |_: u8| {
        rejected_calls.set(rejected_calls.get() + 1);
        Err::<u8, _>("rejected")
    };
    let never = |value: u8| {
        skipped_calls.set(skipped_calls.get() + 1);
        Ok::<_, &'static str>(value)
    };
    let mut pipeline = TryPipe::new(first).try_then(reject).try_then(never);

    assert_eq!(pipeline.run(4), Err("rejected"));
    assert_eq!(first_calls.get(), 1);
    assert_eq!(rejected_calls.get(), 1);
    assert_eq!(skipped_calls.get(), 0);
}

#[test]
fn stops_at_first_middle_and_last_errors() {
    for fail_at in 0_u8..3 {
        let first_calls = Cell::new(0_u8);
        let middle_calls = Cell::new(0_u8);
        let last_calls = Cell::new(0_u8);

        let first = |value: u8| {
            first_calls.set(first_calls.get() + 1);
            if fail_at == 0 {
                Err(fail_at)
            } else {
                Ok(value + 1)
            }
        };
        let middle = |value: u8| {
            middle_calls.set(middle_calls.get() + 1);
            if fail_at == 1 {
                Err(fail_at)
            } else {
                Ok(value + 1)
            }
        };
        let last = |value: u8| {
            last_calls.set(last_calls.get() + 1);
            if fail_at == 2 {
                Err(fail_at)
            } else {
                Ok(value + 1)
            }
        };
        let mut pipeline = TryPipe::new(first).try_then(middle).try_then(last);

        assert_eq!(pipeline.run(4), Err(fail_at));
        assert_eq!(first_calls.get(), 1);
        assert_eq!(middle_calls.get(), u8::from(fail_at >= 1));
        assert_eq!(last_calls.get(), u8::from(fail_at >= 2));
    }
}

#[test]
fn stateful_steps_keep_state_between_completed_runs() {
    let mut calls = 0_u8;
    let count = move |value: u8| {
        calls += 1;
        Ok::<_, ()>(value + calls)
    };
    let mut pipeline = TryPipe::new(count).try_then(|value| Ok::<_, ()>(value * 2));

    assert_eq!(pipeline.run(10), Ok(22));
    assert_eq!(pipeline.run(10), Ok(24));
}

#[test]
fn zero_sized_first_stage_keeps_the_pipeline_zero_sized() {
    let pipeline = TryPipe::new(decode);

    assert_eq!(core::mem::size_of_val(&pipeline), 0);
}
