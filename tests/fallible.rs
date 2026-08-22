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
