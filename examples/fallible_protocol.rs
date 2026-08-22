use skid_pipe::{TryChain, TryPipe};

#[derive(Debug, PartialEq)]
enum DecodeError {
    EmptyPayload,
    UnsupportedVersion,
}

#[derive(Debug, PartialEq)]
struct Frame {
    version: u8,
    payload: u16,
}

fn decode(bytes: [u8; 3]) -> Result<Frame, DecodeError> {
    if bytes[1] == 0 && bytes[2] == 0 {
        return Err(DecodeError::EmptyPayload);
    }

    Ok(Frame {
        version: bytes[0],
        payload: u16::from_be_bytes([bytes[1], bytes[2]]),
    })
}

fn validate(frame: Frame) -> Result<Frame, DecodeError> {
    if frame.version == 1 {
        Ok(frame)
    } else {
        Err(DecodeError::UnsupportedVersion)
    }
}

fn classify(frame: Frame) -> Result<&'static str, DecodeError> {
    Ok(if frame.payload > 100 {
        "high"
    } else {
        "normal"
    })
}

fn protocol_pipeline() -> impl TryChain<[u8; 3], DecodeError, Output = &'static str> {
    TryPipe::new(decode).try_then(validate).try_then(classify)
}

fn main() {
    let mut pipeline = protocol_pipeline();

    assert_eq!(pipeline.run([1, 0, 120]), Ok("high"));
    assert_eq!(
        pipeline.run([2, 0, 120]),
        Err(DecodeError::UnsupportedVersion),
    );
    assert_eq!(pipeline.run([1, 0, 0]), Err(DecodeError::EmptyPayload),);
}
