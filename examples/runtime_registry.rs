#[cfg(feature = "dynamic")]
use skid_pipe::RuntimePipe;

#[cfg(feature = "dynamic")]
#[derive(Debug, PartialEq)]
enum Value {
    Raw(u8),
    Decoded(u16),
    Classified(bool),
}

#[cfg(feature = "dynamic")]
#[derive(Debug, PartialEq)]
enum Error {
    UnknownStep,
    UnexpectedValue,
}

#[cfg(feature = "dynamic")]
fn main() -> Result<(), Error> {
    let configured_steps = ["decode", "classify"];
    let mut pipeline = RuntimePipe::<Value, Error>::new();

    for name in configured_steps {
        match name {
            "decode" => {
                pipeline.push(|value| match value {
                    Value::Raw(raw) => Ok(Value::Decoded(u16::from(raw))),
                    _ => Err(Error::UnexpectedValue),
                });
            }
            "classify" => {
                pipeline.push(|value| match value {
                    Value::Decoded(value) => {
                        Ok(Value::Classified(value > 10))
                    }
                    _ => Err(Error::UnexpectedValue),
                });
            }
            _ => return Err(Error::UnknownStep),
        }
    }

    assert_eq!(
        pipeline.run(Value::Raw(12)),
        Ok(Value::Classified(true)),
    );

    Ok(())
}

#[cfg(not(feature = "dynamic"))]
fn main() {
    eprintln!("run with --features dynamic");
}
