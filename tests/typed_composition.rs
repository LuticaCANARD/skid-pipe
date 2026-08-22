use skid_pipe::Pipe;

#[derive(Debug, PartialEq)]
struct Raw(u16);

#[derive(Debug, PartialEq)]
struct Frame(u16);

#[derive(Debug, PartialEq)]
struct Tensor(u16);

#[derive(Debug, PartialEq)]
enum Class {
    Bright,
    Dark,
}

fn decode(raw: Raw) -> Frame {
    Frame(raw.0)
}

fn normalize(frame: Frame) -> Tensor {
    Tensor(frame.0 / 2)
}

fn classify(tensor: Tensor) -> Class {
    if tensor.0 > 20 {
        Class::Bright
    } else {
        Class::Dark
    }
}

#[test]
fn composes_steps_with_different_input_and_output_types() {
    let mut pipeline = Pipe::new(decode).then(normalize).then(classify);

    assert_eq!(pipeline.run(Raw(64)), Class::Bright);
    assert_eq!(pipeline.run(Raw(8)), Class::Dark);
}
