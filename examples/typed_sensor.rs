use skid_pipe::{Chain, Pipe};

#[derive(Debug, PartialEq)]
enum Reading {
    Normal,
    High,
}

fn sensor_pipeline() -> impl Chain<u16, Output = Reading> {
    Pipe::new(|raw: u16| raw.min(4095))
        .then(|raw: u16| raw as f32 / 4095.0)
        .then(|ratio: f32| {
            if ratio > 0.8 {
                Reading::High
            } else {
                Reading::Normal
            }
        })
}

fn main() {
    let mut pipeline = sensor_pipeline();

    assert_eq!(pipeline.run(500), Reading::Normal);
    assert_eq!(pipeline.run(4000), Reading::High);
}
