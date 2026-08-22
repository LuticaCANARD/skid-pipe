#![no_std]

use skid_pipe::{AsyncPipe, Pipe};

pub fn normalize_and_classify(sample: u16) -> bool {
    let mut pipeline = Pipe::new(|value: u16| value as f32 / 4095.0)
        .then(|value| value > 0.5);

    pipeline.run(sample)
}

pub async fn increment_then_double(sample: u16) -> u16 {
    let mut pipeline = AsyncPipe::new(|value| core::future::ready(value + 1))
        .then(|value| core::future::ready(value * 2));

    pipeline.run(sample).await
}
