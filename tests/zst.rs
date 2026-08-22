use core::mem::size_of_val;

use skid_pipe::Pipe;

fn increment(value: u8) -> u8 {
    value + 1
}

fn double(value: u8) -> u8 {
    value * 2
}

#[test]
fn function_item_pipeline_is_zero_sized() {
    let pipeline = Pipe::new(increment).then(double);

    assert_eq!(size_of_val(&pipeline), 0);
}
