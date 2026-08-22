use core::cell::Cell;

use skid_pipe::Pipe;

#[test]
fn evaluates_steps_from_left_to_right() {
    let order = Cell::new(0_u16);
    let first = |value: i32| {
        order.set(order.get() * 10 + 1);
        value + 1
    };
    let second = |value: i32| {
        order.set(order.get() * 10 + 2);
        value * 2
    };
    let third = |value: i32| {
        order.set(order.get() * 10 + 3);
        value - 3
    };
    let mut pipeline = Pipe::new(first).then(second).then(third);

    assert_eq!(pipeline.run(4), 7);
    assert_eq!(order.get(), 123);
}
