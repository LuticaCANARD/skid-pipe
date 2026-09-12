use skid_pipe::{Chain, End, Pipe};

#[test]
fn opaque_modules_preserve_state_and_execute_once_in_order() {
    fn preprocessing() -> impl Chain<u8, Output = u16> {
        let mut calls = 0;
        Pipe::new(move |raw: u8| {
            calls += 1;
            u16::from(raw) + calls
        })
        .then(|value| value * 2)
    }
    fn classification() -> impl Chain<u16, Output = (u16, usize)> {
        let mut calls = 0;
        Pipe::new(move |value| {
            calls += 1;
            (value, calls)
        })
    }
    let mut pipeline = Pipe::from_chain(preprocessing())
        .then_chain(classification())
        .then(|(value, calls)| (value > 10, calls));
    assert_eq!(pipeline.run(4), (false, 1));
    assert_eq!(pipeline.run(4), (true, 2));
}

#[test]
fn nested_chains_can_borrow_local_state_and_non_copy_inputs() {
    let mut observed = Vec::new();
    {
        let suffix = String::from("!");
        let nested = Pipe::from_chain(Pipe::new(|mut text: String| {
            text.push_str(&suffix);
            text
        }))
        .then_chain(Pipe::new(|text: String| {
            observed.push(text.len());
            text
        }));
        let mut pipeline = Pipe::from_chain(End).then_chain(nested);
        assert_eq!(pipeline.run(String::from("sensor")), "sensor!");
        assert_eq!(pipeline.run(String::from("adc")), "adc!");
    }
    assert_eq!(observed, [7, 4]);
}

#[test]
fn composition_does_not_execute_stages_and_drops_owned_state_once() {
    use core::cell::Cell;
    struct Probe<'a>(&'a Cell<usize>);
    impl Drop for Probe<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    let drops = Cell::new(0);
    let calls = Cell::new(0);
    let call_count = &calls;
    let probe = Probe(&drops);
    let inner = Pipe::new(move |value: u8| {
        let _keep = &probe;
        call_count.set(call_count.get() + 1);
        value
    });
    let pipeline = Pipe::from_chain(inner).then_chain(End);
    assert_eq!(drops.get(), 0);
    assert_eq!(calls.get(), 0);
    drop(pipeline);
    assert_eq!(drops.get(), 1);
}

#[test]
fn externally_implemented_chains_are_supported() {
    struct Accumulator(u32);
    impl Chain<u16> for Accumulator {
        type Output = u32;
        fn run(&mut self, input: u16) -> u32 {
            self.0 += u32::from(input);
            self.0
        }
    }
    let mut pipeline =
        Pipe::from_chain(Accumulator(10)).then_chain(Pipe::new(|total: u32| total.to_string()));
    assert_eq!(pipeline.run(2), "12");
    assert_eq!(pipeline.run(3), "15");
}

#[test]
fn stateless_chain_adapters_remain_zero_sized() {
    let pipeline = Pipe::from_chain(Pipe::new(|n: u8| n + 1)).then_chain(Pipe::new(|n: u8| n * 2));
    assert_eq!(core::mem::size_of_val(&pipeline), 0);
}
