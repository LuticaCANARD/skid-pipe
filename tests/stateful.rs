use skid_pipe::Pipe;

#[test]
fn fn_mut_stages_keep_state_between_runs() {
    let mut calls = 0_i32;
    let counter = move |value: i32| {
        calls += 1;
        value + calls
    };
    let mut pipeline = Pipe::new(counter).then(|value| value * 2);

    assert_eq!(pipeline.run(3), 8);
    assert_eq!(pipeline.run(3), 10);
}
