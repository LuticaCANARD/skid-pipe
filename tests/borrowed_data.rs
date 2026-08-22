use skid_pipe::Pipe;

fn trim(value: &str) -> &str {
    value.trim()
}

fn length(value: &str) -> usize {
    value.len()
}

#[test]
fn composes_functions_that_borrow_their_input() {
    let mut pipeline = Pipe::new(trim).then(length);

    assert_eq!(pipeline.run("  pipe  "), 4);
}
