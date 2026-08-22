#![cfg(feature = "tokio")]

use std::rc::Rc;

use skid_pipe::{AsyncPipe, TokioAsyncChainExt, TokioTryAsyncChainExt, TryAsyncPipe};

fn increment(value: u16) -> core::future::Ready<u16> {
    core::future::ready(value + 1)
}

fn try_increment(value: u16) -> core::future::Ready<Result<u16, &'static str>> {
    core::future::ready(Ok(value + 1))
}

#[test]
fn spawns_owned_async_and_try_async_pipelines() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime must build");

    let async_task = {
        let _guard = runtime.enter();
        AsyncPipe::new(increment).then(increment).spawn(4)
    };
    assert_eq!(runtime.block_on(async_task).expect("task must complete"), 6);

    let try_task = {
        let _guard = runtime.enter();
        TryAsyncPipe::new(try_increment)
            .try_then(try_increment)
            .spawn(4)
    };
    assert_eq!(
        runtime.block_on(try_task).expect("task must complete"),
        Ok(6)
    );
}

#[test]
fn spawns_a_non_send_pipeline_on_a_local_set() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime must build");
    let local = tokio::task::LocalSet::new();

    let (output, try_output) = runtime.block_on(local.run_until(async {
        let offset = Rc::new(1_u16);
        let pipeline = AsyncPipe::new(move |value: u16| {
            let offset = Rc::clone(&offset);
            async move { value + *offset }
        });
        let output = pipeline
            .spawn_local(4)
            .await
            .expect("local task must complete");

        let offset = Rc::new(2_u16);
        let try_pipeline = TryAsyncPipe::new(move |value: u16| {
            let offset = Rc::clone(&offset);
            async move { Ok::<_, &'static str>(value + *offset) }
        });
        let try_output = try_pipeline
            .spawn_local(4)
            .await
            .expect("local task must complete");

        (output, try_output)
    }));

    assert_eq!(output, 5);
    assert_eq!(try_output, Ok(6));
}
