use core::future::Future;
use core::marker::PhantomData;
use std::rc::Rc;

use skid_pipe::{AsyncPipe, TryAsyncPipe};

// This keeps the core contract checked without enabling the optional Tokio
// integration feature. `tokio_feature.rs` verifies the real runtime adapter.
fn accepts_tokio_spawn<F>(future: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    drop(future);
}

// This is the corresponding boundary of `LocalSet::spawn_local`, which does
// not require `Send` but still owns a `'static` task future.
fn accepts_tokio_spawn_local<F>(future: F)
where
    F: Future + 'static,
    F::Output: 'static,
{
    drop(future);
}

// `static_assertions` is not a dependency, so `Send` is probed with the stable
// autoref-specialisation trick: with two levels of reference the `&SendProbe<T>`
// impl is tried first and only applies when `T: Send`; otherwise resolution
// falls through to the unconditional `SendProbe<T>` impl.
//
// Two structural details are load bearing. The probe must expand at the call
// site, because in a generic helper `T` is never known to be `Send`. And the
// probed future must come from behind a `-> impl Future` boundary: for an
// `async` block written in the same body the auto trait is still unresolved
// while methods are being picked, which makes the probe a hard error instead of
// a `false`.
struct SendProbe<T>(PhantomData<T>);

impl<T> SendProbe<T> {
    fn of(_value: &T) -> Self {
        Self(PhantomData)
    }
}

trait ProbeSend {
    fn is_send(&self) -> bool {
        true
    }
}

impl<T: Send> ProbeSend for &SendProbe<T> {}

trait ProbeNotSend {
    fn is_send(&self) -> bool {
        false
    }
}

impl<T> ProbeNotSend for SendProbe<T> {}

macro_rules! is_send {
    ($value:expr) => {{
        let probe = SendProbe::of(&$value);
        let probe = &&probe;
        probe.is_send()
    }};
}

fn ready_increment(value: u16) -> core::future::Ready<u16> {
    core::future::ready(value + 1)
}

fn try_ready_increment(value: u16) -> core::future::Ready<Result<u16, ()>> {
    core::future::ready(Ok(value + 1))
}

fn async_pipeline_task() -> impl Future<Output = u16> + 'static {
    let mut pipeline = AsyncPipe::new(ready_increment)
        .then(ready_increment)
        .then(ready_increment);

    async move { pipeline.run(0).await }
}

fn try_async_pipeline_task() -> impl Future<Output = Result<u16, ()>> + 'static {
    let mut pipeline = TryAsyncPipe::new(try_ready_increment)
        .try_then(try_ready_increment)
        .try_then(try_ready_increment);

    async move { pipeline.run(0).await }
}

// The captured `Rc` is what keeps this task off the `Send` path.
fn non_send_pipeline_task() -> impl Future<Output = u16> + 'static {
    let offset = Rc::new(1_u16);
    let mut pipeline = AsyncPipe::new(move |value: u16| {
        let offset = Rc::clone(&offset);
        async move { value + *offset }
    });

    async move { pipeline.run(4).await }
}

#[test]
fn an_owned_async_pipeline_is_tokio_spawn_compatible() {
    let task = async_pipeline_task();
    assert!(is_send!(task));

    accepts_tokio_spawn(task);
}

#[test]
fn an_owned_try_async_pipeline_is_tokio_spawn_compatible() {
    let task = try_async_pipeline_task();
    assert!(is_send!(task));

    accepts_tokio_spawn(task);
}

#[test]
fn a_non_send_pipeline_is_tokio_local_set_compatible() {
    let task = non_send_pipeline_task();
    assert!(
        !is_send!(task),
        "the `Rc`-capturing pipeline must stay non-`Send`; `spawn_local` \
         compatibility only means something for a task that is not `Send`"
    );

    accepts_tokio_spawn_local(task);
}
