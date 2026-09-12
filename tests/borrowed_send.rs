//! Send execution must only borrow state for the duration of a run.

use core::future::{Future, ready};
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use skid_pipe::{
    AsyncChainSend, AsyncPipe, AsyncStep, TryAsyncChainSend, TryAsyncPipe, TryAsyncStep,
};

mod common;
use common::poll_to_completion;

fn run_borrowed<'run, P>(
    pipeline: &'run mut P,
    input: u32,
) -> impl Future<Output = P::Output> + Send
where
    P: AsyncChainSend<'run, u32>,
{
    pipeline.run_send(input)
}

fn try_run_borrowed<'run, P>(
    pipeline: &'run mut P,
    input: u32,
) -> impl Future<Output = Result<P::Output, ()>> + Send
where
    P: TryAsyncChainSend<'run, u32, ()>,
{
    pipeline.run_send(input)
}

fn require_send<F: Future + Send>(future: F) -> F {
    future
}

#[test]
fn closures_can_borrow_local_state_across_repeated_send_runs() {
    let offset = 3_u32;
    let offset_ref = &offset;
    let mut pipeline = AsyncPipe::new(|value: u32| async move { value + offset_ref });
    let mut fallible = TryAsyncPipe::new(|value: u32| ready(Ok::<_, ()>(value + offset)));

    for value in [1, 2] {
        assert_eq!(
            poll_to_completion(run_borrowed(&mut pipeline, value)),
            value + offset
        );
        assert_eq!(
            poll_to_completion(try_run_borrowed(&mut fallible, value)),
            Ok(value + offset)
        );
    }
}

macro_rules! append_ten {
    ($pipeline:expr, $method:ident, $stage:expr) => {
        $pipeline
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
            .$method($stage)
    };
}

macro_rules! forty_one {
    ($pipeline:expr, $method:ident, $stage:expr) => {
        append_ten!(
            append_ten!(
                append_ten!(append_ten!($pipeline, $method, $stage), $method, $stage),
                $method,
                $stage
            ),
            $method,
            $stage
        )
    };
}

#[test]
fn borrowed_send_stages_cross_both_group_widths_and_short_circuit() {
    let offset = 1_u32;
    let stage = |value: u32| ready(value + offset);
    let mut pipeline = forty_one!(AsyncPipe::new(stage), then, stage);
    assert_eq!(poll_to_completion(require_send(pipeline.run_send(0))), 41);

    let calls = core::sync::atomic::AtomicU32::new(0);
    let stage = |value: u32| {
        calls.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        ready(if value == 20 {
            Err(value)
        } else {
            Ok(value + offset)
        })
    };
    let mut pipeline = forty_one!(TryAsyncPipe::new(stage), try_then, stage);
    assert_eq!(
        poll_to_completion(require_send(pipeline.run_send(0))),
        Err(20)
    );
    assert_eq!(calls.load(core::sync::atomic::Ordering::Relaxed), 21);
}

struct Counter<'state>(&'state mut u32);
struct CountFuture<'run> {
    count: &'run mut u32,
    yielded: bool,
}
impl Future for CountFuture<'_> {
    type Output = u32;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        *self.count += 1;
        Poll::Ready(*self.count)
    }
}
impl AsyncStep<()> for Counter<'_> {
    type Output = u32;
    type Future<'run>
        = CountFuture<'run>
    where
        Self: 'run;
    fn call(&mut self, (): ()) -> Self::Future<'_> {
        CountFuture {
            count: self.0,
            yielded: false,
        }
    }
}
struct TryCountFuture<'run>(CountFuture<'run>);
impl Future for TryCountFuture<'_> {
    type Output = Result<u32, ()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx).map(Ok)
    }
}
impl TryAsyncStep<(), ()> for Counter<'_> {
    type Output = u32;
    type Future<'run>
        = TryCountFuture<'run>
    where
        Self: 'run;
    fn call(&mut self, (): ()) -> Self::Future<'_> {
        TryCountFuture(CountFuture {
            count: self.0,
            yielded: false,
        })
    }
}

fn cancel_after_one_poll(future: impl Future + Send) {
    let mut future = pin!(future);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

#[test]
fn custom_stage_futures_borrow_mutable_state_and_release_it_on_cancellation() {
    let mut calls = 0;
    {
        let mut pipeline = AsyncPipe::new(Counter(&mut calls));
        cancel_after_one_poll(pipeline.run_send(()));
        assert_eq!(poll_to_completion(require_send(pipeline.run_send(()))), 1);
        assert_eq!(poll_to_completion(require_send(pipeline.run_send(()))), 2);
    }
    {
        let mut pipeline = TryAsyncPipe::new(Counter(&mut calls));
        cancel_after_one_poll(pipeline.run_send(()));
        assert_eq!(
            poll_to_completion(require_send(pipeline.run_send(()))),
            Ok(3)
        );
        assert_eq!(
            poll_to_completion(require_send(pipeline.run_send(()))),
            Ok(4)
        );
    }
    assert_eq!(calls, 4);
}

#[test]
fn send_run_can_return_a_borrowed_input_or_error() {
    async fn identity(input: &str) -> &str {
        input
    }
    async fn reject(input: &str) -> Result<(), &str> {
        Err(input)
    }
    let input = String::from("borrowed");
    let mut pipeline = AsyncPipe::new(identity);
    let mut fallible = TryAsyncPipe::new(reject);
    assert_eq!(
        poll_to_completion(require_send(pipeline.run_send(input.as_str()))),
        "borrowed"
    );
    assert_eq!(
        poll_to_completion(require_send(fallible.run_send(input.as_str()))),
        Err("borrowed")
    );
}
