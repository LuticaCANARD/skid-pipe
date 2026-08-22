//! Helpers shared by the integration tests.
//!
//! `tests/common/mod.rs` is a subdirectory module, so cargo does not build it
//! as its own test binary. Not every test binary uses every helper, hence the
//! crate-wide `dead_code` allowance.
#![allow(dead_code)]

use core::cell::Cell;
use core::future::Future;
use core::{
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};

pub fn poll_to_completion<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => continue,
        }
    }
}

pub struct YieldOnce<Output> {
    output: Option<Output>,
    yielded: bool,
}

pub struct PendingDropProbe<'a, Output> {
    output: Option<Output>,
    pending: bool,
    pending_drops: &'a Cell<u8>,
}

pub struct ReadyWithPanickingDrop<'a, Output> {
    pub output: Option<Output>,
    pub drops: &'a Cell<u8>,
}

impl<Output: Unpin> Future for ReadyWithPanickingDrop<'_, Output> {
    type Output = Output;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.output.take().expect("drop probe must complete once"))
    }
}

impl<Output> Drop for ReadyWithPanickingDrop<'_, Output> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("intentional future destructor panic");
    }
}

impl<'a, Output> PendingDropProbe<'a, Output> {
    pub fn new(output: Output, pending: bool, pending_drops: &'a Cell<u8>) -> Self {
        Self {
            output: Some(output),
            pending,
            pending_drops,
        }
    }
}

impl<Output: Unpin> Future for PendingDropProbe<'_, Output> {
    type Output = Output;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(self.output.take().expect("probe must complete once"))
        }
    }
}

impl<Output> Drop for PendingDropProbe<'_, Output> {
    fn drop(&mut self) {
        if self.pending {
            self.pending_drops.set(self.pending_drops.get() + 1);
        }
    }
}

impl<Output> YieldOnce<Output> {
    pub fn new(output: Output) -> Self {
        Self {
            output: Some(output),
            yielded: false,
        }
    }
}

impl<Output: Unpin> Future for YieldOnce<Output> {
    type Output = Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();

        if !this.yielded {
            this.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(this.output.take().expect("future must only complete once"))
        }
    }
}
