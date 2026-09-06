use super::Instant;
use crate::kioto::runtime::reactor;
use core::future::poll_fn;
use core::task::Context;
use mio::Interest;
use std::task::Poll;
use std::time::Duration;

pub enum MissedTickBehavior {
    Delay,
}

pub struct Interval {
    is_first_call: bool,
    start: Instant,
    period: Duration,
    behavior: MissedTickBehavior,
    id: usize,
}

impl Interval {
    /// Returns Poll::Ready when the time passed for a tick
    pub async fn tick(&mut self) -> Instant {
        let instant_future = poll_fn(|cx| self.poll_tick(cx));
        instant_future.await
    }

    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        let id = self.id;

        if self.is_first_call {
            self.is_first_call = false;
            reactor::reactor().set_waker(cx, self.id);
        }

        let now = Instant::now();

        if now > self.start + self.period {
            // reactor::reactor().deregister(&mut self.source, id);
            return Poll::Ready(now);
        }

        reactor::reactor().set_waker(cx, self.id);
        Poll::Pending
    }

    /// Does nothing. It's here to mimic tokio's set_missed_tick_behavior()
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }
}

pub fn interval_at(start: Instant, period: Duration) -> Interval {
    let id = reactor::reactor().next_id();
    // reactor::reactor().register(source, Interest::READABLE, id);
    Interval {
        is_first_call: true,
        start,
        period,
        behavior: MissedTickBehavior::Delay,
        id,
    }
}
