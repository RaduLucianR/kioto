use super::Instant;
use crate::runtime::reactor;
use core::future::poll_fn;
use core::task::Context;
use std::task::Poll;
use std::time::Duration;

pub enum MissedTickBehavior {
    Delay,
}

pub struct Interval {
    deadline: Instant,
    period: Duration,
    behavior: MissedTickBehavior,
    id: usize,
}

impl Interval {
    pub async fn tick(&mut self) -> Instant {
        let instant_future = poll_fn(|cx| self.poll_tick(cx));
        instant_future.await
    }

    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        let now = Instant::now();

        if now >= self.deadline {
            self.deadline = now + self.period;
            return Poll::Ready(now);
        }

        reactor::reactor().register_timer(self.deadline, self.id, cx.waker().clone());
        Poll::Pending
    }

    /// Does nothing. It's here to mimic tokio's set_missed_tick_behavior()
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }
}

pub fn interval_at(start: Instant, period: Duration) -> Interval {
    let id = reactor::reactor().next_id();
    Interval {
        deadline: start,
        period,
        behavior: MissedTickBehavior::Delay,
        id,
    }
}

pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now(), period)
}
