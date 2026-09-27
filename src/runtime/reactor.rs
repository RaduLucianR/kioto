use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Once};
use std::os::fd::AsRawFd;
use std::task::{Context, Poll as TaskPoll, Waker};
use std::time::Duration;
use std::{collections::HashMap, sync::OnceLock};

use crate::time::Instant;

static REACTOR: OnceLock<Reactor> = OnceLock::new();

const WAKE_TOKEN: Token = Token(0);

const STATS_INTERVAL: Duration = Duration::from_secs(10);

type IoStates = Arc<Mutex<HashMap<usize, IoState>>>;
type Timers = Arc<Mutex<BTreeMap<(Instant, usize), Waker>>>;

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Read,
    Write,
}

#[derive(Default)]
struct Slot {
    ready: bool,
    waker: Option<Waker>,
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    ops: u64,
    bytes: u64,
    parked: u64,
    woken: u64,
}

#[derive(Default)]
struct IoState {
    label: String,
    polled_on: Option<String>,
    read: Slot,
    write: Slot,
}

impl IoState {
    fn slot(&mut self, direction: Direction) -> &mut Slot {
        match direction {
            Direction::Read => &mut self.read,
            Direction::Write => &mut self.write,
        }
    }
}

pub fn reactor() -> &'static Reactor {
    start();
    REACTOR.get().expect("kioto reactor failed to start")
}

pub struct Reactor {
    registry: Registry,
    io: IoStates,
    timers: Timers,
    waker: mio::Waker,
    next_id: AtomicUsize,
}

impl Reactor {
    pub fn register(
        &self,
        event_source: &mut impl mio::event::Source,
        interest: Interest,
        id: usize,
        label: impl Into<String>,
    ) {
        let label = label.into();
        self.registry
            .register(event_source, Token(id), interest)
            .unwrap();
        tracing::debug!(id, %label, ?interest, "io source registered");
        self.io.lock().unwrap().insert(
            id,
            IoState {
                label,
                ..IoState::default()
            },
        );
    }

    pub fn record(&self, id: usize, direction: Direction, bytes: usize) {
        if let Some(state) = self.io.lock().unwrap().get_mut(&id) {
            if state.polled_on.is_none() {
                state.polled_on = std::thread::current().name().map(str::to_owned);
            }
            let stats = &mut state.slot(direction).stats;
            stats.ops += 1;
            stats.bytes += bytes as u64;
        }
    }

    pub fn poll_ready(&self, id: usize, direction: Direction, cx: &Context) -> TaskPoll<()> {
        let mut io = self.io.lock().unwrap();
        let slot = io
            .get_mut(&id)
            .expect("polled an unregistered io source")
            .slot(direction);

        if slot.ready {
            slot.ready = false;
            tracing::trace!(id, ?direction, "io event already arrived, retrying");
            TaskPoll::Ready(())
        } else {
            slot.waker = Some(cx.waker().clone());
            slot.stats.parked += 1;
            tracing::trace!(id, ?direction, "would block, waker parked in kioto reactor");
            TaskPoll::Pending
        }
    }

    pub fn deregister(&self, event_source: &mut impl mio::event::Source, id: usize) {
        self.io.lock().unwrap().remove(&id);
        if let Err(e) = self.registry.deregister(event_source) {
            tracing::warn!(id, error = %e, "failed to deregister io source");
        }
        tracing::debug!(id, "io source deregistered");
    }

    pub fn register_timer(&self, deadline: Instant, id: usize, waker: Waker) {
        let mut timers = self.timers.lock().unwrap();
        let is_earliest = timers.keys().next().is_none_or(|(d, _)| deadline < *d);
        timers.insert((deadline, id), waker);
        let pending = timers.len();
        drop(timers);

        tracing::debug!(
            id,
            in_ms = deadline.saturating_duration_since(Instant::now()).as_millis(),
            pending,
            is_earliest,
            "timer registered"
        );

        if is_earliest {
            self.waker.wake().unwrap();
        }
    }

    pub fn next_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn event_loop(mut poll: Poll, io: IoStates, timers: Timers) {
    let mut events = Events::with_capacity(100);
    let mut next_stats = Instant::now() + STATS_INTERVAL;
    loop {
        let next_timer = timers.lock().unwrap().keys().next().map(|(deadline, _)| *deadline);
        let wake_at = next_timer.map_or(next_stats, |timer| timer.min(next_stats));
        let timeout = Some(wake_at.saturating_duration_since(Instant::now()));

        poll.poll(&mut events, timeout).unwrap();

        let mut io_wakers = Vec::new();
        {
            let mut io = io.lock().unwrap();
            for event in events.iter() {
                if event.token() == WAKE_TOKEN {
                    continue;
                }

                let Token(id) = event.token();
                let Some(state) = io.get_mut(&id) else {
                    continue;
                };

                let readable = event.is_readable() || event.is_read_closed() || event.is_error();
                let writable = event.is_writable() || event.is_write_closed() || event.is_error();

                for (is_ready, slot) in [(readable, &mut state.read), (writable, &mut state.write)] {
                    if !is_ready {
                        continue;
                    }
                    slot.ready = true;
                    if let Some(waker) = slot.waker.take() {
                        slot.stats.woken += 1;
                        io_wakers.push(waker);
                    }
                }

                tracing::trace!(id, readable, writable, "io event");
            }
        }
        let io_woken = io_wakers.len();

        let now = Instant::now();
        let mut expired = Vec::new();
        {
            let mut timers = timers.lock().unwrap();
            while let Some(entry) = timers.first_entry().filter(|e| e.key().0 <= now) {
                expired.push(entry.remove());
            }
        }

        if now >= next_stats {
            report_stats(&io);
            next_stats = now + STATS_INTERVAL;
        }

        if io_woken > 0 || !expired.is_empty() {
            tracing::trace!(
                timers_fired = expired.len(),
                io_woken,
                waited_ms = timeout.map(|t| t.as_millis()),
                "reactor wakeup"
            );
        }

        for waker in io_wakers.into_iter().chain(expired) {
            waker.wake();
        }
    }
}

fn report_stats(io: &IoStates) {
    let mut io = io.lock().unwrap();
    for (id, state) in io.iter_mut() {
        let (read, write) = (&state.read.stats, &state.write.stats);
        if read.ops + write.ops + read.parked + write.parked == 0 {
            continue;
        }
        tracing::info!(
            id,
            source = %state.label,
            polled_on = state.polled_on.as_deref().unwrap_or("<unnamed thread>"),
            received = read.ops,
            received_bytes = read.bytes,
            sent = write.ops,
            sent_bytes = write.bytes,
            parked = read.parked + write.parked,
            woken_by_kioto = read.woken + write.woken,
            "kioto reactor: io in the last {}s",
            STATS_INTERVAL.as_secs()
        );
        state.polled_on = None;
        state.read.stats = Stats::default();
        state.write.stats = Stats::default();
    }
}

pub fn start() {
    static STARTED: Once = Once::new();

    STARTED.call_once(|| {
        let io: IoStates = Arc::new(Mutex::new(HashMap::new()));
        let timers: Timers = Arc::new(Mutex::new(BTreeMap::new()));
        let poll = Poll::new().unwrap();
        let registry = poll.registry().try_clone().unwrap();
        let epoll_fds = [poll.as_raw_fd(), registry.as_raw_fd()];
        let waker = mio::Waker::new(poll.registry(), WAKE_TOKEN).unwrap();
        let next_id = AtomicUsize::new(1);
        let reactor = Reactor {
            io: io.clone(),
            timers: timers.clone(),
            registry,
            waker,
            next_id,
        };

        REACTOR
            .set(reactor)
            .ok()
            .expect("Reactor already running");
        tracing::info!(?epoll_fds, pid = std::process::id(), "kioto reactor starting");
        std::thread::Builder::new()
            .name("kioto-reactor".to_string())
            .spawn(move || event_loop(poll, io, timers))
            .expect("failed to start the kioto reactor thread");
    });
}
