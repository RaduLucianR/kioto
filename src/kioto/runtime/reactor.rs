use mio::{Events, Interest, Poll, Registry, Token};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};
use std::{collections::HashMap, sync::OnceLock};

static REACTOR: OnceLock<Reactor> = OnceLock::new();

pub fn reactor() -> &'static Reactor {
    REACTOR
        .get()
        .expect("Called outside a Kioto runtime context")
}

pub struct Reactor {
    registry: Registry,
    wakers: Arc<Mutex<HashMap<usize, Waker>>>,
    next_id: AtomicUsize,
}

impl Reactor {
    pub fn register(
        &self,
        event_source: &mut impl mio::event::Source,
        interest: Interest,
        id: usize,
    ) {
        self.registry
            .register(event_source, Token(id), interest)
            .unwrap();
    }

    pub fn set_waker(&self, cx: &Context, id: usize) {
        let _ = self
            .wakers
            .lock()
            .map(|mut w| w.insert(id, cx.waker().clone()).is_none())
            .unwrap();
    }

    pub fn deregister(&self, event_source: &mut impl mio::event::Source, id: usize) {
        self.wakers
            .lock()
            .map(|mut waker_list| waker_list.remove(&id))
            .unwrap();
        self.registry.deregister(event_source).unwrap();
    }

    pub fn next_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn event_loop(mut poll: Poll, wakers: Arc<Mutex<HashMap<usize, Waker>>>) {
    let mut events = Events::with_capacity(100);
    loop {
        poll.poll(&mut events, None).unwrap();
        for e in events.iter() {
            let Token(id) = e.token();
            let wakers = wakers.lock().unwrap();

            if let Some(waker) = wakers.get(&id) {
                waker.wake_by_ref();
            }
        }
    }
}

pub fn start() {
    let wakers = Arc::new(Mutex::new(HashMap::new()));
    let poll = Poll::new().unwrap();
    let registry = poll.registry().try_clone().unwrap();
    let next_id = AtomicUsize::new(1);
    let reactor = Reactor {
        wakers: wakers.clone(),
        registry,
        next_id,
    };

    REACTOR.set(reactor).ok().expect("Reactor already running");
    std::thread::spawn(move || event_loop(poll, wakers));
}
