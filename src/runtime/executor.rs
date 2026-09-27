use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, Once, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake},
    thread::{self, Thread},
    time::Duration,
};
use tracing;

use crate::runtime::reactor;
use crate::task::{Aborted, JoinHandle, ReadyQueue};

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

const PARK_TIMEOUT: Duration = Duration::from_millis(1000);

static CURRENT_EXECUTOR: ExecutorHandle = ExecutorHandle;
static EXECUTOR: OnceLock<ExecutorInfo> = OnceLock::new();
static WORKER: Once = Once::new();
static WORKER_HOOK: OnceLock<Box<dyn Fn() + Send + Sync>> = OnceLock::new();

pub fn on_worker_start<F>(hook: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let _ = WORKER_HOOK.set(Box::new(hook));
}

pub struct ExecutorHandle;

impl ExecutorHandle {
    fn with<R>(&self, f: impl FnOnce(&ExecutorInfo) -> R) -> R {
        let info = EXECUTOR.get_or_init(ExecutorInfo::default);
        WORKER.call_once(|| {
            reactor::start();
            thread::Builder::new()
                .name("kioto-worker".to_string())
                .spawn(worker_loop)
                .expect("failed to start the kioto worker thread");
        });
        f(info)
    }
}

fn worker_loop() {
    let info = EXECUTOR.get_or_init(ExecutorInfo::default);
    let _ = info.worker.set(thread::current());
    if let Some(hook) = WORKER_HOOK.get() {
        hook();
    }
    Executor::new().drive(false);
}

#[derive(Default)]
pub struct ExecutorInfo {
    tasks: Mutex<HashMap<usize, (Task, &'static str)>>, // HashMap<future_id, (Task, type name)>
    ready_queue: ReadyQueue,
    aborted: Aborted,
    next_id: AtomicUsize,
    worker: OnceLock<Thread>,
}

impl ExecutorInfo {
    fn worker(&self) -> Thread {
        self.worker.get().cloned().unwrap_or_else(thread::current)
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    CURRENT_EXECUTOR.with(|executor| {
        let id = executor.next_id.fetch_add(1, Ordering::Relaxed);
        let name = std::any::type_name::<F>();
        tracing::debug!(id, task = name, "spawned");
        executor
            .tasks
            .lock()
            .unwrap()
            .insert(id, (Box::pin(future), name));
        executor
            .ready_queue
            .lock()
            .map(|mut queue| queue.push(id))
            .unwrap();
        executor.worker().unpark();

        JoinHandle::new(
            id,
            executor.aborted.clone(),
            executor.ready_queue.clone(),
            executor.worker(),
        )
    })
}

pub struct Executor;
impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn block_on<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {   
        tracing::debug!("Spawning top-level future");
        spawn(future);
        self.drive(true);
    }

    pub fn drive(&mut self, exit_when_idle: bool) {
        let mut i = 0;
        loop {
            while let Some(future_id) = self.pop_ready() {
                if self.take_abort(future_id) {
                    self.drop_task(future_id);
                    continue;
                }

                // We extract the future from the hashmap
                let Some((mut top_level_future, name)) = self.get_future(future_id) else {
                    continue;
                };

                let future_waker = self.get_waker(future_id).into();
                let mut async_context = Context::from_waker(&future_waker);

                i += 1;
                if i % 1000 == 0 {
                    tracing::debug!(id = future_id, task = name, "poll");
                }

                match top_level_future.as_mut().poll(&mut async_context) {
                    Poll::Pending => self.insert_task(future_id, (top_level_future, name)),
                    Poll::Ready(_) => {
                        tracing::debug!(id = future_id, task = name, "completed");
                        continue;
                    }
                };
            }

            if exit_when_idle && self.task_count() == 0 {
                break;
            }

            thread::park_timeout(PARK_TIMEOUT);
        }
    }

    pub fn pop_ready(&self) -> Option<usize> {
        CURRENT_EXECUTOR.with(|exec_info| {
            exec_info
                .ready_queue
                .lock()
                .map(|mut queue| queue.pop())
                .unwrap_or_else(|_| None)
        })
    }

    fn take_abort(&self, id: usize) -> bool {
        CURRENT_EXECUTOR.with(|exec_info| {
            exec_info
                .aborted
                .lock()
                .map(|mut aborted| aborted.remove(&id))
                .unwrap_or(false)
        })
    }

    fn drop_task(&self, id: usize) {
        CURRENT_EXECUTOR.with(|exec_info| exec_info.tasks.lock().unwrap().remove(&id));
    }

    fn get_future(&self, id: usize) -> Option<(Task, &'static str)> {
        CURRENT_EXECUTOR.with(|q| q.tasks.lock().unwrap().remove(&id))
    }

    fn get_waker(&self, id: usize) -> Arc<MyWaker> {
        Arc::new(MyWaker {
            id,
            thread: CURRENT_EXECUTOR.with(|exec_info| exec_info.worker()),
            ready_queue: CURRENT_EXECUTOR.with(|exec_info| exec_info.ready_queue.clone()),
        })
    }

    fn insert_task(&self, id: usize, task: (Task, &'static str)) {
        CURRENT_EXECUTOR.with(|exec_info| exec_info.tasks.lock().unwrap().insert(id, task));
    }

    fn task_count(&self) -> usize {
        CURRENT_EXECUTOR.with(|exec_info| exec_info.tasks.lock().unwrap().len())
    }
}

#[derive(Clone)]
pub struct MyWaker {
    thread: Thread,
    id: usize,
    ready_queue: Arc<Mutex<Vec<usize>>>,
}

impl Wake for MyWaker {
    fn wake(self: Arc<Self>) {
        self.ready_queue
            .lock()
            .map(|mut queue| queue.push(self.id))
            .unwrap();
        self.thread.unpark();
    }
}