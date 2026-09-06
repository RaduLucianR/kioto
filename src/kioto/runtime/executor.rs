use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake},
    thread::{self, Thread},
    time::Duration,
};

type Task = Pin<Box<dyn Future<Output = ()>>>;

thread_local! {
    static CURRENT_EXECUTOR: ExecutorInfo = ExecutorInfo::default();
}

#[derive(Default)]
pub struct ExecutorInfo {
    tasks: RefCell<HashMap<usize, Task>>, // HashMap<future_id, Task>
    ready_queue: Arc<Mutex<Vec<usize>>>,
    next_id: Cell<usize>,
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    CURRENT_EXECUTOR.with(|executor| {
        let id = executor.next_id.get();
        executor.tasks.borrow_mut().insert(id, Box::pin(future));
        executor
            .ready_queue
            .lock()
            .map(|mut queue| queue.push(id))
            .unwrap();
        executor.next_id.set(id + 1);
    });
}

pub struct Executor;
impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn block_on<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        spawn(future);

        loop {
            while let Some(future_id) = self.pop_ready() {
                // We extract the future from the hashmap
                let mut top_level_future = match self.get_future(future_id) {
                    None => {
                        println!(
                            "Executor tried to get ready future but the keyed entry is empty!"
                        );
                        continue;
                    }
                    Some(top_level_future) => top_level_future,
                };

                let future_waker = self.get_waker(future_id).into();
                let mut async_context = Context::from_waker(&future_waker);

                match top_level_future.as_mut().poll(&mut async_context) {
                    Poll::Pending => self.insert_task(future_id, top_level_future),
                    Poll::Ready(_) => continue, // Nothing to do, we completed the future
                };
            }

            let remaining_tasks = self.task_count();
            if remaining_tasks > 0 {
                println!(
                    "Executor: {} pending top-level futures (tasks). Waiting...",
                    remaining_tasks
                );
                thread::sleep(Duration::from_millis(1000));
            } else {
                println!("All tasks completed! Exiting...");
                break;
            }
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

    fn get_future(&self, id: usize) -> Option<Task> {
        CURRENT_EXECUTOR.with(|q| q.tasks.borrow_mut().remove(&id))
    }

    fn get_waker(&self, id: usize) -> Arc<MyWaker> {
        Arc::new(MyWaker {
            id,
            thread: thread::current(),
            ready_queue: CURRENT_EXECUTOR.with(|exec_info| exec_info.ready_queue.clone()),
        })
    }

    fn insert_task(&self, id: usize, task: Task) {
        CURRENT_EXECUTOR.with(|exec_info| exec_info.tasks.borrow_mut().insert(id, task));
    }

    fn task_count(&self) -> usize {
        CURRENT_EXECUTOR.with(|exec_info| exec_info.tasks.borrow().len())
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
