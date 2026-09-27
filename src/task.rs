use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::thread::Thread;

pub(crate) type Aborted = Arc<Mutex<HashSet<usize>>>;
pub(crate) type ReadyQueue = Arc<Mutex<Vec<usize>>>;

#[derive(Debug)]
pub struct JoinHandle<T = ()> {
    id: usize,
    aborted: Aborted,
    ready_queue: ReadyQueue,
    thread: Thread,
    _output: PhantomData<fn() -> T>,
}

impl<T> JoinHandle<T> {
    pub(crate) fn new(id: usize, aborted: Aborted, ready_queue: ReadyQueue, thread: Thread) -> Self {
        Self {
            id,
            aborted,
            ready_queue,
            thread,
            _output: PhantomData,
        }
    }

    pub fn abort(&self) {
        if let Ok(mut aborted) = self.aborted.lock() {
            aborted.insert(self.id);
        }

        if let Ok(mut queue) = self.ready_queue.lock() {
            queue.push(self.id);
        }
        self.thread.unpark();
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
pub struct AbortOnDropHandle<T = ()>(JoinHandle<T>);

impl<T> AbortOnDropHandle<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }

    pub fn abort(&self) {
        self.0.abort();
    }
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub struct JoinSet<T = ()> {
    handles: Vec<JoinHandle<T>>,
}

impl<T> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    pub fn abort_all(&self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinSet<()> {
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.push(crate::spawn(future));
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}
