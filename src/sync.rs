use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    queue: VecDeque<T>,
    receiver_waker: Option<Waker>,
    senders: usize,
    receiver_alive: bool,
}

pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        queue: VecDeque::new(),
        receiver_waker: None,
        senders: 1,
        receiver_alive: true,
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), T> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.receiver_alive {
            return Err(value);
        }
        inner.queue.push_back(value);
        let waker = inner.receiver_waker.take();
        drop(inner);

        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.lock().unwrap().senders += 1;
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.senders -= 1;
        let waker = (inner.senders == 0)
            .then(|| inner.receiver_waker.take())
            .flatten();
        drop(inner);

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Receiver<T> {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(value) = inner.queue.pop_front() {
            return Poll::Ready(Some(value));
        }
        if inner.senders == 0 {
            return Poll::Ready(None);
        }
        inner.receiver_waker = Some(cx.waker().clone());
        Poll::Pending
    }

    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv { receiver: self }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.receiver_alive = false;
        inner.queue.clear();
    }
}

pub struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().receiver.poll_recv(cx)
    }
}
