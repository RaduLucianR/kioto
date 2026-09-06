use futures::task::noop_waker;
use std::future::Future;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::thread::sleep;
use std::time::Duration;

pub mod executor;
pub mod reactor;

pub struct KiotoRuntime {
    executor: executor::Executor,
}

impl KiotoRuntime {
    pub fn init() -> Self {
        reactor::start();
        Self {
            executor: executor::Executor::new(),
        }
    }

    pub fn block_on<F: Future>(&mut self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.executor.block_on(future)
        // let waker = noop_waker();
        // let mut context = Context::from_waker(&waker);
        // let mut pinned_future = pin!(future);

        // loop {
        //     println!("One runtime loop iteration!");
        //     match pinned_future.as_mut().poll(&mut context) {
        //         Poll::Pending => {
        //             println!("Busy! Do something else!");
        //         }
        //         Poll::Ready(_) => {
        //             println!("Future is ready! Exiting...");
        //             break;
        //         }
        //     }
        //     sleep(Duration::from_millis(1000));
        // }
    }
}
