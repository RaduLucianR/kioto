use std::future::Future;

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
        F: Future<Output = ()> + Send + 'static,
    {
        self.executor.block_on(future)
    }
}
