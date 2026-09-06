mod kioto;

use std::future::Future;
use std::io;
use std::time::Duration;

use kioto::net::KiotoTcpStream;
use kioto::runtime::KiotoRuntime;

pub const TICK: Duration = Duration::from_secs(1);

async fn my_async_func() {
    println!("Hello from async fn!");
    let delayed_now = kioto::time::Instant::now() + TICK;
    println!("Now is: {:?}", delayed_now);
    KiotoTcpStream::read().await;
    // let start = kioto::time::Instant::now();
    // let mut interval = kioto::time::interval_at(start, Duration::from_millis(5000));
    // interval.tick().await;
    println!("async func completed!");
}

fn async_main() -> impl Future<Output = ()> {
    async {
        my_async_func().await;
    }
}

fn main() -> Result<(), io::Error> {
    let async_main = async_main();
    KiotoRuntime::init().block_on(async_main);
    Ok(())
}
