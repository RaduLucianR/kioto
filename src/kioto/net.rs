use mio::Interest;
use std::future::Future;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::task::Context;
use std::task::Poll;

use crate::kioto::runtime::reactor;
use std::io::ErrorKind;
use std::io::Write;

fn get_req(path: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Connection: close\r\n\
             \r\n"
    )
}

pub struct KiotoTcpStream {
    is_first_poll: bool,
    mio_tcp_stream: mio::net::TcpStream,
    buffer: Vec<u8>,
    id: usize,
}

impl KiotoTcpStream {
    pub fn new(stream: mio::net::TcpStream) -> Self {
        Self {
            is_first_poll: true,
            mio_tcp_stream: stream,
            buffer: vec![],
            id: reactor::reactor().next_id(),
        }
    }

    fn write_request(&mut self) {
        self.mio_tcp_stream
            .write_all(get_req("/5000/HelloAsyncAwait").as_bytes())
            .unwrap();
        println!("Write request sent!");
    }

    pub fn read() -> impl Future<Output = String> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let stream = mio::net::TcpStream::connect(addr).unwrap();
        KiotoTcpStream::new(stream)
    }
}

impl Future for KiotoTcpStream {
    type Output = String;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_first_poll {
            self.is_first_poll = false;
            let id = self.id;
            let stream = &mut self.mio_tcp_stream;
            reactor::reactor().register(stream, Interest::READABLE, id);
            reactor::reactor().set_waker(cx, self.id);
            self.write_request();
        }

        let id = self.id;
        let mut buff = vec![0u8; 147];
        loop {
            match self.mio_tcp_stream.read(&mut buff) {
                Ok(0) => {
                    let s = String::from_utf8_lossy(&self.buffer).to_string();
                    reactor::reactor().deregister(&mut self.mio_tcp_stream, id);
                    break Poll::Ready(s);
                }
                Ok(n) => {
                    self.buffer.extend(&buff[0..n]);
                    continue;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    reactor::reactor().set_waker(cx, self.id);
                    break Poll::Pending;
                }

                Err(e) => panic!("{e:?}"),
            }
        }
    }
}
