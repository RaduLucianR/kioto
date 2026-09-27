use mio::Interest;
use std::future::Future;
use std::io;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::task::Poll;

use crate::runtime::reactor::{self, Direction};
use crate::sync;
use crate::task::AbortOnDropHandle;
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
            reactor::reactor().register(stream, Interest::READABLE, id, "tcp stream");
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
                    match reactor::reactor().poll_ready(id, Direction::Read, cx) {
                        Poll::Ready(()) => continue,
                        Poll::Pending => break Poll::Pending,
                    }
                }

                Err(e) => panic!("{e:?}"),
            }
        }
    }
}

const MAX_DATAGRAM: usize = 65_536;

type Datagram = io::Result<(Vec<u8>, SocketAddr)>;

struct SendRequest {
    buf: Vec<u8>,
    target: SocketAddr,
    reply: sync::Sender<io::Result<usize>>,
}

pub struct UdpSocket {
    raw: Arc<RawUdpSocket>,
    datagrams: Mutex<sync::Receiver<Datagram>>,
    send_requests: sync::Sender<SendRequest>,
    _receive_task: AbortOnDropHandle,
    _send_task: AbortOnDropHandle,
}

impl UdpSocket {
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let raw = Arc::new(RawUdpSocket::bind(addr)?);
        let (datagram_tx, datagram_rx) = sync::channel();
        let (send_tx, send_rx) = sync::channel();

        let receive_task = crate::spawn(receive_loop(raw.clone(), datagram_tx));
        let send_task = crate::spawn(send_loop(raw.clone(), send_rx));

        tracing::info!(
            id = raw.id,
            local_addr = %raw.local_addr()?,
            fd = raw.mio_socket.as_raw_fd(),
            "kioto udp socket bound: its IO runs on kioto's executor and reactor"
        );

        Ok(Self {
            raw,
            datagrams: Mutex::new(datagram_rx),
            send_requests: send_tx,
            _receive_task: AbortOnDropHandle::new(receive_task),
            _send_task: AbortOnDropHandle::new(send_task),
        })
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.raw.mio_socket.set_broadcast(on)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.raw.local_addr()
    }

    pub fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        let mut datagrams = self.datagrams.lock().unwrap();
        match std::task::ready!(datagrams.poll_recv(cx)) {
            Some(Ok((data, addr))) => {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Poll::Ready(Ok((n, addr)))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => Poll::Ready(Err(io::Error::other("kioto udp receive task stopped"))),
        }
    }

    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let (reply, mut reply_rx) = sync::channel();
        let request = SendRequest {
            buf: buf.to_vec(),
            target,
            reply,
        };
        if self.send_requests.send(request).is_err() {
            return Err(io::Error::other("kioto udp send task stopped"));
        }
        reply_rx
            .recv()
            .await
            .unwrap_or_else(|| Err(io::Error::other("kioto udp send task stopped")))
    }
}

async fn receive_loop(socket: Arc<RawUdpSocket>, datagrams: sync::Sender<Datagram>) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let datagram = socket
            .recv_from(&mut buf)
            .await
            .map(|(n, addr)| (buf[..n].to_vec(), addr));
        if datagrams.send(datagram).is_err() {
            break;
        }
    }
}

async fn send_loop(socket: Arc<RawUdpSocket>, mut requests: sync::Receiver<SendRequest>) {
    while let Some(request) = requests.recv().await {
        let result = socket.send_to(&request.buf, request.target).await;
        let _ = request.reply.send(result);
    }
}

struct RawUdpSocket {
    mio_socket: mio::net::UdpSocket,
    id: usize,
}

impl RawUdpSocket {
    fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let std_socket = std::net::UdpSocket::bind(addr)?;
        std_socket.set_nonblocking(true)?;
        let mut mio_socket = mio::net::UdpSocket::from_std(std_socket);

        let local_addr = mio_socket.local_addr()?;
        let id = reactor::reactor().next_id();
        reactor::reactor().register(
            &mut mio_socket,
            Interest::READABLE | Interest::WRITABLE,
            id,
            format!("udp {local_addr}"),
        );
        Ok(Self { mio_socket, id })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.mio_socket.local_addr()
    }

    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> RecvFrom<'a> {
        RecvFrom { socket: self, buf }
    }

    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> SendTo<'a> {
        SendTo {
            socket: self,
            buf,
            target,
        }
    }
}

impl Drop for RawUdpSocket {
    fn drop(&mut self) {
        reactor::reactor().deregister(&mut self.mio_socket, self.id);
    }
}

struct RecvFrom<'a> {
    socket: &'a RawUdpSocket,
    buf: &'a mut [u8],
}

impl Future for RecvFrom<'_> {
    type Output = io::Result<(usize, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let id = this.socket.id;

        loop {
            match this.socket.mio_socket.recv_from(this.buf) {
                Ok((n, addr)) => {
                    reactor::reactor().record(id, Direction::Read, n);
                    tracing::trace!(id, n, %addr, "kioto udp recv_from");
                    return Poll::Ready(Ok((n, addr)));
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    match reactor::reactor().poll_ready(id, Direction::Read, cx) {
                        Poll::Ready(()) => continue,
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

struct SendTo<'a> {
    socket: &'a RawUdpSocket,
    buf: &'a [u8],
    target: SocketAddr,
}

impl Future for SendTo<'_> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let id = this.socket.id;

        loop {
            match this.socket.mio_socket.send_to(this.buf, this.target) {
                Ok(n) => {
                    reactor::reactor().record(id, Direction::Write, n);
                    tracing::trace!(id, n, target = %this.target, "kioto udp send_to");
                    return Poll::Ready(Ok(n));
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    match reactor::reactor().poll_ready(id, Direction::Write, cx) {
                        Poll::Ready(()) => continue,
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}
