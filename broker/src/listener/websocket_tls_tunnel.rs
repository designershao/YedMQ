use std::{pin::Pin, task::{Context, Poll}, io::Error};

use bytes::Bytes;
use futures::{Stream, Sink};
use tokio::{net::TcpStream, io::{AsyncRead, AsyncWrite, ReadBuf, AsyncBufRead}};
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub struct WebsocketTlsTunnel {
    pub inner: StreamReader<StreamWrapper, Bytes>,
}

#[derive(Debug)]
pub struct StreamWrapper {
    pub inner: WebSocketStream<TlsStream<TcpStream>>,
}

impl Stream for StreamWrapper {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.get_mut().inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(res))) => {
                if let Message::Binary(b) = res {
                    Poll::Ready(Some(Ok(Bytes::from(b))))
                } else {
                    Poll::Ready(Some(Err(Error::other("invalid message"))))
                }
            }
            Poll::Ready(Some(Err(err))) => {
                Poll::Ready(Some(Err(Error::other(err))))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl AsyncRead for WebsocketTlsTunnel {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncBufRead for WebsocketTlsTunnel {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.get_mut().inner).consume(amt)
    }
}

impl AsyncWrite for WebsocketTlsTunnel {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match Pin::new(&mut self.get_mut().inner.get_mut().inner).start_send(Message::Binary(buf.to_vec())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(Error::other(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.get_mut().inner.get_mut().inner)
            .poll_flush(cx)
            .map_err(Error::other)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.get_mut().inner.get_mut().inner)
            .poll_close(cx)
            .map_err(Error::other)
    }
}