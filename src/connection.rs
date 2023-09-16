use bytes::{BytesMut, Buf};
use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt, AsyncRead, AsyncWrite}};

use crate::protocol::MqttPacketV3;
use anyhow::Result;


// Represents MQTT connection
#[derive(Debug)]
pub struct Connection<T: AsyncRead + AsyncWrite + Unpin> {

    stream: T,

    // The buffer for reading packets
    buffer: BytesMut,

}

impl<T> Connection<T> 
where
    T: AsyncRead + AsyncWrite + Unpin
{

    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(4096),
        } 
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        //self.stream.shutdown().await?;
        self.stream.shutdown().await?;
        Ok(())
    }

    // Read a single packet from the underlying stream.
    pub async fn read_packet(&mut self) -> Result<MqttPacketV3, std::io::Error> {
        loop {
            let packet_result = crate::protocol::parse(&self.buffer);

            if let Ok((_, (consumed_bytes ,packet))) = packet_result {
                self.buffer.advance(consumed_bytes.len());
                return Ok(packet);
            }

            let n= self.stream.read_buf(&mut self.buffer).await?;

            if 0 == n {
                if self.buffer.is_empty() {

                } else {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "Connection closed"));
                }
            } 
        }
    }

    // Write a single packet to the underlying stream.
    pub async fn write_packet(&mut self, packet: &MqttPacketV3) -> Result<()>{
        self.stream.write(&packet.to_bytes()).await?;
        self.stream.flush().await?;
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut, Buf};
    use nom::AsBytes;
    use tokio::{io::{AsyncRead, AsyncWrite}, sync::mpsc::Receiver};
    use crate::protocol::{v3::{fixed_header::FixHeader, publish::{VariableHeader, Payload, PublishPacket, PublishPacketBuilder}}, PacketType, MqttPacketV3};
    use super::Connection;

    struct MockTcpStream {
        inner_buf: BytesMut,
    }

    impl MockTcpStream {
        fn write_bytes(&mut self, bytes: &[u8]) {
            self.inner_buf.put(bytes);
        }
    }

    impl AsyncRead for MockTcpStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if !self.inner_buf.is_empty() {
                buf.put(self.inner_buf.as_bytes());
                std::task::Poll::Ready(Ok(()))
            } else {
                std::task::Poll::Pending
            }
        }
    }

    impl AsyncWrite for MockTcpStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            unsafe{
                self.get_unchecked_mut().inner_buf.put(buf);
            }
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl Unpin for MockTcpStream {
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_read_packet_from_stream() {
        let mut connection = Connection::new(MockTcpStream { inner_buf: BytesMut::new() }); 

        let publish_packet = PublishPacketBuilder::new("a/b/c".to_string(),vec![0x01]).build();

        let packet = MqttPacketV3::Publish(publish_packet);
        connection.stream.write_bytes(packet.to_bytes().as_ref());
        let packet = connection.read_packet().await;
        if let Ok(packet) = packet {
            if let MqttPacketV3::Publish(publish_packet) = packet {
                assert_eq!(publish_packet.variable_header.topic_name, "a/b/c"); 
            } else {
                assert!(false)
            }
        } else {
            assert!(false)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_write_packet() {
        let mut connection = Connection::new(MockTcpStream { inner_buf: BytesMut::new() }); 
        let publish_packet = PublishPacketBuilder::new("a/b".to_string(),vec![0x01]).dup(true).retain(true).qos(1).packet_identifier(0x10).build();

        let packet = MqttPacketV3::Publish(publish_packet);

        let r = connection.write_packet(&packet).await;
        assert_eq!(true, r.is_ok());

        assert_eq!(connection.stream.inner_buf.as_bytes(), &[0x3B,0x08,0x00,0x03,0x61,0x2F,0x62,0x00,0x10,0x01]);

    }

}