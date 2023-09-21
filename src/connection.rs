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
    
    pub fn get_stream(&mut self) -> &mut T {
        &mut self.stream
    }

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
    use nom::AsBytes;
    use crate::protocol::{v3::{publish::{PublishPacketBuilder}}, MqttPacketV3};
    use super::Connection;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_read_packet_from_stream() {
        let publish_packet = PublishPacketBuilder::new("a/b/c".to_string(),vec![0x01]).build();

        let packet = MqttPacketV3::Publish(publish_packet);

        let mock_io = tokio_test::io::Builder::new().read(packet.to_bytes().as_bytes()).build();

        let mut connection = Connection::new(mock_io); 

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
        let publish_packet = PublishPacketBuilder::new("a/b".to_string(),vec![0x01]).dup(true).retain(true).qos(1).packet_identifier(0x10).build();

        let packet = MqttPacketV3::Publish(publish_packet);
        let mock_io = tokio_test::io::Builder::new()
            .write(&[0x3B,0x08,0x00,0x03,0x61,0x2F,0x62,0x00,0x10,0x01])
            .build();

        let mut connection = Connection::new(mock_io); 

        let r = connection.write_packet(&packet).await;
        assert_eq!(true, r.is_ok());
    }

}