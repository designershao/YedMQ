use bytes::{BytesMut, Buf};
use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt}};

use crate::protocol::MqttPacketV3;


// Represents MQTT connection
#[derive(Debug)]
pub struct Connection {

    stream: TcpStream,

    // The buffer for reading packets
    buffer: BytesMut,

}

impl Connection {

    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(4096),
        } 
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
    pub async fn write_packet(&mut self, packet: &MqttPacketV3) -> Result<(), std::io::Error>{
        self.stream.write(&packet.to_bytes()).await?;
        self.stream.flush().await
    }

}
