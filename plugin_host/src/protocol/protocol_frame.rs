use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use std::io;
use tokio_util::codec::{Decoder, Encoder};
use super::plugin_protocol::ProtocolMessage;


// Magic number for protocol frame
const MAGIC_NUMBER: u32 = 0x5514;
const PROTOCOL_VERSION: u8 = 0x01;
const FRAME_HEADER_SIZE: usize = 9; // 4 bytes magic + 1 byte version + 4 bytes length

/// Protocol frame for binary communication
#[derive(Debug, Clone)]
pub struct ProtocolFrame {
    pub magic_number: u32,
    pub version: u8,
    pub length: u32,
    pub payload: Bytes,
}

impl ProtocolFrame {
    pub fn new(payload: Bytes) -> Self {
        Self {
            magic_number: MAGIC_NUMBER,
            version: PROTOCOL_VERSION,
            length: payload.len() as u32,
            payload,
        }
    }

    pub fn from_message(message: &ProtocolMessage) -> Result<Self> {
        let payload = message.encode_to_vec();
        Ok(Self::new(Bytes::from(payload)))
    }

    pub fn to_message(&self) -> Result<ProtocolMessage> {
        ProtocolMessage::decode(&*self.payload)
            .context("Failed to decode protocol message from frame")
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic_number != MAGIC_NUMBER {
            return Err(anyhow::anyhow!(
                "Invalid magic number: expected {:#X}, got {:#X}",
                MAGIC_NUMBER,
                self.magic_number
            ));
        }

        if self.version != PROTOCOL_VERSION {
            return Err(anyhow::anyhow!(
                "Unsupported protocol version: expected {}, got {}",
                PROTOCOL_VERSION,
                self.version
            ));
        }

        if self.length != self.payload.len() as u32 {
            return Err(anyhow::anyhow!(
                "Payload length mismatch: header says {}, actual {}",
                self.length,
                self.payload.len()
            ));
        }

        Ok(())
    }

    pub fn total_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.payload.len()
    }
}

/// Tokio codec for encoding and decoding protocol frames
pub struct ProtocolFrameCodec {
    max_frame_size: usize,
}

impl ProtocolFrameCodec {
    pub fn new() -> Self {
        Self {
            max_frame_size: 4 * 1024 * 1024, // 4MB max frame size
        }
    }

    pub fn with_max_frame_size(max_size: usize) -> Self {
        Self {
            max_frame_size: max_size,
        }
    }
}

impl Default for ProtocolFrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for ProtocolFrameCodec {
    type Item = ProtocolFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least the header to proceed
        if src.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Read the header without consuming bytes (peek)
        let magic_number = (&src[0..4]).get_u32();
        let version = src[4];
        let length = (&src[5..9]).get_u32();

        // Validate magic number early
        if magic_number != MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid magic number: {:#X}", magic_number),
            ));
        }

        // Validate version
        if version != PROTOCOL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported protocol version: {}", version),
            ));
        }

        // Check frame size limits
        if length as usize > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Frame too large: {} bytes (max: {} bytes)",
                    length, self.max_frame_size
                ),
            ));
        }

        let total_frame_size = FRAME_HEADER_SIZE + length as usize;

        // Check if we have the complete frame
        if src.len() < total_frame_size {
            // Reserve space for the complete frame to avoid multiple allocations
            src.reserve(total_frame_size - src.len());
            return Ok(None);
        }

        // Now we can consume the bytes since we have a complete frame
        let magic_number = src.get_u32();
        let version = src.get_u8();
        let length = src.get_u32();
        let payload = src.split_to(length as usize).freeze();

        let frame = ProtocolFrame {
            magic_number,
            version,
            length,
            payload,
        };

        Ok(Some(frame))
    }
}

impl Encoder<ProtocolFrame> for ProtocolFrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ProtocolFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Validate frame size
        if item.payload.len() > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Frame too large: {} bytes (max: {} bytes)",
                    item.payload.len(),
                    self.max_frame_size
                ),
            ));
        }

        // Ensure we have enough space
        dst.reserve(item.total_size());

        // Write frame header
        dst.put_u32(item.magic_number);
        dst.put_u8(item.version);
        dst.put_u32(item.length);

        // Write payload
        dst.extend_from_slice(&item.payload);

        Ok(())
    }
}

impl Encoder<ProtocolMessage> for ProtocolFrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ProtocolMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = item.encode_to_vec();
        let frame = ProtocolFrame::new(Bytes::from(payload));
        self.encode(frame, dst)
    }
}

/// Helper struct for encoding/decoding ProtocolMessage directly
pub struct ProtocolMessageCodec {
    frame_codec: ProtocolFrameCodec,
}

impl ProtocolMessageCodec {
    pub fn new() -> Self {
        Self {
            frame_codec: ProtocolFrameCodec::new(),
        }
    }

    pub fn with_max_frame_size(max_size: usize) -> Self {
        Self {
            frame_codec: ProtocolFrameCodec::with_max_frame_size(max_size),
        }
    }
}

impl Default for ProtocolMessageCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for ProtocolMessageCodec {
    type Item = ProtocolMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.frame_codec.decode(src)? {
            Some(frame) => {
                let message = frame
                    .to_message()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Ok(Some(message))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<ProtocolMessage> for ProtocolMessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ProtocolMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.frame_codec.encode(item, dst)
    }
}

#[cfg(test)]
mod tests {
    use crate::{create_message_id, create_timestamp, protocol::plugin_protocol::{MessageType, Method}};

    use super::*;
    use prost_types::Any;
    use std::collections::HashMap;

    #[test]
    fn test_protocol_frame_creation() {
        let payload = Bytes::from("test payload");
        let frame = ProtocolFrame::new(payload.clone());

        assert_eq!(frame.magic_number, MAGIC_NUMBER);
        assert_eq!(frame.version, PROTOCOL_VERSION);
        assert_eq!(frame.length, payload.len() as u32);
        assert_eq!(frame.payload, payload);

        frame.validate().unwrap();
    }

    #[test]
    fn test_frame_codec_encode_decode() {
        let mut codec = ProtocolFrameCodec::new();
        let payload = Bytes::from("test message payload");
        let original_frame = ProtocolFrame::new(payload);

        // Encode
        let mut buffer = BytesMut::new();
        codec.encode(original_frame.clone(), &mut buffer).unwrap();

        // Decode
        let decoded_frame = codec.decode(&mut buffer).unwrap().unwrap();

        assert_eq!(decoded_frame.magic_number, original_frame.magic_number);
        assert_eq!(decoded_frame.version, original_frame.version);
        assert_eq!(decoded_frame.length, original_frame.length);
        assert_eq!(decoded_frame.payload, original_frame.payload);
    }

    #[test]
    fn test_message_codec_encode_decode() {
        let mut codec = ProtocolMessageCodec::new();

        let original_message = ProtocolMessage {
            version: "1.0".to_string(),
            r#type: MessageType::Request as i32,
            id: create_message_id(),
            timestamp: Some(create_timestamp()),
            source: "test".to_string(),
            target: "target".to_string(),
            method: Some(Method::Ping as i32),
            params: Some(Any::default()),
            result: None,
            error: None,
            metadata: HashMap::new(),
        };

        // Encode
        let mut buffer = BytesMut::new();
        codec.encode(original_message.clone(), &mut buffer).unwrap();

        // Decode
        let decoded_message = codec.decode(&mut buffer).unwrap().unwrap();

        assert_eq!(decoded_message.version, original_message.version);
        assert_eq!(decoded_message.r#type, original_message.r#type);
        assert_eq!(decoded_message.id, original_message.id);
        assert_eq!(decoded_message.source, original_message.source);
        assert_eq!(decoded_message.target, original_message.target);
    }

    #[test]
    fn test_partial_frame_handling() {
        let mut codec = ProtocolFrameCodec::new();
        let payload = Bytes::from("test payload");
        let frame = ProtocolFrame::new(payload);

        // Encode complete frame
        let mut complete_buffer = BytesMut::new();
        codec.encode(frame, &mut complete_buffer).unwrap();

        // Test partial frame (only header)
        let mut partial_buffer = complete_buffer.split_to(FRAME_HEADER_SIZE);
        assert!(codec.decode(&mut partial_buffer).unwrap().is_none());

        // Add remaining data
        partial_buffer.unsplit(complete_buffer);
        let decoded = codec.decode(&mut partial_buffer).unwrap().unwrap();

        assert_eq!(decoded.magic_number, MAGIC_NUMBER);
    }

    #[test]
    fn test_invalid_magic_number() {
        let mut codec = ProtocolFrameCodec::new();
        let mut buffer = BytesMut::new();

        // Write invalid magic number
        buffer.put_u32(0xDEADBEEF);
        buffer.put_u8(PROTOCOL_VERSION);
        buffer.put_u32(0);

        let result = codec.decode(&mut buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid magic number"));
    }

    #[test]
    fn test_invalid_version() {
        let mut codec = ProtocolFrameCodec::new();
        let mut buffer = BytesMut::new();

        // Write invalid version
        buffer.put_u32(MAGIC_NUMBER);
        buffer.put_u8(0xFF); // Invalid version
        buffer.put_u32(0);

        let result = codec.decode(&mut buffer);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported protocol version"));
    }

    #[test]
    fn test_frame_size_limit() {
        let mut codec = ProtocolFrameCodec::with_max_frame_size(100);
        let large_payload = vec![0u8; 200];
        let frame = ProtocolFrame::new(Bytes::from(large_payload));

        let mut buffer = BytesMut::new();
        let result = codec.encode(frame, &mut buffer);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Frame too large"));
    }
}
