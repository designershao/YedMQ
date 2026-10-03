use crate::protobuf::raft_payload::BulkSyncChunk;
use bytes::Bytes;

pub(super) const CHUNK_SIZE: usize = 1024 * 1024;
pub(super) const RPC_SIZE_LIMIT: usize = 4 * 1024 * 1024;

pub(super) fn chunks(key: String, data: Bytes) -> impl Iterator<Item = BulkSyncChunk> {
    let size = data.len();
    let checksum = crc32fast::hash(&data);
    (0..size.max(1)).step_by(CHUNK_SIZE).map(move |offset| {
        let end = (offset + CHUNK_SIZE).min(size);
        let last = end == size;
        BulkSyncChunk {
            key: key.clone(),
            data: data.slice(offset..end).to_vec(),
            offset: offset as u64,
            is_last_chunk: last,
            total_size: if last { size as u64 } else { 0 },
            checksum: if last { checksum } else { 0 },
        }
    })
}

/// Buffers one key at a time; incomplete or invalid data is never published.
#[derive(Default)]
pub(super) struct ChunkAssembler {
    current: Option<(String, Vec<u8>)>,
}

impl ChunkAssembler {
    pub(super) fn push(&mut self, chunk: BulkSyncChunk) -> anyhow::Result<Option<(String, Bytes)>> {
        let (key, data) = self
            .current
            .get_or_insert_with(|| (chunk.key.clone(), Vec::new()));
        anyhow::ensure!(*key == chunk.key, "interleaved payload keys");
        anyhow::ensure!(chunk.offset == data.len() as u64, "invalid payload offset");
        anyhow::ensure!(
            !chunk.data.is_empty() || chunk.is_last_chunk,
            "empty intermediate chunk"
        );
        data.extend_from_slice(&chunk.data);
        if !chunk.is_last_chunk {
            return Ok(None);
        }
        anyhow::ensure!(
            chunk.total_size == data.len() as u64,
            "payload size mismatch"
        );
        anyhow::ensure!(
            chunk.checksum == crc32fast::hash(data),
            "payload checksum mismatch"
        );
        let (key, data) = self.current.take().unwrap();
        Ok(Some((key, Bytes::from(data))))
    }

    pub(super) fn finish(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.current.is_none(), "incomplete payload stream");
        Ok(())
    }
}
