use anyhow::Result;
use blake3::Hasher;
use fastcdc::v2020::{FastCDC, StreamCDC};
use serde::{Deserialize, Serialize};
use std::io::Read;

/// Configuration for FastCDC chunking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingConfig {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
    pub enable_blake3_hashing: bool,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            min_size: 8192,  // 8KB minimum
            avg_size: 16384, // 16KB average
            max_size: 32768, // 32KB maximum
            enable_blake3_hashing: true,
        }
    }
}

/// A content chunk with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentChunk {
    pub data: Vec<u8>,
    pub offset: u64,
    pub length: u32,
    pub blake3_hash: Option<String>,
}

/// Chunking service for content-defined chunking
pub struct ChunkingService {
    config: ChunkingConfig,
}

impl ChunkingService {
    pub fn new(config: ChunkingConfig) -> Self {
        Self { config }
    }

    pub fn with_default_config() -> Self {
        Self::new(ChunkingConfig::default())
    }

    /// Chunk data from a byte slice
    pub fn chunk_bytes(&self, data: &[u8]) -> Result<Vec<ContentChunk>> {
        let chunker = FastCDC::new(
            data,
            self.config.min_size,
            self.config.avg_size,
            self.config.max_size,
        );

        let mut chunks = Vec::new();
        for chunk_data in chunker {
            let blake3_hash = if self.config.enable_blake3_hashing {
                let mut hasher = Hasher::new();
                hasher.update(&data[chunk_data.offset..(chunk_data.offset + chunk_data.length)]);
                Some(hasher.finalize().to_hex().to_string())
            } else {
                None
            };

            chunks.push(ContentChunk {
                data: data[chunk_data.offset..(chunk_data.offset + chunk_data.length)].to_vec(),
                offset: chunk_data.offset as u64,
                length: chunk_data.length as u32,
                blake3_hash,
            });
        }

        Ok(chunks)
    }

    /// Chunk data from a reader stream (useful for large files)
    pub fn chunk_stream<R: Read>(&self, reader: R) -> Result<Vec<ContentChunk>> {
        let stream_chunker = StreamCDC::new(
            reader,
            self.config.min_size,
            self.config.avg_size,
            self.config.max_size,
        );

        let mut chunks = Vec::new();
        let mut offset = 0u64;

        for chunk_result in stream_chunker {
            let chunk_data = chunk_result?;

            let blake3_hash = if self.config.enable_blake3_hashing {
                let mut hasher = Hasher::new();
                hasher.update(&chunk_data.data);
                Some(hasher.finalize().to_hex().to_string())
            } else {
                None
            };

            chunks.push(ContentChunk {
                data: chunk_data.data,
                offset,
                length: chunk_data.length as u32,
                blake3_hash,
            });

            offset += chunk_data.length as u64;
        }

        Ok(chunks)
    }

    /// Chunk a string payload (common for JSON event payloads)
    pub fn chunk_string(&self, content: &str) -> Result<Vec<ContentChunk>> {
        self.chunk_bytes(content.as_bytes())
    }

    /// Calculate optimal chunk boundaries for a given size
    pub fn calculate_chunk_info(&self, total_size: u64) -> ChunkInfo {
        let avg_chunks = (total_size as f64 / self.config.avg_size as f64).ceil() as u32;
        let min_chunks = (total_size as f64 / self.config.max_size as f64).ceil() as u32;
        let max_chunks = (total_size as f64 / self.config.min_size as f64).ceil() as u32;

        ChunkInfo {
            total_size,
            estimated_chunks: avg_chunks,
            min_possible_chunks: min_chunks,
            max_possible_chunks: max_chunks,
            config: self.config.clone(),
        }
    }
}

/// Information about chunking for a given content size
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub total_size: u64,
    pub estimated_chunks: u32,
    pub min_possible_chunks: u32,
    pub max_possible_chunks: u32,
    pub config: ChunkingConfig,
}

/// Utility functions for chunk deduplication
pub mod deduplication {
    use super::*;
    use std::collections::HashMap;

    /// Deduplicate chunks based on BLAKE3 hash
    pub fn deduplicate_chunks(chunks: Vec<ContentChunk>) -> (Vec<ContentChunk>, DedupStats) {
        let mut seen_hashes = HashMap::new();
        let mut unique_chunks = Vec::new();
        let mut duplicate_count = 0;
        let mut bytes_saved = 0;

        for chunk in chunks {
            if let Some(hash) = &chunk.blake3_hash {
                if seen_hashes.contains_key(hash) {
                    duplicate_count += 1;
                    bytes_saved += chunk.length as u64;
                } else {
                    seen_hashes.insert(hash.clone(), chunk.offset);
                    unique_chunks.push(chunk);
                }
            } else {
                // No hash available, treat as unique
                unique_chunks.push(chunk);
            }
        }

        let stats = DedupStats {
            original_chunks: seen_hashes.len() + duplicate_count as usize,
            unique_chunks: unique_chunks.len(),
            duplicate_chunks: duplicate_count as usize,
            bytes_saved,
        };

        (unique_chunks, stats)
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DedupStats {
        pub original_chunks: usize,
        pub unique_chunks: usize,
        pub duplicate_chunks: usize,
        pub bytes_saved: u64,
    }
}

/// Event payload chunking utilities
pub mod event_chunking {
    use super::*;
    use serde_json::Value as JsonValue;

    /// Chunk large JSON event payloads
    pub fn chunk_json_payload(
        payload: &JsonValue,
        config: &ChunkingConfig,
    ) -> Result<Vec<ContentChunk>> {
        let json_string = serde_json::to_string(payload)?;
        let chunker = ChunkingService::new(config.clone());
        chunker.chunk_string(&json_string)
    }

    /// Check if a JSON payload should be chunked based on size
    pub fn should_chunk_payload(payload: &JsonValue, threshold_bytes: usize) -> bool {
        if let Ok(json_string) = serde_json::to_string(payload) {
            json_string.len() > threshold_bytes
        } else {
            false
        }
    }

    /// Reconstruct JSON payload from chunks
    pub fn reconstruct_json_from_chunks(chunks: &[ContentChunk]) -> Result<JsonValue> {
        // Sort chunks by offset to ensure correct order
        let mut sorted_chunks = chunks.to_vec();
        sorted_chunks.sort_by_key(|c| c.offset);

        let mut reconstructed = Vec::new();
        for chunk in sorted_chunks {
            reconstructed.extend_from_slice(&chunk.data);
        }

        let json_string = String::from_utf8(reconstructed)?;
        let payload = serde_json::from_str(&json_string)?;
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunking_service_creation() {
        let service = ChunkingService::with_default_config();
        assert_eq!(service.config.min_size, 8192);
        assert_eq!(service.config.avg_size, 16384);
        assert_eq!(service.config.max_size, 32768);
    }

    #[test]
    fn test_small_data_chunking() {
        let service = ChunkingService::with_default_config();
        let data = b"Hello, world! This is a small piece of test data.";

        let chunks = service
            .chunk_bytes(data)
            .expect("Should chunk successfully");

        // Small data should result in one chunk
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, data);
        assert_eq!(chunks[0].offset, 0);
        assert!(chunks[0].blake3_hash.is_some());
    }

    #[test]
    fn test_large_data_chunking() {
        let service = ChunkingService::with_default_config();
        // Create data larger than max chunk size
        let data = vec![b'A'; 100_000]; // 100KB of 'A'

        let chunks = service
            .chunk_bytes(&data)
            .expect("Should chunk successfully");

        // Should result in multiple chunks
        assert!(chunks.len() > 1);

        // Verify chunks reconstruct original data
        let mut reconstructed = Vec::new();
        for chunk in &chunks {
            reconstructed.extend_from_slice(&chunk.data);
        }
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_json_payload_chunking() {
        use serde_json::Value as JsonValue;

        let config = ChunkingConfig::default();

        // Create large JSON payload
        let mut large_object = serde_json::Map::new();
        for i in 0..1000 {
            large_object.insert(
                format!("key_{}", i),
                JsonValue::String(format!("value_{}_with_lots_of_data_to_make_it_large", i)),
            );
        }
        let payload = JsonValue::Object(large_object);

        let chunks = event_chunking::chunk_json_payload(&payload, &config)
            .expect("Should chunk JSON payload");

        assert!(chunks.len() > 1, "Large JSON should be chunked");

        // Test reconstruction
        let reconstructed =
            event_chunking::reconstruct_json_from_chunks(&chunks).expect("Should reconstruct JSON");

        assert_eq!(reconstructed, payload);
    }

    #[test]
    fn test_chunk_deduplication() {
        let service = ChunkingService::with_default_config();

        // Create data with repeated patterns
        let mut data = Vec::new();
        let pattern = b"This is a repeated pattern for testing deduplication. ";
        for _ in 0..100 {
            data.extend_from_slice(pattern);
        }

        let chunks = service
            .chunk_bytes(&data)
            .expect("Should chunk successfully");
        let (deduped_chunks, stats) = deduplication::deduplicate_chunks(chunks);

        assert!(deduped_chunks.len() <= stats.original_chunks);
        if stats.duplicate_chunks > 0 {
            assert!(stats.bytes_saved > 0);
        }
    }
}