use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::Mutex;
use chrono::{DateTime, Utc};
use ulid::Ulid as InnerUlid;
use crate::Ulid;

/// A thread-safe monotonic ULID generator that ensures strict ordering
/// even when multiple ULIDs are generated in the same millisecond
pub struct MonotonicUlidGenerator {
    /// Last timestamp in milliseconds
    last_timestamp: AtomicU64,
    /// Counter for same-millisecond generation
    counter: AtomicU32,
    /// Process ID component for multi-process uniqueness
    process_id: u16,
    /// Lock for critical section during generation
    generation_lock: Mutex<()>,
}

impl MonotonicUlidGenerator {
    /// Create a new monotonic ULID generator
    pub fn new() -> Self {
        // Use lower 16 bits of process ID for multi-process uniqueness
        let process_id = std::process::id() as u16;
        
        Self {
            last_timestamp: AtomicU64::new(0),
            counter: AtomicU32::new(0),
            process_id,
            generation_lock: Mutex::new(()),
        }
    }
    
    /// Generate a new monotonic ULID
    pub fn generate(&self) -> Ulid {
        // Lock to ensure atomic timestamp check and update
        let _guard = self.generation_lock.lock().unwrap();
        
        let now = Utc::now();
        let timestamp_ms = now.timestamp_millis() as u64;
        
        let last_ts = self.last_timestamp.load(Ordering::SeqCst);
        
        if timestamp_ms > last_ts {
            // New millisecond - reset counter
            self.last_timestamp.store(timestamp_ms, Ordering::SeqCst);
            self.counter.store(0, Ordering::SeqCst);
            
            // Generate with process ID in random component
            let mut ulid = InnerUlid::from_datetime(now.into());
            self.embed_process_id(&mut ulid);
            
            Ulid::from(ulid)
        } else {
            // Same millisecond - increment counter
            let counter = self.counter.fetch_add(1, Ordering::SeqCst);
            
            // Check for counter overflow (extremely unlikely but handle gracefully)
            if counter == u32::MAX {
                // Wait for next millisecond to avoid overflow
                std::thread::sleep(std::time::Duration::from_millis(1));
                
                // Now we're in a new millisecond, reset and generate
                let now = Utc::now();
                let new_timestamp_ms = now.timestamp_millis() as u64;
                self.last_timestamp.store(new_timestamp_ms, Ordering::SeqCst);
                self.counter.store(0, Ordering::SeqCst);
                
                let mut ulid = InnerUlid::from_datetime(now.into());
                self.embed_process_id(&mut ulid);
                
                Ulid::from(ulid)
            } else {
                // Create ULID with embedded counter and process ID
                let ulid = self.create_with_counter(timestamp_ms, counter);
                
                Ulid::from(ulid)
            }
        }
    }
    
    /// Generate a ULID from a specific datetime with monotonic guarantee
    pub fn generate_from_datetime(&self, datetime: DateTime<Utc>) -> Ulid {
        let _guard = self.generation_lock.lock().unwrap();
        
        let timestamp_ms = datetime.timestamp_millis() as u64;
        let last_ts = self.last_timestamp.load(Ordering::SeqCst);
        
        if timestamp_ms > last_ts {
            self.last_timestamp.store(timestamp_ms, Ordering::SeqCst);
            self.counter.store(0, Ordering::SeqCst);
            
            let mut ulid = InnerUlid::from_datetime(datetime.into());
            self.embed_process_id(&mut ulid);
            
            Ulid::from(ulid)
        } else {
            // Use counter even for past timestamps to maintain uniqueness
            let counter = self.counter.fetch_add(1, Ordering::SeqCst);
            let ulid = self.create_with_counter(timestamp_ms, counter);
            
            Ulid::from(ulid)
        }
    }
    
    /// Create a ULID with specific timestamp and counter values
    fn create_with_counter(&self, timestamp_ms: u64, counter: u32) -> InnerUlid {
        let mut bytes = [0u8; 16];
        
        // First 6 bytes: timestamp (48 bits)
        bytes[0] = (timestamp_ms >> 40) as u8;
        bytes[1] = (timestamp_ms >> 32) as u8;
        bytes[2] = (timestamp_ms >> 24) as u8;
        bytes[3] = (timestamp_ms >> 16) as u8;
        bytes[4] = (timestamp_ms >> 8) as u8;
        bytes[5] = timestamp_ms as u8;
        
        // Next 2 bytes: process ID (16 bits)
        bytes[6] = (self.process_id >> 8) as u8;
        bytes[7] = self.process_id as u8;
        
        // Next 4 bytes: counter (32 bits)
        bytes[8] = (counter >> 24) as u8;
        bytes[9] = (counter >> 16) as u8;
        bytes[10] = (counter >> 8) as u8;
        bytes[11] = counter as u8;
        
        // Last 4 bytes: random for additional entropy
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.fill(&mut bytes[12..16]);
        
        InnerUlid::from_bytes(bytes)
    }
    
    /// Embed process ID into an existing ULID's random component
    fn embed_process_id(&self, ulid: &mut InnerUlid) {
        let mut bytes = ulid.to_bytes();
        
        // Embed process ID in bytes 6-7 of the random component
        bytes[6] = (self.process_id >> 8) as u8;
        bytes[7] = self.process_id as u8;
        
        *ulid = InnerUlid::from_bytes(bytes);
    }
    
    /// Get statistics about the generator
    pub fn stats(&self) -> GeneratorStats {
        GeneratorStats {
            last_timestamp: self.last_timestamp.load(Ordering::Relaxed),
            current_counter: self.counter.load(Ordering::Relaxed),
            process_id: self.process_id,
        }
    }
}

impl Default for MonotonicUlidGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct GeneratorStats {
    pub last_timestamp: u64,
    pub current_counter: u32,
    pub process_id: u16,
}

lazy_static::lazy_static! {
    /// Global monotonic ULID generator for convenience
    static ref GLOBAL_GENERATOR: MonotonicUlidGenerator = MonotonicUlidGenerator::new();
}

/// Generate a globally monotonic ULID
pub fn generate_monotonic() -> Ulid {
    GLOBAL_GENERATOR.generate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;
    
    #[test]
    fn test_monotonic_generation() {
        let generator = MonotonicUlidGenerator::new();
        
        let mut ulids = Vec::new();
        for _ in 0..1000 {
            ulids.push(generator.generate());
        }
        
        // Check that all ULIDs are strictly increasing
        for i in 1..ulids.len() {
            assert!(ulids[i] > ulids[i-1], "ULID {} not greater than {}", ulids[i], ulids[i-1]);
        }
    }
    
    #[test]
    fn test_concurrent_generation() {
        let generator = Arc::new(MonotonicUlidGenerator::new());
        let mut handles = vec![];
        
        // Spawn multiple threads generating ULIDs
        for _ in 0..10 {
            let gen = Arc::clone(&generator);
            let handle = thread::spawn(move || {
                let mut local_ulids = Vec::new();
                for _ in 0..100 {
                    local_ulids.push(gen.generate());
                }
                local_ulids
            });
            handles.push(handle);
        }
        
        // Collect all ULIDs
        let mut all_ulids = Vec::new();
        for handle in handles {
            all_ulids.extend(handle.join().unwrap());
        }
        
        // Check uniqueness
        let unique_ulids: HashSet<_> = all_ulids.iter().collect();
        assert_eq!(unique_ulids.len(), all_ulids.len(), "Found duplicate ULIDs");
        
        // Check that process ID is embedded
        let stats = generator.stats();
        for ulid in &all_ulids {
            let bytes = ulid.to_bytes();
            let embedded_pid = ((bytes[6] as u16) << 8) | (bytes[7] as u16);
            assert_eq!(embedded_pid, stats.process_id);
        }
    }
    
    #[test]
    fn test_counter_increment() {
        let generator = MonotonicUlidGenerator::new();
        
        // Generate many ULIDs quickly to force same-millisecond generation
        let start = Utc::now();
        let mut ulids = Vec::new();
        
        while Utc::now().signed_duration_since(start).num_milliseconds() < 2 {
            ulids.push(generator.generate());
        }
        
        // Should have generated multiple ULIDs
        assert!(ulids.len() > 1);
        
        // All should be unique and ordered
        for i in 1..ulids.len() {
            assert!(ulids[i] > ulids[i-1]);
        }
    }
}