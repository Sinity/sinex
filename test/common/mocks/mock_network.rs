// Mock network implementation for testing
//
// Provides a controllable network substitute that can simulate:
// - Network partitions
// - Packet loss
// - Bandwidth limitations
// - Latency variations
// - Connection failures

use crate::common::prelude::*;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

/// Configuration for MockNetwork behavior
#[derive(Debug, Clone)]
pub struct MockNetworkConfig {
    /// Base network latency
    pub base_latency: Duration,
    /// Network jitter (random latency variation)
    pub jitter: Duration,
    /// Packet loss rate (0.0 to 1.0)
    pub packet_loss_rate: f64,
    /// Bandwidth limit in bytes per second
    pub bandwidth_limit: usize,
    /// Connection failure rate (0.0 to 1.0)
    pub connection_failure_rate: f64,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Whether to simulate network partitions
    pub simulate_partitions: bool,
    /// List of partitioned hosts
    pub partitioned_hosts: Vec<String>,
    /// Connection timeout
    pub connection_timeout: Duration,
}

impl Default for MockNetworkConfig {
    fn default() -> Self {
        Self {
            base_latency: Duration::from_millis(1),
            jitter: Duration::from_millis(0),
            packet_loss_rate: 0.0,
            bandwidth_limit: 1024 * 1024 * 1024, // 1GB/s (effectively unlimited)
            connection_failure_rate: 0.0,
            max_connections: 1000,
            simulate_partitions: false,
            partitioned_hosts: Vec::new(),
            connection_timeout: Duration::from_secs(30),
        }
    }
}

/// Mock network implementation
pub struct MockNetwork {
    config: Arc<RwLock<MockNetworkConfig>>,
    connections: Arc<RwLock<HashMap<String, MockConnection>>>,
    listeners: Arc<RwLock<HashMap<SocketAddr, MockListener>>>,
    traffic_stats: Arc<RwLock<NetworkTrafficStats>>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
struct MockConnection {
    id: String,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    established_at: Instant,
    bytes_sent: usize,
    bytes_received: usize,
    active: bool,
}

#[derive(Debug)]
struct MockListener {
    addr: SocketAddr,
    active: bool,
    accept_count: usize,
}

#[derive(Debug, Clone, Default)]
struct NetworkTrafficStats {
    total_connections: usize,
    active_connections: usize,
    bytes_sent: usize,
    bytes_received: usize,
    packets_sent: usize,
    packets_received: usize,
    packets_lost: usize,
    connection_failures: usize,
}

impl MockNetwork {
    pub fn new(config: MockNetworkConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            connections: Arc::new(RwLock::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(HashMap::new())),
            traffic_stats: Arc::new(RwLock::new(NetworkTrafficStats::default())),
            start_time: Instant::now(),
        }
    }

    pub async fn connect(
        &self,
        addr: SocketAddr,
    ) -> AnyhowResult<MockNetworkConnection, MockNetworkError> {
        let config = self.config.read().await;

        // Check if host is partitioned
        if config.simulate_partitions {
            let host = addr.ip().to_string();
            if config.partitioned_hosts.contains(&host) {
                return Err(MockNetworkError::NetworkPartition);
            }
        }

        // Check connection failure rate
        if fastrand::f64() < config.connection_failure_rate {
            let mut stats = self.traffic_stats.write().await;
            stats.connection_failures += 1;
            return Err(MockNetworkError::ConnectionFailed);
        }

        // Check connection limit
        let connections = self.connections.read().await;
        if connections.len() >= config.max_connections {
            return Err(MockNetworkError::TooManyConnections);
        }
        drop(connections);

        // Simulate connection latency
        let latency = self.calculate_latency(&config).await;
        tokio::time::sleep(latency).await;

        // Create connection
        let connection_id = format!("conn_{}", fastrand::u64(..));
        let local_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), fastrand::u16(1024..65535));

        let mock_connection = MockConnection {
            id: connection_id.clone(),
            local_addr,
            remote_addr: addr,
            established_at: Instant::now(),
            bytes_sent: 0,
            bytes_received: 0,
            active: true,
        };

        let mut connections = self.connections.write().await;
        connections.insert(connection_id.clone(), mock_connection);

        let mut stats = self.traffic_stats.write().await;
        stats.total_connections += 1;
        stats.active_connections += 1;

        Ok(MockNetworkConnection {
            network: self.clone(),
            connection_id,
            local_addr,
            remote_addr: addr,
            connected: true,
        })
    }

    pub async fn bind(
        &self,
        addr: SocketAddr,
    ) -> AnyhowResult<MockNetworkListener, MockNetworkError> {
        let config = self.config.read().await;

        // Check if address is already bound
        let listeners = self.listeners.read().await;
        if listeners.contains_key(&addr) {
            return Err(MockNetworkError::AddressInUse);
        }
        drop(listeners);

        let listener = MockListener {
            addr,
            active: true,
            accept_count: 0,
        };

        let mut listeners = self.listeners.write().await;
        listeners.insert(addr, listener);

        Ok(MockNetworkListener {
            network: self.clone(),
            addr,
            active: true,
        })
    }

    async fn calculate_latency(&self, config: &MockNetworkConfig) -> Duration {
        let base = config.base_latency;
        let jitter = if config.jitter.is_zero() {
            Duration::ZERO
        } else {
            let jitter_ms = fastrand::u64(0..config.jitter.as_millis() as u64);
            Duration::from_millis(jitter_ms)
        };
        base + jitter
    }

    pub async fn simulate_partition(&self, hosts: Vec<String>) {
        let mut config = self.config.write().await;
        config.simulate_partitions = true;
        config.partitioned_hosts = hosts;
    }

    pub async fn heal_partition(&self) {
        let mut config = self.config.write().await;
        config.simulate_partitions = false;
        config.partitioned_hosts.clear();
    }

    pub async fn set_latency(&self, latency: Duration, jitter: Duration) {
        let mut config = self.config.write().await;
        config.base_latency = latency;
        config.jitter = jitter;
    }

    pub async fn set_packet_loss(&self, loss_rate: f64) {
        let mut config = self.config.write().await;
        config.packet_loss_rate = loss_rate;
    }

    pub async fn set_bandwidth_limit(&self, bytes_per_second: usize) {
        let mut config = self.config.write().await;
        config.bandwidth_limit = bytes_per_second;
    }

    pub async fn get_stats(&self) -> NetworkTrafficStats {
        self.traffic_stats.read().await.clone()
    }

    pub async fn reset(&self) {
        let mut connections = self.connections.write().await;
        connections.clear();

        let mut listeners = self.listeners.write().await;
        listeners.clear();

        let mut stats = self.traffic_stats.write().await;
        *stats = NetworkTrafficStats::default();
    }

    async fn disconnect_connection(&self, connection_id: &str) {
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            conn.active = false;

            let mut stats = self.traffic_stats.write().await;
            stats.active_connections = stats.active_connections.saturating_sub(1);
        }
    }

    async fn send_data(
        &self,
        connection_id: &str,
        data: &[u8],
    ) -> AnyhowResult<(), MockNetworkError> {
        let config = self.config.read().await;

        // Check packet loss
        if fastrand::f64() < config.packet_loss_rate {
            let mut stats = self.traffic_stats.write().await;
            stats.packets_lost += 1;
            return Err(MockNetworkError::PacketLoss);
        }

        // Simulate bandwidth limiting
        let send_time = Duration::from_secs_f64(data.len() as f64 / config.bandwidth_limit as f64);
        tokio::time::sleep(send_time).await;

        // Simulate network latency
        let latency = self.calculate_latency(&config).await;
        tokio::time::sleep(latency).await;

        // Update connection stats
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            conn.bytes_sent += data.len();
        }

        // Update traffic stats
        let mut stats = self.traffic_stats.write().await;
        stats.bytes_sent += data.len();
        stats.packets_sent += 1;

        Ok(())
    }

    async fn receive_data(
        &self,
        connection_id: &str,
        buffer: &mut [u8],
    ) -> AnyhowResult<usize, MockNetworkError> {
        let config = self.config.read().await;

        // Check packet loss
        if fastrand::f64() < config.packet_loss_rate {
            let mut stats = self.traffic_stats.write().await;
            stats.packets_lost += 1;
            return Err(MockNetworkError::PacketLoss);
        }

        // Simulate network latency
        let latency = self.calculate_latency(&config).await;
        tokio::time::sleep(latency).await;

        // Simulate receiving data (for testing, we'll just return a fixed pattern)
        let data = b"mock_data_response";
        let copy_len = buffer.len().min(data.len());
        buffer[..copy_len].copy_from_slice(&data[..copy_len]);

        // Update connection stats
        let mut connections = self.connections.write().await;
        if let Some(conn) = connections.get_mut(connection_id) {
            conn.bytes_received += copy_len;
        }

        // Update traffic stats
        let mut stats = self.traffic_stats.write().await;
        stats.bytes_received += copy_len;
        stats.packets_received += 1;

        Ok(copy_len)
    }
}

impl Clone for MockNetwork {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            connections: self.connections.clone(),
            listeners: self.listeners.clone(),
            traffic_stats: self.traffic_stats.clone(),
            start_time: self.start_time,
        }
    }
}

/// Mock network connection
pub struct MockNetworkConnection {
    network: MockNetwork,
    connection_id: String,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    connected: bool,
}

impl MockNetworkConnection {
    pub async fn send(&mut self, data: &[u8]) -> AnyhowResult<(), MockNetworkError> {
        if !self.connected {
            return Err(MockNetworkError::NotConnected);
        }

        self.network.send_data(&self.connection_id, data).await
    }

    pub async fn receive(&mut self, buffer: &mut [u8]) -> AnyhowResult<usize, MockNetworkError> {
        if !self.connected {
            return Err(MockNetworkError::NotConnected);
        }

        self.network.receive_data(&self.connection_id, buffer).await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub async fn close(&mut self) -> AnyhowResult<(), MockNetworkError> {
        if self.connected {
            self.connected = false;
            self.network
                .disconnect_connection(&self.connection_id)
                .await;
        }
        Ok(())
    }
}

impl Drop for MockNetworkConnection {
    fn drop(&mut self) {
        if self.connected {
            self.connected = false;
            // In a real implementation, we'd need to handle cleanup asynchronously
        }
    }
}

/// Mock network listener
pub struct MockNetworkListener {
    network: MockNetwork,
    addr: SocketAddr,
    active: bool,
}

impl MockNetworkListener {
    pub async fn accept(&mut self) -> AnyhowResult<MockNetworkConnection, MockNetworkError> {
        if !self.active {
            return Err(MockNetworkError::ListenerClosed);
        }

        // Update listener stats
        let mut listeners = self.network.listeners.write().await;
        if let Some(listener) = listeners.get_mut(&self.addr) {
            listener.accept_count += 1;
        }

        // Create a mock incoming connection
        let remote_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            fastrand::u16(1024..65535),
        );

        let connection_id = format!("accept_{}", fastrand::u64(..));
        let mock_connection = MockConnection {
            id: connection_id.clone(),
            local_addr: self.addr,
            remote_addr,
            established_at: Instant::now(),
            bytes_sent: 0,
            bytes_received: 0,
            active: true,
        };

        let mut connections = self.network.connections.write().await;
        connections.insert(connection_id.clone(), mock_connection);

        let mut stats = self.network.traffic_stats.write().await;
        stats.total_connections += 1;
        stats.active_connections += 1;

        Ok(MockNetworkConnection {
            network: self.network.clone(),
            connection_id,
            local_addr: self.addr,
            remote_addr,
            connected: true,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn close(&mut self) -> AnyhowResult<(), MockNetworkError> {
        if self.active {
            self.active = false;
            let mut listeners = self.network.listeners.write().await;
            listeners.remove(&self.addr);
        }
        Ok(())
    }
}

impl Drop for MockNetworkListener {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            // In a real implementation, we'd need to handle cleanup asynchronously
        }
    }
}

/// Mock network errors
#[derive(Debug, Clone)]
pub enum MockNetworkError {
    NotConnected,
    ConnectionFailed,
    NetworkPartition,
    TooManyConnections,
    AddressInUse,
    PacketLoss,
    Timeout,
    ListenerClosed,
    IoError,
}

impl std::fmt::Display for MockNetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MockNetworkError::NotConnected => write!(f, "Not connected"),
            MockNetworkError::ConnectionFailed => write!(f, "Connection failed"),
            MockNetworkError::NetworkPartition => write!(f, "Network partition"),
            MockNetworkError::TooManyConnections => write!(f, "Too many connections"),
            MockNetworkError::AddressInUse => write!(f, "Address already in use"),
            MockNetworkError::PacketLoss => write!(f, "Packet loss"),
            MockNetworkError::Timeout => write!(f, "Network timeout"),
            MockNetworkError::ListenerClosed => write!(f, "Listener closed"),
            MockNetworkError::IoError => write!(f, "IO error"),
        }
    }
}

impl std::error::Error for MockNetworkError {}

/// Test utilities for MockNetwork
impl MockNetwork {
    pub fn for_testing() -> Self {
        Self::new(MockNetworkConfig::default())
    }

    pub fn with_high_latency(latency: Duration) -> Self {
        let config = MockNetworkConfig {
            base_latency: latency,
            jitter: latency / 4, // 25% jitter
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_packet_loss(loss_rate: f64) -> Self {
        let config = MockNetworkConfig {
            packet_loss_rate: loss_rate,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_failures(failure_rate: f64) -> Self {
        let config = MockNetworkConfig {
            connection_failure_rate: failure_rate,
            packet_loss_rate: failure_rate,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_bandwidth_limit(bytes_per_second: usize) -> Self {
        let config = MockNetworkConfig {
            bandwidth_limit: bytes_per_second,
            ..Default::default()
        };
        Self::new(config)
    }

    pub async fn verify_connection_count(&self, expected: usize) -> bool {
        let stats = self.get_stats().await;
        stats.active_connections == expected
    }

    pub async fn verify_bytes_sent(&self, expected: usize) -> bool {
        let stats = self.get_stats().await;
        stats.bytes_sent == expected
    }

    pub async fn get_connection_count(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }

    pub async fn get_listener_count(&self) -> usize {
        let listeners = self.listeners.read().await;
        listeners.len()
    }

    /// Simulate a complete network outage
    pub async fn simulate_outage(&self, duration: Duration) {
        self.set_packet_loss(1.0).await;

        let network = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            network.set_packet_loss(0.0).await;
        });
    }

    /// Simulate slow network conditions
    pub async fn simulate_slow_network(&self, latency: Duration, bandwidth: usize) {
        self.set_latency(latency, latency / 4).await;
        self.set_bandwidth_limit(bandwidth).await;
    }

    /// Simulate network instability
    pub async fn simulate_instability(&self, loss_rate: f64, failure_rate: f64) {
        self.set_packet_loss(loss_rate).await;

        let mut config = self.config.write().await;
        config.connection_failure_rate = failure_rate;
    }
}
