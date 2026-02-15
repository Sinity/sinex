use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

/// Collected system metrics during test execution
#[derive(Debug, Default, Clone)]
pub struct SystemMetrics {
    pub cpu_samples: Vec<f32>,
    pub mem_samples: Vec<u64>,
}

impl SystemMetrics {
    #[must_use]
    pub fn avg_cpu(&self) -> f32 {
        if self.cpu_samples.is_empty() {
            0.0
        } else {
            self.cpu_samples.iter().sum::<f32>() / self.cpu_samples.len() as f32
        }
    }

    #[must_use]
    pub fn max_mem_mb(&self) -> f64 {
        if self.mem_samples.is_empty() {
            0.0
        } else {
            (*self.mem_samples.iter().max().unwrap_or(&0) as f64) / 1024.0 / 1024.0
        }
    }
}

/// Handles background system resource monitoring
pub struct TestMonitor {
    running: Arc<AtomicBool>,
    metrics: Arc<Mutex<SystemMetrics>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestMonitor {
    /// Start monitoring system resources in a background thread
    #[must_use]
    pub fn start() -> Self {
        let metrics = Arc::new(Mutex::new(SystemMetrics::default()));
        let running = Arc::new(AtomicBool::new(true));

        let metrics_clone = metrics.clone();
        let running_clone = running.clone();

        let handle = thread::spawn(move || {
            let mut sys = System::new_with_specifics(
                RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            );

            // Wait a bit before first sample to avoid startup spike
            thread::sleep(Duration::from_millis(500));

            while running_clone.load(Ordering::Relaxed) {
                sys.refresh_cpu_all();
                sys.refresh_memory();

                let cpu_global = sys.global_cpu_usage();
                let mem_used = sys.used_memory();

                if let Ok(mut m) = metrics_clone.lock() {
                    m.cpu_samples.push(cpu_global);
                    m.mem_samples.push(mem_used);
                }
                thread::sleep(Duration::from_secs(1));
            }
        });

        Self {
            running,
            metrics,
            handle: Some(handle),
        }
    }

    /// Stop monitoring and return the collected metrics
    pub fn stop(&mut self) -> SystemMetrics {
        self.running.store(false, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        self.metrics.lock().expect("metrics lock poisoned").clone()
    }
}
