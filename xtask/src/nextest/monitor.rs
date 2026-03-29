use parking_lot::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
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

            // Brief delay before first sample to skip process-startup spike
            thread::sleep(Duration::from_millis(200));

            while running_clone.load(Ordering::Relaxed) {
                sys.refresh_cpu_all();
                sys.refresh_memory();

                let cpu_global = sys.global_cpu_usage();
                let mem_used = sys.used_memory();

                let mut metrics = metrics_clone.lock();
                metrics.cpu_samples.push(cpu_global);
                metrics.mem_samples.push(mem_used);
                // 250ms sampling: ~4x resolution vs 1s, negligible overhead
                // (sysinfo refresh is µs-scale). Short test runs (< 5s) now
                // get 16-20 samples instead of 2-4.
                thread::sleep(Duration::from_millis(250));
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

        self.metrics.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{SystemMetrics, TestMonitor};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn system_metrics_compute_averages() {
        let metrics = SystemMetrics {
            cpu_samples: vec![25.0, 75.0],
            mem_samples: vec![1024 * 1024, 2 * 1024 * 1024],
        };

        assert_eq!(metrics.avg_cpu(), 50.0);
        assert_eq!(metrics.max_mem_mb(), 2.0);
    }

    #[test]
    fn test_monitor_collects_samples_before_stop() {
        let mut monitor = TestMonitor::start();
        thread::sleep(Duration::from_millis(350));

        let metrics = monitor.stop();

        assert!(!metrics.cpu_samples.is_empty(), "expected at least one CPU sample");
        assert_eq!(metrics.cpu_samples.len(), metrics.mem_samples.len());
    }
}
