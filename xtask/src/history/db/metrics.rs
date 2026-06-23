use super::*;

impl HistoryDb {
    pub fn record_system_metrics(
        &self,
        invocation_id: i64,
        cpu_usage_avg: f32,
        memory_usage_max_mb: f64,
    ) -> Result<()> {
        self.conn.execute(
            r"
            UPDATE invocations
            SET cpu_usage_avg = ?1, memory_usage_max_mb = ?2
            WHERE id = ?3
            ",
            params![cpu_usage_avg, memory_usage_max_mb, invocation_id],
        )?;
        Ok(())
    }

    /// Record invocation-local resource metrics for an invocation.
    pub fn record_resource_metrics(
        &self,
        invocation_id: i64,
        metrics: &crate::process::InvocationResourceMetrics,
    ) -> Result<()> {
        self.ensure_compat_schema()?;
        self.conn.execute(
            r"
            UPDATE invocations
            SET process_cpu_usage_avg = ?1,
                process_memory_usage_max_mb = ?2,
                root_process_cpu_usage_avg = ?3,
                root_process_memory_usage_max_mb = ?4,
                shared_nix_daemon_cpu_usage_avg = ?5,
                shared_nix_daemon_memory_usage_max_mb = ?6,
                shared_nix_build_slice_cpu_usage_avg = ?7,
                shared_nix_build_slice_memory_usage_max_mb = ?8,
                shared_background_slice_cpu_usage_avg = ?9,
                shared_background_slice_memory_usage_max_mb = ?10,
                host_cpu_pressure_some_avg10_max = ?11,
                host_io_pressure_some_avg10_max = ?12,
                host_io_pressure_full_avg10_max = ?13,
                host_memory_pressure_some_avg10_max = ?14,
                host_memory_pressure_full_avg10_max = ?15,
                host_block_read_mib_delta = ?16,
                host_block_write_mib_delta = ?17,
                host_block_read_iops_avg = ?18,
                host_block_write_iops_avg = ?19,
                host_block_busiest_device = ?20,
                host_block_busiest_device_total_mib_delta = ?21,
                host_block_busiest_device_read_iops_avg = ?22,
                host_block_busiest_device_write_iops_avg = ?23,
                host_block_busiest_device_weighted_io_ms_per_s = ?24,
                shm_free_min_mb = ?25,
                shm_used_max_mb = ?26,
                process_count_max = ?27,
                resource_sample_count = ?28
            WHERE id = ?29
            ",
            params![
                metrics.process_tree.cpu_usage_avg,
                metrics.process_tree.memory_usage_max_mb,
                metrics.process_tree.root_cpu_usage_avg,
                metrics.process_tree.root_memory_usage_max_mb,
                metrics.shared_build.shared_nix_daemon_cpu_usage_avg,
                metrics.shared_build.shared_nix_daemon_memory_usage_max_mb,
                metrics.shared_build.shared_nix_build_slice_cpu_usage_avg,
                metrics
                    .shared_build
                    .shared_nix_build_slice_memory_usage_max_mb,
                metrics.shared_build.shared_background_slice_cpu_usage_avg,
                metrics
                    .shared_build
                    .shared_background_slice_memory_usage_max_mb,
                metrics.host_pressure.cpu_some_avg10_max,
                metrics.host_pressure.io_some_avg10_max,
                metrics.host_pressure.io_full_avg10_max,
                metrics.host_pressure.memory_some_avg10_max,
                metrics.host_pressure.memory_full_avg10_max,
                metrics.host_block_io.read_mib_delta,
                metrics.host_block_io.write_mib_delta,
                metrics.host_block_io.read_iops_avg,
                metrics.host_block_io.write_iops_avg,
                metrics.host_block_io.busiest_device.clone(),
                metrics.host_block_io.busiest_device_total_mib_delta,
                metrics.host_block_io.busiest_device_read_iops_avg,
                metrics.host_block_io.busiest_device_write_iops_avg,
                metrics.host_block_io.busiest_device_weighted_io_ms_per_s,
                metrics.host_pressure.shm_free_min_mb,
                metrics.host_pressure.shm_used_max_mb,
                metrics.process_tree.process_count_max.map(i64::from),
                i64::from(metrics.process_tree.sample_count),
                invocation_id
            ],
        )?;
        Ok(())
    }

    fn invocation_columns(&self) -> Result<HashSet<String>> {
        let mut stmt = self
            .conn
            .prepare("PRAGMA table_info(invocations)")
            .context("failed to inspect invocation history schema")?;
        let mut columns = HashSet::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for row in rows {
            columns.insert(row?);
        }
        Ok(columns)
    }

    /// Get resource usage (CPU/memory) for recent invocations.
    pub fn get_resource_usage(
        &self,
        command_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ResourceUsage>> {
        self.get_resource_usage_with_zombies(command_filter, limit, false)
    }

    /// Get resource usage (CPU/memory) for recent invocations, optionally including zombie cancellations.
    pub fn get_resource_usage_with_zombies(
        &self,
        command_filter: Option<&str>,
        limit: usize,
        include_zombies: bool,
    ) -> Result<Vec<ResourceUsage>> {
        let columns = self.invocation_columns()?;
        let process_cpu_expr = if columns.contains("process_cpu_usage_avg") {
            "process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let process_mem_expr = if columns.contains("process_memory_usage_max_mb") {
            "process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let root_process_cpu_expr = if columns.contains("root_process_cpu_usage_avg") {
            "root_process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let root_process_mem_expr = if columns.contains("root_process_memory_usage_max_mb") {
            "root_process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let shared_nix_daemon_cpu_expr = if columns.contains("shared_nix_daemon_cpu_usage_avg") {
            "shared_nix_daemon_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_daemon_mem_expr =
            if columns.contains("shared_nix_daemon_memory_usage_max_mb") {
                "shared_nix_daemon_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_nix_build_cpu_expr = if columns.contains("shared_nix_build_slice_cpu_usage_avg")
        {
            "shared_nix_build_slice_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_build_mem_expr =
            if columns.contains("shared_nix_build_slice_memory_usage_max_mb") {
                "shared_nix_build_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_background_cpu_expr =
            if columns.contains("shared_background_slice_cpu_usage_avg") {
                "shared_background_slice_cpu_usage_avg"
            } else {
                "NULL"
            };
        let shared_background_mem_expr =
            if columns.contains("shared_background_slice_memory_usage_max_mb") {
                "shared_background_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let process_count_expr = if columns.contains("process_count_max") {
            "process_count_max"
        } else {
            "NULL"
        };
        let sample_count_expr = if columns.contains("resource_sample_count") {
            "resource_sample_count"
        } else {
            "NULL"
        };
        let host_cpu_pressure_expr = if columns.contains("host_cpu_pressure_some_avg10_max") {
            "host_cpu_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_some_expr = if columns.contains("host_io_pressure_some_avg10_max") {
            "host_io_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_full_expr = if columns.contains("host_io_pressure_full_avg10_max") {
            "host_io_pressure_full_avg10_max"
        } else {
            "NULL"
        };
        let host_memory_pressure_some_expr =
            if columns.contains("host_memory_pressure_some_avg10_max") {
                "host_memory_pressure_some_avg10_max"
            } else {
                "NULL"
            };
        let host_memory_pressure_full_expr =
            if columns.contains("host_memory_pressure_full_avg10_max") {
                "host_memory_pressure_full_avg10_max"
            } else {
                "NULL"
            };
        let host_block_read_mib_expr = if columns.contains("host_block_read_mib_delta") {
            "host_block_read_mib_delta"
        } else {
            "NULL"
        };
        let host_block_write_mib_expr = if columns.contains("host_block_write_mib_delta") {
            "host_block_write_mib_delta"
        } else {
            "NULL"
        };
        let host_block_read_iops_expr = if columns.contains("host_block_read_iops_avg") {
            "host_block_read_iops_avg"
        } else {
            "NULL"
        };
        let host_block_write_iops_expr = if columns.contains("host_block_write_iops_avg") {
            "host_block_write_iops_avg"
        } else {
            "NULL"
        };
        let host_block_busiest_device_expr = if columns.contains("host_block_busiest_device") {
            "host_block_busiest_device"
        } else {
            "NULL"
        };
        let host_block_busiest_total_mib_expr =
            if columns.contains("host_block_busiest_device_total_mib_delta") {
                "host_block_busiest_device_total_mib_delta"
            } else {
                "NULL"
            };
        let host_block_busiest_read_iops_expr =
            if columns.contains("host_block_busiest_device_read_iops_avg") {
                "host_block_busiest_device_read_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_write_iops_expr =
            if columns.contains("host_block_busiest_device_write_iops_avg") {
                "host_block_busiest_device_write_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_weighted_expr =
            if columns.contains("host_block_busiest_device_weighted_io_ms_per_s") {
                "host_block_busiest_device_weighted_io_ms_per_s"
            } else {
                "NULL"
            };
        let shm_free_expr = if columns.contains("shm_free_min_mb") {
            "shm_free_min_mb"
        } else {
            "NULL"
        };
        let shm_used_expr = if columns.contains("shm_used_max_mb") {
            "shm_used_max_mb"
        } else {
            "NULL"
        };
        let mut query = String::from(&format!(
            r"SELECT command,
                         status,
                         started_at,
                         duration_secs,
                         {process_cpu_expr},
                         {process_mem_expr},
                         {root_process_cpu_expr},
                         {root_process_mem_expr},
                         {shared_nix_daemon_cpu_expr},
                         {shared_nix_daemon_mem_expr},
                         {shared_nix_build_cpu_expr},
                         {shared_nix_build_mem_expr},
                         {shared_background_cpu_expr},
                         {shared_background_mem_expr},
                         {process_count_expr},
                         {sample_count_expr},
                         cpu_usage_avg,
                         memory_usage_max_mb,
                         {host_cpu_pressure_expr},
                         {host_io_pressure_some_expr},
                         {host_io_pressure_full_expr},
                         {host_memory_pressure_some_expr},
                         {host_memory_pressure_full_expr},
                         {host_block_read_mib_expr},
                         {host_block_write_mib_expr},
                         {host_block_read_iops_expr},
                         {host_block_write_iops_expr},
                         {host_block_busiest_device_expr},
                         {host_block_busiest_total_mib_expr},
                         {host_block_busiest_read_iops_expr},
                         {host_block_busiest_write_iops_expr},
                         {host_block_busiest_weighted_expr},
                         {shm_free_expr},
                         {shm_used_expr}
              FROM invocations
              WHERE status != 'running'
               AND ({process_cpu_expr} IS NOT NULL
                     OR {process_mem_expr} IS NOT NULL
                     OR {root_process_cpu_expr} IS NOT NULL
                     OR {root_process_mem_expr} IS NOT NULL
                     OR {shared_nix_daemon_cpu_expr} IS NOT NULL
                     OR {shared_nix_daemon_mem_expr} IS NOT NULL
                     OR {shared_nix_build_cpu_expr} IS NOT NULL
                     OR {shared_nix_build_mem_expr} IS NOT NULL
                     OR {shared_background_cpu_expr} IS NOT NULL
                     OR {shared_background_mem_expr} IS NOT NULL
                     OR {host_cpu_pressure_expr} IS NOT NULL
                     OR {host_io_pressure_some_expr} IS NOT NULL
                     OR {host_io_pressure_full_expr} IS NOT NULL
                     OR {host_memory_pressure_some_expr} IS NOT NULL
                     OR {host_memory_pressure_full_expr} IS NOT NULL
                     OR {host_block_read_mib_expr} IS NOT NULL
                     OR {host_block_write_mib_expr} IS NOT NULL
                     OR {host_block_read_iops_expr} IS NOT NULL
                     OR {host_block_write_iops_expr} IS NOT NULL
                     OR {host_block_busiest_device_expr} IS NOT NULL
                     OR {host_block_busiest_total_mib_expr} IS NOT NULL
                     OR {host_block_busiest_read_iops_expr} IS NOT NULL
                     OR {host_block_busiest_write_iops_expr} IS NOT NULL
                     OR {host_block_busiest_weighted_expr} IS NOT NULL
                     OR {shm_free_expr} IS NOT NULL
                     OR {shm_used_expr} IS NOT NULL
                     OR cpu_usage_avg IS NOT NULL
                     OR memory_usage_max_mb IS NOT NULL)",
        ));
        if !include_zombies {
            query.push_str(" AND ");
            query.push_str(&non_zombie_cancel_filter(""));
        }
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut param_idx = 1usize;

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(" ORDER BY id DESC LIMIT ?{param_idx}"));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |row| {
            Ok(ResourceUsage {
                command: row.get(0)?,
                status: row.get(1)?,
                started_at: row.get(2)?,
                duration_secs: row.get(3)?,
                process_cpu_usage_avg: row.get(4)?,
                process_memory_usage_max_mb: row.get(5)?,
                root_process_cpu_usage_avg: row.get(6)?,
                root_process_memory_usage_max_mb: row.get(7)?,
                shared_nix_daemon_cpu_usage_avg: row.get(8)?,
                shared_nix_daemon_memory_usage_max_mb: row.get(9)?,
                shared_nix_build_slice_cpu_usage_avg: row.get(10)?,
                shared_nix_build_slice_memory_usage_max_mb: row.get(11)?,
                shared_background_slice_cpu_usage_avg: row.get(12)?,
                shared_background_slice_memory_usage_max_mb: row.get(13)?,
                process_count_max: row.get::<_, Option<i64>>(14)?.map(|value| value as u32),
                sample_count: row.get::<_, Option<i64>>(15)?.map(|value| value as u32),
                host_cpu_usage_avg: row.get(16)?,
                host_memory_usage_max_mb: row.get(17)?,
                host_cpu_pressure_some_avg10_max: row.get(18)?,
                host_io_pressure_some_avg10_max: row.get(19)?,
                host_io_pressure_full_avg10_max: row.get(20)?,
                host_memory_pressure_some_avg10_max: row.get(21)?,
                host_memory_pressure_full_avg10_max: row.get(22)?,
                host_block_read_mib_delta: row.get(23)?,
                host_block_write_mib_delta: row.get(24)?,
                host_block_read_iops_avg: row.get(25)?,
                host_block_write_iops_avg: row.get(26)?,
                host_block_busiest_device: row.get(27)?,
                host_block_busiest_device_total_mib_delta: row.get(28)?,
                host_block_busiest_device_read_iops_avg: row.get(29)?,
                host_block_busiest_device_write_iops_avg: row.get(30)?,
                host_block_busiest_device_weighted_io_ms_per_s: row.get(31)?,
                shm_free_min_mb: row.get(32)?,
                shm_used_max_mb: row.get(33)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get resource usage (CPU/memory/process count) for a specific invocation.
    pub fn get_resource_usage_for_invocation(
        &self,
        invocation_id: i64,
    ) -> Result<Option<ResourceUsage>> {
        let columns = self.invocation_columns()?;
        let process_cpu_expr = if columns.contains("process_cpu_usage_avg") {
            "process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let process_mem_expr = if columns.contains("process_memory_usage_max_mb") {
            "process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let root_process_cpu_expr = if columns.contains("root_process_cpu_usage_avg") {
            "root_process_cpu_usage_avg"
        } else {
            "NULL"
        };
        let root_process_mem_expr = if columns.contains("root_process_memory_usage_max_mb") {
            "root_process_memory_usage_max_mb"
        } else {
            "NULL"
        };
        let shared_nix_daemon_cpu_expr = if columns.contains("shared_nix_daemon_cpu_usage_avg") {
            "shared_nix_daemon_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_daemon_mem_expr =
            if columns.contains("shared_nix_daemon_memory_usage_max_mb") {
                "shared_nix_daemon_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_nix_build_cpu_expr = if columns.contains("shared_nix_build_slice_cpu_usage_avg")
        {
            "shared_nix_build_slice_cpu_usage_avg"
        } else {
            "NULL"
        };
        let shared_nix_build_mem_expr =
            if columns.contains("shared_nix_build_slice_memory_usage_max_mb") {
                "shared_nix_build_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let shared_background_cpu_expr =
            if columns.contains("shared_background_slice_cpu_usage_avg") {
                "shared_background_slice_cpu_usage_avg"
            } else {
                "NULL"
            };
        let shared_background_mem_expr =
            if columns.contains("shared_background_slice_memory_usage_max_mb") {
                "shared_background_slice_memory_usage_max_mb"
            } else {
                "NULL"
            };
        let process_count_expr = if columns.contains("process_count_max") {
            "process_count_max"
        } else {
            "NULL"
        };
        let sample_count_expr = if columns.contains("resource_sample_count") {
            "resource_sample_count"
        } else {
            "NULL"
        };
        let host_cpu_pressure_expr = if columns.contains("host_cpu_pressure_some_avg10_max") {
            "host_cpu_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_some_expr = if columns.contains("host_io_pressure_some_avg10_max") {
            "host_io_pressure_some_avg10_max"
        } else {
            "NULL"
        };
        let host_io_pressure_full_expr = if columns.contains("host_io_pressure_full_avg10_max") {
            "host_io_pressure_full_avg10_max"
        } else {
            "NULL"
        };
        let host_memory_pressure_some_expr =
            if columns.contains("host_memory_pressure_some_avg10_max") {
                "host_memory_pressure_some_avg10_max"
            } else {
                "NULL"
            };
        let host_memory_pressure_full_expr =
            if columns.contains("host_memory_pressure_full_avg10_max") {
                "host_memory_pressure_full_avg10_max"
            } else {
                "NULL"
            };
        let host_block_read_mib_expr = if columns.contains("host_block_read_mib_delta") {
            "host_block_read_mib_delta"
        } else {
            "NULL"
        };
        let host_block_write_mib_expr = if columns.contains("host_block_write_mib_delta") {
            "host_block_write_mib_delta"
        } else {
            "NULL"
        };
        let host_block_read_iops_expr = if columns.contains("host_block_read_iops_avg") {
            "host_block_read_iops_avg"
        } else {
            "NULL"
        };
        let host_block_write_iops_expr = if columns.contains("host_block_write_iops_avg") {
            "host_block_write_iops_avg"
        } else {
            "NULL"
        };
        let host_block_busiest_device_expr = if columns.contains("host_block_busiest_device") {
            "host_block_busiest_device"
        } else {
            "NULL"
        };
        let host_block_busiest_total_mib_expr =
            if columns.contains("host_block_busiest_device_total_mib_delta") {
                "host_block_busiest_device_total_mib_delta"
            } else {
                "NULL"
            };
        let host_block_busiest_read_iops_expr =
            if columns.contains("host_block_busiest_device_read_iops_avg") {
                "host_block_busiest_device_read_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_write_iops_expr =
            if columns.contains("host_block_busiest_device_write_iops_avg") {
                "host_block_busiest_device_write_iops_avg"
            } else {
                "NULL"
            };
        let host_block_busiest_weighted_expr =
            if columns.contains("host_block_busiest_device_weighted_io_ms_per_s") {
                "host_block_busiest_device_weighted_io_ms_per_s"
            } else {
                "NULL"
            };
        let shm_free_expr = if columns.contains("shm_free_min_mb") {
            "shm_free_min_mb"
        } else {
            "NULL"
        };
        let shm_used_expr = if columns.contains("shm_used_max_mb") {
            "shm_used_max_mb"
        } else {
            "NULL"
        };

        let query = format!(
            r"SELECT command,
                     status,
                     started_at,
                     duration_secs,
                     {process_cpu_expr},
                     {process_mem_expr},
                     {root_process_cpu_expr},
                     {root_process_mem_expr},
                     {shared_nix_daemon_cpu_expr},
                     {shared_nix_daemon_mem_expr},
                     {shared_nix_build_cpu_expr},
                     {shared_nix_build_mem_expr},
                     {shared_background_cpu_expr},
                     {shared_background_mem_expr},
                     {process_count_expr},
                     {sample_count_expr},
                     cpu_usage_avg,
                     memory_usage_max_mb,
                     {host_cpu_pressure_expr},
                     {host_io_pressure_some_expr},
                     {host_io_pressure_full_expr},
                     {host_memory_pressure_some_expr},
                     {host_memory_pressure_full_expr},
                     {host_block_read_mib_expr},
                     {host_block_write_mib_expr},
                     {host_block_read_iops_expr},
                     {host_block_write_iops_expr},
                     {host_block_busiest_device_expr},
                     {host_block_busiest_total_mib_expr},
                     {host_block_busiest_read_iops_expr},
                     {host_block_busiest_write_iops_expr},
                     {host_block_busiest_weighted_expr},
                     {shm_free_expr},
                     {shm_used_expr}
              FROM invocations
              WHERE id = ?1
              LIMIT 1"
        );

        let usage = self
            .conn
            .query_row(&query, params![invocation_id], |row| {
                Ok(ResourceUsage {
                    command: row.get(0)?,
                    status: row.get(1)?,
                    started_at: row.get(2)?,
                    duration_secs: row.get(3)?,
                    process_cpu_usage_avg: row.get(4)?,
                    process_memory_usage_max_mb: row.get(5)?,
                    root_process_cpu_usage_avg: row.get(6)?,
                    root_process_memory_usage_max_mb: row.get(7)?,
                    shared_nix_daemon_cpu_usage_avg: row.get(8)?,
                    shared_nix_daemon_memory_usage_max_mb: row.get(9)?,
                    shared_nix_build_slice_cpu_usage_avg: row.get(10)?,
                    shared_nix_build_slice_memory_usage_max_mb: row.get(11)?,
                    shared_background_slice_cpu_usage_avg: row.get(12)?,
                    shared_background_slice_memory_usage_max_mb: row.get(13)?,
                    process_count_max: row.get::<_, Option<i64>>(14)?.map(|value| value as u32),
                    sample_count: row.get::<_, Option<i64>>(15)?.map(|value| value as u32),
                    host_cpu_usage_avg: row.get(16)?,
                    host_memory_usage_max_mb: row.get(17)?,
                    host_cpu_pressure_some_avg10_max: row.get(18)?,
                    host_io_pressure_some_avg10_max: row.get(19)?,
                    host_io_pressure_full_avg10_max: row.get(20)?,
                    host_memory_pressure_some_avg10_max: row.get(21)?,
                    host_memory_pressure_full_avg10_max: row.get(22)?,
                    host_block_read_mib_delta: row.get(23)?,
                    host_block_write_mib_delta: row.get(24)?,
                    host_block_read_iops_avg: row.get(25)?,
                    host_block_write_iops_avg: row.get(26)?,
                    host_block_busiest_device: row.get(27)?,
                    host_block_busiest_device_total_mib_delta: row.get(28)?,
                    host_block_busiest_device_read_iops_avg: row.get(29)?,
                    host_block_busiest_device_write_iops_avg: row.get(30)?,
                    host_block_busiest_device_weighted_io_ms_per_s: row.get(31)?,
                    shm_free_min_mb: row.get(32)?,
                    shm_used_max_mb: row.get(33)?,
                })
            })
            .optional()
            .context("failed to get resource usage for invocation")?;

        Ok(usage.filter(ResourceUsage::has_samples))
    }

    // ──────────────────────────────────────────────────────────────────────
    // G2: Stage Analytics — slowest stages and per-stage trend
    // ──────────────────────────────────────────────────────────────────────

    /// Get aggregate stage timing statistics (G2 — slowest stages view).
    ///
    /// Returns stages sorted by average duration descending.
    pub fn get_slowest_stages(
        &self,
        command_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<StageStats>> {
        let mut query = String::from(
            r"
            SELECT
                st.stage_name,
                AVG(st.duration_secs) as avg_duration,
                MAX(st.duration_secs) as max_duration,
                COUNT(*) as run_count
            FROM stage_timings st
            JOIN invocations i ON st.invocation_id = i.id
            WHERE 1=1
            ",
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut param_idx = 1usize;

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(
            " GROUP BY st.stage_name
              ORDER BY avg_duration DESC
              LIMIT ?{param_idx}"
        ));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |row| {
            Ok(StageStats {
                stage_name: row.get(0)?,
                avg_duration_secs: row.get(1)?,
                max_duration_secs: row.get(2)?,
                run_count: row.get::<_, i64>(3)? as usize,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get timing trend for a specific stage over recent invocations (G2).
    pub fn get_stage_trend(
        &self,
        stage_name: &str,
        command_filter: Option<&str>,
        window: usize,
    ) -> Result<Vec<StageTrendPoint>> {
        let mut query = String::from(
            r"
            SELECT
                st.invocation_id,
                i.started_at,
                st.duration_secs,
                st.success
            FROM stage_timings st
            JOIN invocations i ON st.invocation_id = i.id
            WHERE st.stage_name = ?1
            ",
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        params_vec.push(Box::new(stage_name.to_string()));
        let mut param_idx = 2usize;

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(" ORDER BY i.started_at DESC LIMIT ?{param_idx}"));
        params_vec.push(Box::new(window as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(params_refs), |row| {
            let success_int: i32 = row.get(3)?;
            Ok(StageTrendPoint {
                invocation_id: row.get(0)?,
                started_at: row.get(1)?,
                duration_secs: row.get(2)?,
                success: success_int != 0,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        results.reverse(); // Chronological order
        Ok(results)
    }
}
