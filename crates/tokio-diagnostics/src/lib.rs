//! Opt-in Tokio diagnostics for the AgentENV server: tokio-console task
//! inspection, periodic runtime metrics logging, and wall-clock tracing
//! flame graphs.
//!
//! Everything is disabled unless turned on via `[tokio_diagnostics]` in the
//! config. Requires `--cfg tokio_unstable` (set workspace-wide in
//! `.cargo/config.toml`). Intended as a temporary performance-analysis aid
//! for hunting serialization under load.
//!
//! Integration surface (kept minimal on purpose):
//! - `AppConfig` gains one nested `TokioDiagnosticsConfig` field.
//! - `main` builds the runtime via [`build_runtime`] instead of
//!   `#[tokio::main]`, then calls [`start`] once the config is loaded.
//! - The logging facade installs the [`layers`] output while building the
//!   subscriber; per-layer filters cannot be hot-plugged into a live
//!   subscriber (console-subscriber panics; see `logging::init`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use confique::Config;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tracing::info;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::{EnvFilter, Layer, Registry};

#[derive(Debug, Config, Clone)]
pub struct TokioDiagnosticsConfig {
    /// Live async task inspection via tokio-console.
    #[config(nested)]
    pub console: TokioConsoleConfig,
    /// Periodic Tokio runtime metrics logging (poll times, queue depths).
    #[config(nested)]
    pub runtime_metrics: TokioRuntimeMetricsConfig,
    /// Wall-clock tracing flame graph output.
    #[config(nested)]
    pub flame: TracingFlameConfig,
}

impl TokioDiagnosticsConfig {
    /// Expand the `$AENV_HOME` placeholder in the output paths, then anchor
    /// them at `config_dir` when still relative.
    pub fn normalize_paths(&mut self, home_path: &Path, config_dir: &Path) {
        self.flame.output_path = expand_path(&self.flame.output_path, home_path, config_dir);
        self.runtime_metrics.output_path =
            expand_path(&self.runtime_metrics.output_path, home_path, config_dir);
    }
}

fn expand_path(raw: &Path, home_path: &Path, config_dir: &Path) -> PathBuf {
    const HOME_PLACEHOLDER: &str = "$AENV_HOME";
    let expanded = match raw.to_str() {
        Some(s) if s.contains(HOME_PLACEHOLDER) => {
            PathBuf::from(s.replace(HOME_PLACEHOLDER, &home_path.to_string_lossy()))
        }
        _ => raw.to_path_buf(),
    };
    if expanded.is_absolute() {
        expanded
    } else {
        config_dir.join(expanded)
    }
}

#[derive(Debug, Config, Clone)]
pub struct TokioConsoleConfig {
    /// Serve tokio-console data from the server process. Connect with the
    /// `tokio-console` CLI. Off by default; has tracing overhead when on.
    #[config(default = false, env = "AENV_TOKIO_CONSOLE_ENABLED")]
    pub enabled: bool,
    #[config(default = "0.0.0.0:6669", env = "AENV_TOKIO_CONSOLE_BIND_ADDR")]
    pub bind_addr: String,
}

#[derive(Debug, Config, Clone)]
pub struct TokioRuntimeMetricsConfig {
    /// Append a Tokio runtime metrics summary line to `output_path` every
    /// `interval_secs` (kept out of the main log on purpose).
    #[config(default = false, env = "AENV_TOKIO_METRICS_ENABLED")]
    pub enabled: bool,
    #[config(default = 10u64, env = "AENV_TOKIO_METRICS_INTERVAL_SECS")]
    pub interval_secs: u64,
    #[config(
        default = "$AENV_HOME/logs/tokio-metrics.log",
        env = "AENV_TOKIO_METRICS_OUTPUT",
        parse_env = parse_required_path
    )]
    pub output_path: PathBuf,
}

#[derive(Debug, Config, Clone)]
pub struct TracingFlameConfig {
    /// Write folded-stack flame graph data while the server runs. Wall-clock
    /// (not CPU) based; use `inferno-flamegraph` to render. Off by default.
    #[config(default = false, env = "AENV_TRACING_FLAME_ENABLED")]
    pub enabled: bool,
    #[config(
        default = "$AENV_HOME/logs/tracing-flame.folded",
        env = "AENV_TRACING_FLAME_PATH",
        parse_env = parse_required_path
    )]
    pub output_path: PathBuf,
}

fn parse_required_path(raw: &str) -> std::result::Result<PathBuf, std::convert::Infallible> {
    Ok(PathBuf::from(raw.trim()))
}

/// Guards the non-layer pieces of the diagnostics stack.
///
/// Dropping it stops flushing flame-graph data; [`start`] parks it in a
/// task for the runtime lifetime. The tokio-console server driver is taken
/// out via [`DiagnosticsGuard::take_console_server`].
#[derive(Default)]
pub struct DiagnosticsGuard {
    console_server: Option<console_subscriber::Server>,
    flame_guard: Option<tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>>,
}

impl DiagnosticsGuard {
    /// Take the tokio-console server driver, if console diagnostics are enabled.
    pub fn take_console_server(&mut self) -> Option<console_subscriber::Server> {
        self.console_server.take()
    }
}

/// Build the diagnostics tracing layers selected by `config`, plus the guard
/// owning their non-layer resources.
///
/// `filter` (the same env filter used for fmt logs) is applied to the flame
/// layer; the console layer gets its own `tokio=trace,runtime=trace` filter
/// so tokio's TRACE task instrumentation never reaches the log output.
/// Layers that fail to initialize are skipped with a stderr warning.
pub fn layers(
    config: &TokioDiagnosticsConfig,
    filter: &EnvFilter,
) -> (
    Vec<Box<dyn Layer<Registry> + Send + Sync>>,
    DiagnosticsGuard,
) {
    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();
    let mut guard = DiagnosticsGuard::default();

    if config.flame.enabled {
        if let Some(parent) = config.flame.output_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match tracing_flame::FlameLayer::with_file(&config.flame.output_path) {
            Ok((flame_layer, flush_guard)) => {
                guard.flame_guard = Some(flush_guard);
                layers.push(flame_layer.with_filter(filter.clone()).boxed());
            }
            Err(err) => eprintln!(
                "failed to create tracing flame output {}: {err}",
                config.flame.output_path.display()
            ),
        }
    }

    if config.console.enabled {
        match config.console.bind_addr.parse::<SocketAddr>() {
            Ok(addr) => {
                let targets = Targets::new()
                    .with_target("tokio", tracing::Level::TRACE)
                    .with_target("runtime", tracing::Level::TRACE);
                let (console_layer, server) = console_subscriber::ConsoleLayer::builder()
                    .server_addr(addr)
                    .build();
                guard.console_server = Some(server);
                layers.push(console_layer.with_filter(targets).boxed());
            }
            Err(err) => eprintln!(
                "invalid tokio_diagnostics.console.bind_addr {:?}: {err}",
                config.console.bind_addr
            ),
        }
    }

    (layers, guard)
}

/// Build the multi-thread Tokio runtime for the server.
///
/// Equivalent to `#[tokio::main]` plus the poll-time histogram backing the
/// metrics reporter's poll durations. The histogram is always enabled (a
/// few atomic ops per task poll) so callers need no config access before
/// the runtime exists.
pub fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    builder.enable_metrics_poll_time_histogram();
    builder.build()
}

/// Activate the selected diagnostics on the current runtime: spawn the
/// tokio-console server driver and the runtime metrics reporter, and park
/// `guard` in a task so flame data keeps flushing until shutdown.
pub fn start(config: &TokioDiagnosticsConfig, mut guard: DiagnosticsGuard) {
    if let Some(server) = guard.take_console_server() {
        info!(
            target: "agentenv::tokio_diagnostics",
            addr = %config.console.bind_addr,
            "tokio-console diagnostics enabled"
        );
        tokio::spawn(server.serve());
    }
    if config.runtime_metrics.enabled {
        info!(
            target: "agentenv::tokio_diagnostics",
            path = %config.runtime_metrics.output_path.display(),
            interval_secs = config.runtime_metrics.interval_secs,
            "tokio runtime metrics enabled (follow with `tail -f` on the output file)"
        );
        spawn_runtime_metrics_reporter(
            &Handle::current(),
            Duration::from_secs(config.runtime_metrics.interval_secs.max(1)),
            config.runtime_metrics.output_path.clone(),
        );
    }
    tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
}

/// Append a compact Tokio runtime metrics summary line to `output_path`
/// every `interval` — deliberately separate from the main log so it is not
/// flooded away; follow with `tail -f`.
///
/// Reading guide when hunting serialization under load:
/// - `worker_max_mean_poll_us` high while `busy_ratio` is low: a blocking
///   call is stalling one runtime worker (e.g. sync I/O or `block_on` on a
///   runtime thread).
/// - `global_queue_depth` / `max_local_queue_depth` deep while `busy_ratio`
///   is low: tasks pile up behind a serialization point (a lock, a single
///   consumer, a dedicated thread) rather than behind the scheduler.
/// - `blocking_queue_depth` growing: the `spawn_blocking` pool is saturated.
fn spawn_runtime_metrics_reporter(handle: &Handle, interval: Duration, output_path: PathBuf) {
    let monitor = tokio_metrics::RuntimeMonitor::new(handle);
    tokio::spawn(async move {
        if let Some(parent) = output_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)
            .await
        {
            Ok(file) => file,
            Err(err) => {
                eprintln!(
                    "failed to open tokio metrics output {}: {err}",
                    output_path.display()
                );
                return;
            }
        };

        let mut ticker = tokio::time::interval(interval);
        for metrics in monitor.intervals() {
            ticker.tick().await;
            let workers = metrics.workers_count.max(1) as f64;
            let busy_ratio = metrics.total_busy_duration.as_secs_f64()
                / (metrics.elapsed.as_secs_f64() * workers);
            let timestamp = time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default();
            let line = format!(
                "{timestamp} workers={} live_tasks={} busy_ratio={busy_ratio:.3} \
                 mean_poll_us={} worker_max_mean_poll_us={} total_polls={} \
                 global_queue_depth={} max_local_queue_depth={} blocking_queue_depth={} \
                 overflow_count={} budget_forced_yields={} io_driver_ready={}\n",
                metrics.workers_count,
                metrics.live_tasks_count,
                metrics.mean_poll_duration.as_micros() as u64,
                metrics.mean_poll_duration_worker_max.as_micros() as u64,
                metrics.total_polls_count,
                metrics.global_queue_depth,
                metrics.max_local_queue_depth,
                metrics.blocking_queue_depth,
                metrics.total_overflow_count,
                metrics.budget_forced_yield_count,
                metrics.io_driver_ready_count,
            );
            if let Err(err) = file.write_all(line.as_bytes()).await {
                eprintln!(
                    "failed to write tokio metrics to {}: {err}",
                    output_path.display()
                );
                return;
            }
        }
    });
}
