use std::{
    collections::{BTreeMap, VecDeque},
    io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid as UnixPid,
};
use reqwest::Client;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    net::{UnixListener, UnixStream},
    process::{Child, Command},
    sync::{mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval, sleep},
};

use crate::{
    config::{Config, ServiceConfig},
    ipc::{
        ClientMessage, LogEntry, ProcessAction, ServerMessage, ServiceSnapshot, ServiceState,
        read_message, write_message,
    },
};

const RECENT_LOG_LIMIT: usize = 512;
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(3);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const HTTP_PROBE_TIMEOUT: Duration = Duration::from_millis(750);

pub async fn run(config_path: PathBuf, socket_path: PathBuf, state_dir: PathBuf) -> Result<()> {
    tokio::fs::create_dir_all(&state_dir)
        .await
        .with_context(|| format!("failed to create {}", state_dir.display()))?;

    if tokio::fs::try_exists(&socket_path).await? {
        tokio::fs::remove_file(&socket_path)
            .await
            .with_context(|| format!("failed to remove stale {}", socket_path.display()))?;
    }

    let config = Config::load(&config_path)?;
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("failed to bind {}", socket_path.display()))?;
    let http_client = Client::builder()
        .timeout(HTTP_PROBE_TIMEOUT)
        .build()
        .context("failed to create HTTP client")?;

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let root_dir = config_path.parent().unwrap_or(Path::new(".")).to_path_buf();

    let mut manager = Manager {
        root_dir,
        socket_path,
        config,
        services: BTreeMap::new(),
        recent_logs: VecDeque::with_capacity(RECENT_LOG_LIMIT),
        subscribers: BTreeMap::new(),
        next_subscriber_id: 0,
        event_tx: event_tx.clone(),
        event_rx,
        http_client,
        system: System::new(),
        shutdown_requested: false,
    };
    manager.initialize_services();

    let run_result = manager.run_loop(listener).await;
    let cleanup_result = tokio::fs::remove_file(&manager.socket_path).await;

    match (run_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        (Ok(()), Err(error)) => Err(error)
            .with_context(|| format!("failed to remove {}", manager.socket_path.display())),
        (Err(error), _) => Err(error),
    }
}

struct Manager {
    root_dir: PathBuf,
    socket_path: PathBuf,
    config: Config,
    services: BTreeMap<String, ManagedService>,
    recent_logs: VecDeque<LogEntry>,
    subscribers: BTreeMap<usize, mpsc::UnboundedSender<ServerMessage>>,
    next_subscriber_id: usize,
    event_tx: mpsc::UnboundedSender<ManagerEvent>,
    event_rx: mpsc::UnboundedReceiver<ManagerEvent>,
    http_client: Client,
    system: System,
    shutdown_requested: bool,
}

#[derive(Debug)]
struct ManagedService {
    config: ServiceConfig,
    child: Option<Child>,
    pid: Option<u32>,
    state: ServiceState,
    ready: bool,
    manually_stopped: bool,
    cpu_percent: f32,
    memory_bytes: u64,
    last_exit_code: Option<i32>,
}

#[derive(Debug)]
enum ManagerEvent {
    Attach {
        sender: mpsc::UnboundedSender<ServerMessage>,
        respond_to: oneshot::Sender<ServerMessage>,
    },
    Command {
        command: ControlCommand,
        respond_to: oneshot::Sender<ServerMessage>,
    },
    Log {
        service: String,
        line: String,
    },
    ShutdownSignal,
}

#[derive(Debug)]
enum ControlCommand {
    Shutdown,
    AllAction {
        action: ProcessAction,
    },
    ProcessAction {
        service: String,
        action: ProcessAction,
    },
}

impl Manager {
    fn initialize_services(&mut self) {
        for (name, config) in &self.config.services {
            self.services.insert(
                name.clone(),
                ManagedService {
                    config: config.clone(),
                    child: None,
                    pid: None,
                    state: ServiceState::Stopped,
                    ready: false,
                    manually_stopped: false,
                    cpu_percent: 0.0,
                    memory_bytes: 0,
                    last_exit_code: None,
                },
            );
        }
    }

    async fn run_loop(&mut self, listener: UnixListener) -> Result<()> {
        self.initialize_runtime().await?;
        let signal_tx = self.event_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = signal_tx.send(ManagerEvent::ShutdownSignal);
            }
        });

        self.run_event_loop(listener).await
    }

    async fn initialize_runtime(&mut self) -> Result<()> {
        if let Err(error) = self.start_all().await {
            return self.cleanup_failed_initialization(error).await;
        }
        self.refresh_metrics();
        if let Err(error) = self.refresh_http_probes().await {
            return self.cleanup_failed_initialization(error).await;
        }

        Ok(())
    }

    async fn cleanup_failed_initialization(&mut self, error: anyhow::Error) -> Result<()> {
        match self.stop_all().await {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(error.context(format!(
                "manager initialization cleanup failed: {cleanup_error}"
            ))),
        }
    }

    async fn run_event_loop(&mut self, listener: UnixListener) -> Result<()> {
        let mut exit_poll = interval(EXIT_POLL_INTERVAL);
        exit_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut status_refresh = interval(STATUS_REFRESH_INTERVAL);
        status_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            if self.shutdown_requested {
                self.broadcast(ServerMessage::ManagerStopping);
                self.stop_all().await?;
                return Ok(());
            }

            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, _) = accept_result.context("failed to accept client connection")?;
                    let event_tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let _ = handle_client(stream, event_tx).await;
                    });
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event).await?;
                }
                _ = exit_poll.tick() => {
                    self.poll_process_exits()?;
                }
                _ = status_refresh.tick() => {
                    self.refresh_metrics();
                    self.refresh_http_probes().await?;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: ManagerEvent) -> Result<()> {
        match event {
            ManagerEvent::Attach { sender, respond_to } => {
                if self.shutdown_requested {
                    let _ = respond_to.send(ServerMessage::Error {
                        message: "pm is shutting down".to_owned(),
                    });
                } else {
                    self.next_subscriber_id += 1;
                    self.subscribers.insert(self.next_subscriber_id, sender);
                    let _ = respond_to.send(self.snapshot_message());
                }
            }
            ManagerEvent::Command {
                command,
                respond_to,
            } => {
                let response = match command {
                    ControlCommand::Shutdown => {
                        self.shutdown_requested = true;
                        ServerMessage::Ack {
                            message: "pm stopped".to_owned(),
                        }
                    }
                    ControlCommand::AllAction { action } => {
                        match self.handle_all_process_action(action).await {
                            Ok(message) => ServerMessage::Ack { message },
                            Err(error) => ServerMessage::Error {
                                message: error.to_string(),
                            },
                        }
                    }
                    ControlCommand::ProcessAction { service, action } => {
                        match self.handle_process_action(&service, action).await {
                            Ok(message) => ServerMessage::Ack { message },
                            Err(error) => ServerMessage::Error {
                                message: error.to_string(),
                            },
                        }
                    }
                };
                let _ = respond_to.send(response);
            }
            ManagerEvent::Log { service, line } => self.handle_log(service, line),
            ManagerEvent::ShutdownSignal => {
                self.shutdown_requested = true;
            }
        }
        Ok(())
    }

    async fn handle_all_process_action(&mut self, action: ProcessAction) -> Result<String> {
        match action {
            ProcessAction::Restart => {
                self.stop_all().await?;
                self.start_all().await?;
                Ok("restarted all services".to_owned())
            }
            ProcessAction::Stop => {
                self.stop_all().await?;
                Ok("stopped all services".to_owned())
            }
            ProcessAction::Start => {
                self.start_all().await?;
                Ok("started all services".to_owned())
            }
        }
    }

    async fn handle_process_action(
        &mut self,
        service: &str,
        action: ProcessAction,
    ) -> Result<String> {
        match action {
            ProcessAction::Restart => {
                self.restart_service(service).await?;
                Ok(format!("restarted {service}"))
            }
            ProcessAction::Stop => {
                self.stop_service(service).await?;
                Ok(format!("stopped {service}"))
            }
            ProcessAction::Start => {
                self.start_service(service).await?;
                Ok(format!("started {service}"))
            }
        }
    }

    async fn start_all(&mut self) -> Result<()> {
        let names = self.services.keys().cloned().collect::<Vec<_>>();
        for name in names {
            self.start_service(&name).await?;
        }
        Ok(())
    }

    async fn stop_all(&mut self) -> Result<()> {
        let names = self.services.keys().cloned().collect::<Vec<_>>();
        let mut first_error = None;
        for name in names {
            if let Err(error) = self.stop_service(&name).await {
                if first_error.is_none() {
                    first_error = Some(error.context(format!("failed to stop service `{name}`")));
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    async fn restart_service(&mut self, name: &str) -> Result<()> {
        self.stop_service(name).await?;
        self.start_service(name).await
    }

    async fn start_service(&mut self, name: &str) -> Result<()> {
        let Some(service) = self.services.get_mut(name) else {
            bail!("unknown service `{name}`");
        };

        if service.child.is_some() {
            return Ok(());
        }

        let mut command = Command::new("sh");
        let working_dir = service
            .config
            .working_dir
            .as_ref()
            .map(|path| {
                if path.is_absolute() {
                    path.clone()
                } else {
                    self.root_dir.join(path)
                }
            })
            .unwrap_or_else(|| self.root_dir.clone());
        command
            .arg("-lc")
            .arg(&service.config.command)
            .current_dir(working_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);

        for (key, value) in &service.config.env {
            command.env(key, value);
        }

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start service `{name}`"))?;

        if let Some(stdout) = child.stdout.take() {
            spawn_log_reader(name.to_owned(), stdout, self.event_tx.clone());
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_reader(name.to_owned(), stderr, self.event_tx.clone());
        }

        service.pid = child.id();
        service.child = Some(child);
        service.manually_stopped = false;
        service.last_exit_code = None;
        if has_readiness_probe(&service.config) {
            service.ready = false;
            service.state = ServiceState::Starting;
        } else {
            service.ready = true;
            service.state = ServiceState::Running;
        }
        self.broadcast_service_update(name);
        Ok(())
    }

    async fn stop_service(&mut self, name: &str) -> Result<()> {
        let pid = {
            let Some(service) = self.services.get_mut(name) else {
                bail!("unknown service `{name}`");
            };

            if service.child.is_none() {
                service.manually_stopped = true;
                service.ready = false;
                service.state = ServiceState::Stopped;
                service.cpu_percent = 0.0;
                service.memory_bytes = 0;
                self.broadcast_service_update(name);
                return Ok(());
            }

            service.manually_stopped = true;
            service.pid
        };

        if let Some(pid) = pid {
            let _ = kill_process_group(pid, Signal::SIGTERM);
        }

        let deadline = Instant::now() + STOP_GRACE_PERIOD;
        loop {
            if self.try_finalize_exit(name)? {
                self.mark_stopped(name);
                self.broadcast_service_update(name);
                return Ok(());
            }
            if Instant::now() >= deadline {
                break;
            }
            sleep(STOP_POLL_INTERVAL).await;
        }

        if let Some(pid) = self.services.get(name).and_then(|service| service.pid) {
            let _ = kill_process_group(pid, Signal::SIGKILL);
        }

        let child = self
            .services
            .get_mut(name)
            .and_then(|service| service.child.take());
        if let Some(mut child) = child {
            let status = child
                .wait()
                .await
                .context("failed to wait on child process")?;
            self.finish_exit(name, status.code(), true);
        } else {
            self.mark_stopped(name);
        }
        self.broadcast_service_update(name);
        Ok(())
    }

    fn poll_process_exits(&mut self) -> Result<()> {
        let names = self.services.keys().cloned().collect::<Vec<_>>();
        for name in names {
            self.try_finalize_exit(&name)?;
        }
        Ok(())
    }

    fn try_finalize_exit(&mut self, name: &str) -> Result<bool> {
        let exit_code = {
            let Some(service) = self.services.get_mut(name) else {
                return Ok(false);
            };
            let Some(child) = service.child.as_mut() else {
                return Ok(false);
            };
            match child
                .try_wait()
                .with_context(|| format!("failed to poll `{name}`"))?
            {
                Some(status) => Some(status.code()),
                None => None,
            }
        };

        if let Some(exit_code) = exit_code {
            let was_manual = self
                .services
                .get(name)
                .map(|service| service.manually_stopped)
                .unwrap_or(false);
            self.finish_exit(name, exit_code, was_manual);
            self.broadcast_service_update(name);
            return Ok(true);
        }
        Ok(false)
    }

    fn finish_exit(&mut self, name: &str, exit_code: Option<i32>, manual_stop: bool) {
        if let Some(service) = self.services.get_mut(name) {
            service.child = None;
            service.pid = None;
            service.ready = false;
            service.cpu_percent = 0.0;
            service.memory_bytes = 0;
            service.last_exit_code = exit_code;
            service.state = if manual_stop {
                ServiceState::Stopped
            } else {
                ServiceState::Exited
            };
        }
    }

    fn mark_stopped(&mut self, name: &str) {
        if let Some(service) = self.services.get_mut(name) {
            service.child = None;
            service.pid = None;
            service.ready = false;
            service.cpu_percent = 0.0;
            service.memory_bytes = 0;
            service.state = ServiceState::Stopped;
        }
    }

    fn handle_log(&mut self, service_name: String, line: String) {
        if let Some(service) = self.services.get_mut(&service_name) {
            if let Some(needle) = &service.config.ready_probe_log_line_contains {
                if !service.ready && line.contains(needle) {
                    service.ready = true;
                    service.state = ServiceState::Running;
                    self.broadcast_service_update(&service_name);
                }
            }
        }

        let entry = LogEntry {
            service: service_name,
            line,
        };
        if self.recent_logs.len() == RECENT_LOG_LIMIT {
            self.recent_logs.pop_front();
        }
        self.recent_logs.push_back(entry.clone());
        self.broadcast(ServerMessage::Log { entry });
    }

    async fn refresh_http_probes(&mut self) -> Result<()> {
        let targets = self
            .services
            .iter()
            .filter_map(|(name, service)| {
                let url = service.config.ready_probe_http.clone()?;
                if service.child.is_some() {
                    Some((name.clone(), url))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for (name, url) in targets {
            let ready = match self.http_client.get(url).send().await {
                Ok(response) => {
                    response.status().is_success() || response.status().is_redirection()
                }
                Err(_) => false,
            };

            let mut changed = false;
            if let Some(service) = self.services.get_mut(&name) {
                if service.ready != ready {
                    service.ready = ready;
                    service.state = if ready {
                        ServiceState::Running
                    } else {
                        ServiceState::Starting
                    };
                    changed = true;
                }
            }
            if changed {
                self.broadcast_service_update(&name);
            }
        }

        Ok(())
    }

    fn refresh_metrics(&mut self) {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing()
                .with_cpu()
                .with_memory()
                .without_tasks(),
        );

        let names = self.services.keys().cloned().collect::<Vec<_>>();
        for name in names {
            let Some(pid) = self.services.get(&name).and_then(|service| service.pid) else {
                continue;
            };
            let root_pid = Pid::from_u32(pid);
            let (cpu_percent, memory_bytes) = collect_process_tree_metrics(&self.system, root_pid);
            let mut changed = false;

            if let Some(service) = self.services.get_mut(&name) {
                if (service.cpu_percent - cpu_percent).abs() > f32::EPSILON {
                    service.cpu_percent = cpu_percent;
                    changed = true;
                }
                if service.memory_bytes != memory_bytes {
                    service.memory_bytes = memory_bytes;
                    changed = true;
                }
            }

            if changed {
                self.broadcast_service_update(&name);
            }
        }
    }

    fn snapshot_message(&self) -> ServerMessage {
        ServerMessage::Snapshot {
            services: self
                .services
                .keys()
                .map(|name| self.snapshot_for(name))
                .collect(),
            recent_logs: self.recent_logs.iter().cloned().collect(),
            max_name_width: self.config.max_name_width(),
        }
    }

    fn snapshot_for(&self, name: &str) -> ServiceSnapshot {
        let service = &self.services[name];
        ServiceSnapshot {
            name: name.to_owned(),
            pid: service.pid,
            state: service.state.clone(),
            ready: service.ready,
            cpu_percent: service.cpu_percent,
            memory_bytes: service.memory_bytes,
            last_exit_code: service.last_exit_code,
        }
    }

    fn broadcast_service_update(&mut self, name: &str) {
        let snapshot = self.snapshot_for(name);
        self.broadcast(ServerMessage::ServiceUpdate { service: snapshot });
    }

    fn broadcast(&mut self, message: ServerMessage) {
        self.subscribers
            .retain(|_, sender| sender.send(message.clone()).is_ok());
    }
}

fn spawn_log_reader<R>(
    service_name: String,
    reader: R,
    event_tx: mpsc::UnboundedSender<ManagerEvent>,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let line = line.trim_end_matches('\r').to_owned();
                    let _ = event_tx.send(ManagerEvent::Log {
                        service: service_name.clone(),
                        line,
                    });
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });
}

async fn handle_client(
    stream: UnixStream,
    event_tx: mpsc::UnboundedSender<ManagerEvent>,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let Some(message) = read_message::<_, ClientMessage>(&mut reader).await? else {
        return Ok(());
    };

    match message {
        ClientMessage::Attach => {
            let (subscriber_tx, mut subscriber_rx) = mpsc::unbounded_channel();
            let (response_tx, response_rx) = oneshot::channel();
            event_tx
                .send(ManagerEvent::Attach {
                    sender: subscriber_tx,
                    respond_to: response_tx,
                })
                .map_err(|_| anyhow!("pm manager is no longer running"))?;

            let response = response_rx
                .await
                .map_err(|_| anyhow!("pm manager dropped the attach request"))?;
            let should_stream = matches!(response, ServerMessage::Snapshot { .. });
            write_message(&mut write_half, &response).await?;

            if should_stream {
                while let Some(message) = subscriber_rx.recv().await {
                    if write_message(&mut write_half, &message).await.is_err() {
                        break;
                    }
                    if matches!(message, ServerMessage::ManagerStopping) {
                        break;
                    }
                }
            }
        }
        ClientMessage::Ping => {
            write_message(&mut write_half, &ServerMessage::Ready).await?;
        }
        ClientMessage::Shutdown => {
            respond_to_command(&mut write_half, &event_tx, ControlCommand::Shutdown).await?;
        }
        ClientMessage::AllAction { action } => {
            respond_to_command(
                &mut write_half,
                &event_tx,
                ControlCommand::AllAction { action },
            )
            .await?;
        }
        ClientMessage::ProcessAction { service, action } => {
            respond_to_command(
                &mut write_half,
                &event_tx,
                ControlCommand::ProcessAction { service, action },
            )
            .await?;
        }
    }

    Ok(())
}

async fn respond_to_command<W>(
    writer: &mut W,
    event_tx: &mpsc::UnboundedSender<ManagerEvent>,
    command: ControlCommand,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let (response_tx, response_rx) = oneshot::channel();
    event_tx
        .send(ManagerEvent::Command {
            command,
            respond_to: response_tx,
        })
        .map_err(|_| anyhow!("pm manager is no longer running"))?;

    let response = response_rx
        .await
        .map_err(|_| anyhow!("pm manager dropped the command"))?;
    write_message(writer, &response).await
}

fn has_readiness_probe(config: &ServiceConfig) -> bool {
    config.ready_probe_http.is_some() || config.ready_probe_log_line_contains.is_some()
}

fn kill_process_group(pid: u32, signal: Signal) -> Result<()> {
    killpg(UnixPid::from_raw(pid as i32), signal)
        .with_context(|| format!("failed to send {signal:?} to process group {pid}"))?;
    Ok(())
}

fn collect_process_tree_metrics(system: &System, root_pid: Pid) -> (f32, u64) {
    let mut stack = vec![root_pid];
    let mut visited = Vec::new();
    let processes = system.processes();
    let mut cpu_percent = 0.0;
    let mut memory_bytes = 0;

    while let Some(pid) = stack.pop() {
        if visited.contains(&pid) {
            continue;
        }
        visited.push(pid);

        if let Some(process) = processes.get(&pid) {
            cpu_percent += process.cpu_usage();
            memory_bytes += process.memory();
        }

        for (child_pid, process) in processes {
            if process.parent() == Some(pid) {
                stack.push(*child_pid);
            }
        }
    }

    (cpu_percent, memory_bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use reqwest::Client;

    use super::*;
    use crate::config::{Config, ServiceConfig};

    fn should_run_env_sensitive_tests() -> bool {
        std::env::var_os("PM_RUN_ENV_SENSITIVE_TESTS").is_some()
    }

    #[tokio::test]
    async fn failed_initialization_stops_services_that_already_started() {
        if !should_run_env_sensitive_tests() {
            eprintln!("skipping env-sensitive test; set PM_RUN_ENV_SENSITIVE_TESTS=1 to run it");
            return;
        }

        let root_dir = tempfile::tempdir().unwrap();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let mut manager = Manager {
            root_dir: root_dir.path().to_path_buf(),
            socket_path: root_dir.path().join("manager.sock"),
            config: Config {
                services: BTreeMap::from([
                    (
                        "api".to_owned(),
                        ServiceConfig {
                            command: "sleep 30".to_owned(),
                            working_dir: None,
                            ready_probe_http: None,
                            ready_probe_log_line_contains: None,
                            env: BTreeMap::new(),
                        },
                    ),
                    (
                        "web".to_owned(),
                        ServiceConfig {
                            command: "sleep 30".to_owned(),
                            working_dir: Some("missing-dir".into()),
                            ready_probe_http: None,
                            ready_probe_log_line_contains: None,
                            env: BTreeMap::new(),
                        },
                    ),
                ]),
            },
            services: BTreeMap::new(),
            recent_logs: VecDeque::with_capacity(RECENT_LOG_LIMIT),
            subscribers: BTreeMap::new(),
            next_subscriber_id: 0,
            event_tx,
            event_rx,
            http_client: Client::builder()
                .timeout(HTTP_PROBE_TIMEOUT)
                .build()
                .unwrap(),
            system: System::new(),
            shutdown_requested: false,
        };
        manager.initialize_services();

        let error = manager.initialize_runtime().await.unwrap_err();

        assert!(error.to_string().contains("failed to start service `web`"));
        assert!(manager.services["api"].child.is_none());
        assert!(manager.services["api"].pid.is_none());
        assert_eq!(manager.services["api"].state, ServiceState::Stopped);
        assert!(manager.services["web"].child.is_none());
    }
}
