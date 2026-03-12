mod attach;
mod config;
mod ipc;
mod manager;

use std::{
    env,
    path::{Path, PathBuf},
    process::{Child, ExitStatus, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use ipc::{ClientMessage, ProcessAction, ServerMessage, read_message, write_message};
use tokio::{
    io::BufReader,
    net::UnixStream,
    time::{Instant, sleep, timeout},
};

#[derive(Debug, Parser)]
#[command(name = "pm", version, about = "Local development process manager")]
struct Cli {
    #[arg(global = true, short, long, default_value = "pm.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Up {
        #[arg(short = 'a', long = "attach")]
        attach: bool,
    },
    Down,
    Attach,
    Process {
        name: String,
        #[arg(value_enum)]
        action: ProcessActionArg,
    },
    #[command(name = "__manager", hide = true)]
    InternalManager {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        socket: PathBuf,
        #[arg(long)]
        state_dir: PathBuf,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, ValueEnum)]
enum ProcessActionArg {
    Restart,
    Stop,
    Start,
}

impl From<ProcessActionArg> for ProcessAction {
    fn from(value: ProcessActionArg) -> Self {
        match value {
            ProcessActionArg::Restart => Self::Restart,
            ProcessActionArg::Stop => Self::Stop,
            ProcessActionArg::Start => Self::Start,
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimePaths {
    config_path: PathBuf,
    root_dir: PathBuf,
    state_dir: PathBuf,
    socket_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::InternalManager {
            config,
            socket,
            state_dir,
        } => manager::run(config, socket, state_dir).await,
        command => {
            let runtime = resolve_runtime_paths(&cli.config)?;
            match command {
                Command::Up { attach } => command_up(&runtime, attach).await,
                Command::Down => command_down(&runtime).await,
                Command::Attach => attach::run(&runtime.socket_path).await,
                Command::Process { name, action } => {
                    command_process(&runtime, &name, action.into()).await
                }
                Command::InternalManager { .. } => unreachable!(),
            }
        }
    }
}

fn resolve_runtime_paths(config_arg: &Path) -> Result<RuntimePaths> {
    let cwd = env::current_dir().context("failed to resolve current directory")?;
    let config_path = if config_arg.is_absolute() {
        config_arg.to_path_buf()
    } else {
        cwd.join(config_arg)
    };
    let root_dir = config_path.parent().unwrap_or(cwd.as_path()).to_path_buf();
    let state_dir = root_dir.join(".pm");
    let socket_path = state_dir.join("manager.sock");

    Ok(RuntimePaths {
        config_path,
        root_dir,
        state_dir,
        socket_path,
    })
}

async fn command_up(runtime: &RuntimePaths, attach: bool) -> Result<()> {
    let _config = config::Config::load(&runtime.config_path)?;

    tokio::fs::create_dir_all(&runtime.state_dir)
        .await
        .with_context(|| format!("failed to create {}", runtime.state_dir.display()))?;

    if socket_is_live(&runtime.socket_path).await {
        bail!(
            "pm is already running for {}",
            runtime.root_dir.to_string_lossy()
        );
    }

    if tokio::fs::try_exists(&runtime.socket_path).await? {
        tokio::fs::remove_file(&runtime.socket_path)
            .await
            .with_context(|| format!("failed to remove stale {}", runtime.socket_path.display()))?;
    }

    let exe = env::current_exe().context("failed to resolve current executable")?;
    let mut manager = std::process::Command::new(exe)
        .arg("__manager")
        .arg("--config")
        .arg(&runtime.config_path)
        .arg("--socket")
        .arg(&runtime.socket_path)
        .arg("--state-dir")
        .arg(&runtime.state_dir)
        .current_dir(&runtime.root_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start pm manager")?;

    wait_for_manager_startup(&runtime.socket_path, &mut manager, Duration::from_secs(5)).await?;

    if attach {
        attach::run(&runtime.socket_path).await?;
    } else {
        println!("pm started");
    }

    Ok(())
}

async fn command_down(runtime: &RuntimePaths) -> Result<()> {
    let response = request_response(&runtime.socket_path, &ClientMessage::Shutdown).await?;
    match response {
        ServerMessage::Ack { message } => {
            wait_for_socket_removal(&runtime.socket_path, Duration::from_secs(5)).await?;
            println!("{message}");
            Ok(())
        }
        ServerMessage::Error { message } => bail!(message),
        other => bail!("unexpected response from manager: {other:?}"),
    }
}

async fn command_process(
    runtime: &RuntimePaths,
    service: &str,
    action: ProcessAction,
) -> Result<()> {
    let response = request_response(
        &runtime.socket_path,
        &ClientMessage::ProcessAction {
            service: service.to_owned(),
            action,
        },
    )
    .await?;

    match response {
        ServerMessage::Ack { message } => {
            println!("{message}");
            Ok(())
        }
        ServerMessage::Error { message } => bail!(message),
        other => bail!("unexpected response from manager: {other:?}"),
    }
}

async fn request_response(socket_path: &Path, message: &ClientMessage) -> Result<ServerMessage> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", socket_path.display()))?;
    let (read_half, mut write_half) = stream.into_split();
    write_message(&mut write_half, message).await?;

    let mut reader = BufReader::new(read_half);
    read_message::<_, ServerMessage>(&mut reader)
        .await?
        .context("manager closed the connection before replying")
}

async fn socket_is_live(socket_path: &Path) -> bool {
    UnixStream::connect(socket_path).await.is_ok()
}

async fn wait_for_manager_startup(
    socket_path: &Path,
    manager: &mut Child,
    startup_timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + startup_timeout;
    loop {
        if let Some(status) = manager
            .try_wait()
            .context("failed to poll pm manager during startup")?
        {
            bail!(manager_exit_message(status));
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("timed out waiting for pm manager to finish initialization");
        }

        match timeout(remaining, wait_for_manager_ready(socket_path, remaining)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => {
                if let Some(status) = manager
                    .try_wait()
                    .context("failed to poll pm manager during startup")?
                {
                    return Err(error.context(manager_exit_message(status)));
                }
                if Instant::now() >= deadline {
                    return Err(error)
                        .context("timed out waiting for pm manager to finish initialization");
                }
                sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {
                if let Some(status) = manager
                    .try_wait()
                    .context("failed to poll pm manager during startup")?
                {
                    bail!(manager_exit_message(status));
                }
                bail!("timed out waiting for pm manager to finish initialization");
            }
        }
    }
}

async fn wait_for_manager_ready(socket_path: &Path, timeout_duration: Duration) -> Result<()> {
    let response = timeout(
        timeout_duration,
        request_response(socket_path, &ClientMessage::Ping),
    )
    .await
    .context("timed out waiting for pm manager to finish initialization")??;

    match response {
        ServerMessage::Ready => Ok(()),
        ServerMessage::Error { message } => bail!(message),
        other => bail!("unexpected response from manager: {other:?}"),
    }
}

fn manager_exit_message(status: ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("pm manager exited during startup with status code {code}"),
        None => "pm manager exited during startup after receiving a signal".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn waits_for_ready_handshake_after_socket_bind() {
        let state_dir = tempfile::tempdir().unwrap();
        let socket_path = state_dir.path().join("manager.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();

        let server = tokio::spawn(async move {
            sleep(Duration::from_millis(150)).await;
            let (stream, _) = listener.accept().await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);
            let message = read_message::<_, ClientMessage>(&mut reader)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(message, ClientMessage::Ping));
            write_message(&mut write_half, &ServerMessage::Ready)
                .await
                .unwrap();
        });

        let started = Instant::now();
        wait_for_manager_ready(&socket_path, Duration::from_secs(1))
            .await
            .unwrap();

        assert!(started.elapsed() >= Duration::from_millis(150));
        server.await.unwrap();
    }
}

async fn wait_for_socket_removal(socket_path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if !tokio::fs::try_exists(socket_path).await? || !socket_is_live(socket_path).await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for pm to stop");
        }
        sleep(Duration::from_millis(100)).await;
    }
}
