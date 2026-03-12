use anyhow::{Context, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Attach,
    Ping,
    Shutdown,
    AllAction {
        action: ProcessAction,
    },
    ProcessAction {
        service: String,
        action: ProcessAction,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessAction {
    Restart,
    Stop,
    Start,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceState {
    Starting,
    Running,
    Stopped,
    Exited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSnapshot {
    pub name: String,
    pub pid: Option<u32>,
    pub state: ServiceState,
    pub ready: bool,
    pub cpu_percent: f32,
    pub memory_bytes: u64,
    pub last_exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub service: String,
    pub line: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Ready,
    Snapshot {
        services: Vec<ServiceSnapshot>,
        recent_logs: Vec<LogEntry>,
        max_name_width: usize,
    },
    Log {
        entry: LogEntry,
    },
    ServiceUpdate {
        service: ServiceSnapshot,
    },
    Ack {
        message: String,
    },
    Error {
        message: String,
    },
    ManagerStopping,
}

pub async fn read_message<R, T>(reader: &mut BufReader<R>) -> Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut line = String::new();
    let bytes_read = reader
        .read_line(&mut line)
        .await
        .context("failed to read from socket")?;
    if bytes_read == 0 {
        return Ok(None);
    }
    let message =
        serde_json::from_str::<T>(line.trim_end()).context("failed to decode socket message")?;
    Ok(Some(message))
}

pub async fn write_message<W, T>(writer: &mut W, message: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let mut payload = serde_json::to_vec(message).context("failed to encode socket message")?;
    payload.push(b'\n');
    writer
        .write_all(&payload)
        .await
        .context("failed to write to socket")?;
    writer.flush().await.context("failed to flush socket")?;
    Ok(())
}
