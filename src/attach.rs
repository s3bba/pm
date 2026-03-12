use std::{
    collections::BTreeMap,
    io::{Stdout, Write, stdout},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use crossterm::{
    cursor::{Hide, MoveTo, RestorePosition, SavePosition, Show},
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute, queue,
    style::{Color, Print, ResetColor, SetForegroundColor},
    terminal::{self, Clear, ClearType},
};
use tokio::{
    io::BufReader,
    net::UnixStream,
    sync::mpsc::{self, UnboundedReceiver},
};

use crate::ipc::{
    ClientMessage, LogEntry, ProcessAction, ServerMessage, ServiceSnapshot, ServiceState,
    read_message, write_message,
};

const COMMAND_ROW_HEIGHT: u16 = 1;
const MIN_ATTACH_ROWS: u16 = 3;

pub async fn run(socket_path: &Path) -> Result<()> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", socket_path.display()))?;
    let (read_half, mut write_half) = stream.into_split();
    write_message(&mut write_half, &ClientMessage::Attach).await?;

    let mut reader = BufReader::new(read_half);
    let Some(first_message) = read_message::<_, ServerMessage>(&mut reader).await? else {
        bail!("manager closed the connection before sending a snapshot");
    };

    let (services, recent_logs, max_name_width) = match first_message {
        ServerMessage::Snapshot {
            services,
            recent_logs,
            max_name_width,
        } => (services, recent_logs, max_name_width),
        ServerMessage::Error { message } => bail!(message),
        other => bail!("unexpected response from manager: {other:?}"),
    };

    let mut terminal = AttachTerminal::new(max_name_width, services.len())?;
    let mut service_map = services
        .into_iter()
        .map(|service| (service.name.clone(), service))
        .collect::<BTreeMap<_, _>>();

    for entry in recent_logs {
        terminal.print_log(&entry)?;
    }
    terminal.draw_status(&service_map)?;

    let mut input_events = spawn_input_reader();
    let mut input_mode = InputMode::Status;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c => break,
            maybe_event = input_events.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                match handle_input_event(
                    event,
                    &mut input_mode,
                    &mut terminal,
                    &service_map,
                    socket_path,
                ).await? {
                    AttachAction::Continue => {}
                    AttachAction::Exit => break,
                }
            }
            message = read_message::<_, ServerMessage>(&mut reader) => {
                let Some(message) = message? else {
                    break;
                };
                match message {
                    ServerMessage::Log { entry } => {
                        terminal.print_log(&entry)?;
                        if let InputMode::Command(buffer) = &input_mode {
                            terminal.draw_command_prompt(&service_map, buffer)?;
                        }
                    }
                    ServerMessage::ServiceUpdate { service } => {
                        service_map.insert(service.name.clone(), service);
                        match &input_mode {
                            InputMode::Status => terminal.draw_status(&service_map)?,
                            InputMode::Command(buffer) => {
                                terminal.draw_command_prompt(&service_map, buffer)?
                            }
                        }
                    }
                    ServerMessage::ManagerStopping => break,
                    ServerMessage::Error { message } => bail!(message),
                    ServerMessage::Ready
                    | ServerMessage::Snapshot { .. }
                    | ServerMessage::Ack { .. } => {}
                }
            }
        }
    }

    terminal.restore()?;
    Ok(())
}

struct AttachTerminal {
    stdout: Stdout,
    columns: u16,
    rows: u16,
    max_name_width: usize,
    service_count: usize,
    service_rows: u16,
    restored: bool,
}

impl AttachTerminal {
    fn new(max_name_width: usize, service_count: usize) -> Result<Self> {
        let (columns, rows) = terminal::size().context("failed to read terminal size")?;
        if rows < MIN_ATTACH_ROWS {
            bail!("attach requires a terminal with at least three rows");
        }

        terminal::enable_raw_mode().context("failed to enable raw mode")?;
        let mut terminal = Self {
            stdout: stdout(),
            columns,
            rows,
            max_name_width,
            service_count,
            service_rows: service_rows_for(rows, service_count),
            restored: false,
        };
        if let Err(error) = (|| -> Result<()> {
            execute!(terminal.stdout, Hide)?;
            terminal.configure_scroll_region()?;
            Ok(())
        })() {
            let _ = terminal::disable_raw_mode();
            return Err(error);
        }
        Ok(terminal)
    }

    fn print_log(&mut self, entry: &LogEntry) -> Result<()> {
        self.refresh_size()?;
        let log_bottom = self.log_bottom_row();
        let prefix = format_log_prefix(&entry.service, self.max_name_width);
        queue!(
            self.stdout,
            MoveTo(0, log_bottom),
            Clear(ClearType::CurrentLine),
            SetForegroundColor(Color::DarkGrey),
            Print(prefix),
            ResetColor,
            Print(&entry.line),
            Print("\n")
        )?;
        self.stdout.flush()?;
        Ok(())
    }

    fn draw_status(&mut self, services: &BTreeMap<String, ServiceSnapshot>) -> Result<()> {
        self.draw_footer(services, None)
    }

    fn draw_command_prompt(
        &mut self,
        services: &BTreeMap<String, ServiceSnapshot>,
        buffer: &str,
    ) -> Result<()> {
        self.draw_footer(services, Some(buffer))
    }

    fn restore(&mut self) -> Result<()> {
        if self.restored {
            return Ok(());
        }
        let status_row = self.status_start_row();

        terminal::disable_raw_mode().context("failed to disable raw mode")?;
        write!(self.stdout, "\x1b[r")?;
        execute!(
            self.stdout,
            MoveTo(0, status_row),
            Clear(ClearType::FromCursorDown),
            Show
        )?;
        writeln!(self.stdout)?;
        self.stdout.flush()?;
        self.restored = true;
        Ok(())
    }

    fn refresh_size(&mut self) -> Result<()> {
        let (columns, rows) = terminal::size().context("failed to read terminal size")?;
        if rows < MIN_ATTACH_ROWS {
            bail!("attach requires a terminal with at least three rows");
        }
        if columns != self.columns || rows != self.rows {
            self.columns = columns;
            self.rows = rows;
            self.service_rows = service_rows_for(rows, self.service_count);
            self.configure_scroll_region()?;
        }
        Ok(())
    }

    fn configure_scroll_region(&mut self) -> Result<()> {
        let top = 1u16;
        let bottom = self.status_start_row();
        let status_row = self.status_start_row();
        write!(self.stdout, "\x1b[{top};{bottom}r")?;
        queue!(
            self.stdout,
            MoveTo(0, status_row),
            Clear(ClearType::FromCursorDown)
        )?;
        self.stdout.flush()?;
        Ok(())
    }

    fn log_bottom_row(&self) -> u16 {
        self.status_start_row().saturating_sub(1)
    }

    fn status_start_row(&self) -> u16 {
        self.rows.saturating_sub(self.footer_height())
    }

    fn command_row(&self) -> u16 {
        self.rows.saturating_sub(COMMAND_ROW_HEIGHT)
    }

    fn footer_height(&self) -> u16 {
        self.service_rows + COMMAND_ROW_HEIGHT
    }

    fn clear_footer(&mut self) -> Result<()> {
        for row in self.status_start_row()..self.rows {
            queue!(self.stdout, MoveTo(0, row), Clear(ClearType::CurrentLine))?;
        }
        Ok(())
    }

    fn update_service_count(&mut self, service_count: usize) -> Result<()> {
        if self.service_count == service_count {
            return Ok(());
        }

        self.service_count = service_count;
        self.service_rows = service_rows_for(self.rows, self.service_count);
        self.configure_scroll_region()?;
        Ok(())
    }

    fn draw_footer(
        &mut self,
        services: &BTreeMap<String, ServiceSnapshot>,
        prompt: Option<&str>,
    ) -> Result<()> {
        self.update_service_count(services.len())?;
        self.refresh_size()?;
        let status_row = self.status_start_row();
        let status_lines = status_lines(services, self.service_rows as usize, self.max_name_width);

        queue!(self.stdout, SavePosition)?;
        self.clear_footer()?;

        for (index, line) in status_lines.iter().enumerate() {
            queue!(self.stdout, MoveTo(0, status_row + index as u16))?;

            let mut remaining = self.columns as usize;
            for segment in &line.segments {
                if remaining == 0 {
                    break;
                }
                let text = truncate_ascii(&segment.text, remaining);
                if text.is_empty() {
                    continue;
                }
                if let Some(color) = segment.color {
                    queue!(self.stdout, SetForegroundColor(color))?;
                }
                queue!(self.stdout, Print(text.clone()))?;
                if segment.color.is_some() {
                    queue!(self.stdout, ResetColor)?;
                }
                remaining = remaining.saturating_sub(text.len());
            }
        }

        if let Some(buffer) = prompt {
            let command_row = self.command_row();
            queue!(
                self.stdout,
                MoveTo(0, command_row),
                SetForegroundColor(Color::White),
                Print(truncate_ascii(buffer, self.columns as usize)),
                ResetColor
            )?;
        }

        queue!(self.stdout, RestorePosition)?;
        self.stdout.flush()?;
        Ok(())
    }
}

impl Drop for AttachTerminal {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

#[derive(Debug, Clone)]
struct StatusSegment {
    text: String,
    color: Option<Color>,
}

#[derive(Debug, Clone)]
struct StatusLine {
    segments: Vec<StatusSegment>,
}

#[derive(Debug)]
enum InputMode {
    Status,
    Command(String),
}

#[derive(Debug)]
enum InputEvent {
    Key(KeyEvent),
    Error(String),
}

#[derive(Debug)]
enum AttachAction {
    Continue,
    Exit,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum SlashCommand {
    RestartAll,
    RestartService(String),
}

fn status_lines(
    services: &BTreeMap<String, ServiceSnapshot>,
    max_lines: usize,
    name_width: usize,
) -> Vec<StatusLine> {
    if services.is_empty() {
        return vec![StatusLine {
            segments: vec![StatusSegment {
                text: "no services".to_owned(),
                color: Some(Color::DarkGrey),
            }],
        }];
    }

    let visible_services = if services.len() > max_lines && max_lines > 0 {
        max_lines.saturating_sub(1)
    } else {
        services.len()
    };

    let mut lines = services
        .values()
        .take(visible_services)
        .map(|service| StatusLine {
            segments: vec![
                StatusSegment {
                    text: format!(
                        "{:width$} {:name_width$} ",
                        status_label(service),
                        service.name,
                        width = status_width(),
                        name_width = name_width
                    ),
                    color: Some(state_color(service)),
                },
                StatusSegment {
                    text: format!(
                        "{:>5.1}% {}",
                        service.cpu_percent,
                        format_memory(service.memory_bytes)
                    ),
                    color: Some(Color::DarkGrey),
                },
            ],
        })
        .collect::<Vec<_>>();

    if services.len() > visible_services {
        let hidden_count = services.len() - visible_services;
        lines.push(StatusLine {
            segments: vec![StatusSegment {
                text: format!("... {hidden_count} more service(s)"),
                color: Some(Color::DarkGrey),
            }],
        });
    }

    lines
}

fn status_label(service: &ServiceSnapshot) -> &'static str {
    match service.state {
        ServiceState::Starting => "starting",
        ServiceState::Running => "running",
        ServiceState::Stopped => "stopped",
        ServiceState::Exited => "exited",
    }
}

fn status_width() -> usize {
    "starting".len()
}

fn state_color(service: &ServiceSnapshot) -> Color {
    match service.state {
        ServiceState::Running if service.ready => Color::Green,
        ServiceState::Running | ServiceState::Starting => Color::Yellow,
        ServiceState::Stopped => Color::DarkGrey,
        ServiceState::Exited => Color::Red,
    }
}

pub fn format_log_prefix(service_name: &str, max_name_width: usize) -> String {
    let padding = " ".repeat(max_name_width.saturating_sub(service_name.len()));
    format!("{padding}{service_name}>  ")
}

fn truncate_ascii(value: &str, max_len: usize) -> String {
    value.chars().take(max_len).collect()
}

fn service_rows_for(rows: u16, service_count: usize) -> u16 {
    let max_service_rows = rows.saturating_sub(COMMAND_ROW_HEIGHT + 1) as usize;
    service_count.clamp(1, max_service_rows.max(1)) as u16
}

fn spawn_input_reader() -> UnboundedReceiver<InputEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        while !tx.is_closed() {
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => match event::read() {
                    Ok(Event::Key(key_event)) if key_event.kind == KeyEventKind::Press => {
                        if tx.send(InputEvent::Key(key_event)).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let _ = tx.send(InputEvent::Error(error.to_string()));
                        break;
                    }
                },
                Ok(false) => {}
                Err(error) => {
                    let _ = tx.send(InputEvent::Error(error.to_string()));
                    break;
                }
            }
        }
    });
    rx
}

async fn handle_input_event(
    event: InputEvent,
    input_mode: &mut InputMode,
    terminal: &mut AttachTerminal,
    services: &BTreeMap<String, ServiceSnapshot>,
    socket_path: &Path,
) -> Result<AttachAction> {
    match event {
        InputEvent::Error(message) => bail!(message),
        InputEvent::Key(key_event) => {
            if key_event.modifiers.contains(KeyModifiers::CONTROL)
                && matches!(key_event.code, KeyCode::Char('c') | KeyCode::Char('C'))
            {
                return Ok(AttachAction::Exit);
            }

            match input_mode {
                InputMode::Status => handle_status_key(key_event, input_mode, terminal, services),
                InputMode::Command(buffer) => {
                    handle_command_key(key_event, buffer, terminal, services, socket_path).await?;
                    if buffer.is_empty() {
                        *input_mode = InputMode::Status;
                    }
                    Ok(AttachAction::Continue)
                }
            }
        }
    }
}

fn handle_status_key(
    key_event: KeyEvent,
    input_mode: &mut InputMode,
    terminal: &mut AttachTerminal,
    services: &BTreeMap<String, ServiceSnapshot>,
) -> Result<AttachAction> {
    if matches!(key_event.code, KeyCode::Char(':')) {
        *input_mode = InputMode::Command(":".to_owned());
        terminal.draw_command_prompt(services, ":")?;
    }
    Ok(AttachAction::Continue)
}

async fn handle_command_key(
    key_event: KeyEvent,
    buffer: &mut String,
    terminal: &mut AttachTerminal,
    services: &BTreeMap<String, ServiceSnapshot>,
    socket_path: &Path,
) -> Result<()> {
    match key_event.code {
        KeyCode::Esc => {
            buffer.clear();
            terminal.draw_status(services)?;
        }
        KeyCode::Backspace => {
            buffer.pop();
            if buffer.is_empty() {
                terminal.draw_status(services)?;
            } else {
                terminal.draw_command_prompt(services, buffer)?;
            }
        }
        KeyCode::Enter => {
            let command_input = buffer.trim().to_owned();
            buffer.clear();
            terminal.draw_status(services)?;

            if command_input == ":" || command_input.is_empty() {
                return Ok(());
            }

            let line = match parse_colon_command(&command_input) {
                Ok(command) => match execute_slash_command(socket_path, command).await {
                    Ok(message) => message,
                    Err(error) => format!("command failed: {error}"),
                },
                Err(error) => format!("command failed: {error}"),
            };
            terminal.print_log(&LogEntry {
                service: "pm".to_owned(),
                line,
            })?;
            terminal.draw_status(services)?;
        }
        KeyCode::Char(character) => {
            if !key_event.modifiers.contains(KeyModifiers::CONTROL) {
                buffer.push(character);
                terminal.draw_command_prompt(services, buffer)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_colon_command(input: &str) -> Result<SlashCommand> {
    let trimmed = input.trim();
    let Some(command) = trimmed.strip_prefix(':') else {
        bail!("commands must start with `:`");
    };
    let mut parts = command.split_whitespace();
    let Some(name) = parts.next() else {
        bail!("empty command");
    };

    match name {
        "ra" | "restart-all" => {
            if parts.next().is_some() {
                bail!("`:{name}` does not take arguments");
            }
            Ok(SlashCommand::RestartAll)
        }
        "r" | "restart" => match (parts.next(), parts.next()) {
            (Some(service), None) => Ok(SlashCommand::RestartService(service.to_owned())),
            (None, None) => bail!("`:{name}` requires a service name"),
            _ => bail!("`:{name}` accepts exactly one service name"),
        },
        _ => bail!("unknown command `:{name}`"),
    }
}

async fn execute_slash_command(socket_path: &Path, command: SlashCommand) -> Result<String> {
    let message = match command {
        SlashCommand::RestartAll => ClientMessage::AllAction {
            action: ProcessAction::Restart,
        },
        SlashCommand::RestartService(service) => ClientMessage::ProcessAction {
            service,
            action: ProcessAction::Restart,
        },
    };
    match request_response(socket_path, &message).await? {
        ServerMessage::Ack { message } => Ok(message),
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

fn format_memory(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        SlashCommand, format_log_prefix, format_memory, parse_colon_command, service_rows_for,
        status_label, status_lines, status_width,
    };
    use crate::ipc::{ServiceSnapshot, ServiceState};

    #[test]
    fn aligns_log_prefixes() {
        assert_eq!(format_log_prefix("abc", 6), "   abc>  ");
        assert_eq!(format_log_prefix("abcdef", 6), "abcdef>  ");
    }

    #[test]
    fn formats_memory_units() {
        assert_eq!(format_memory(512), "512 B");
        assert_eq!(format_memory(1024 * 1024 * 32), "32.0 MiB");
    }

    #[test]
    fn parses_restart_commands() {
        assert_eq!(
            parse_colon_command(":ra").unwrap(),
            SlashCommand::RestartAll
        );
        assert_eq!(
            parse_colon_command(":restart-all").unwrap(),
            SlashCommand::RestartAll
        );
        assert_eq!(
            parse_colon_command(":r forge_web").unwrap(),
            SlashCommand::RestartService("forge_web".to_owned())
        );
        assert_eq!(
            parse_colon_command(":restart forge_web").unwrap(),
            SlashCommand::RestartService("forge_web".to_owned())
        );
    }

    #[test]
    fn builds_one_status_line_per_service() {
        let services = BTreeMap::from([
            (
                "api".to_owned(),
                sample_service("api", ServiceState::Running),
            ),
            (
                "worker".to_owned(),
                sample_service("worker", ServiceState::Starting),
            ),
        ]);

        let lines = status_lines(&services, 10, 6);

        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0].segments[0].text,
            format!(
                "{:width$} {:name_width$} ",
                "running",
                "api",
                width = status_width(),
                name_width = 6
            )
        );
        assert_eq!(lines[0].segments[1].text, " 12.5% 32.0 MiB");
        assert_eq!(
            lines[1].segments[0].text,
            format!(
                "{:width$} {:name_width$} ",
                "starting",
                "worker",
                width = status_width(),
                name_width = 6
            )
        );
    }

    #[test]
    fn limits_service_rows_to_leave_room_for_logs_and_command_prompt() {
        assert_eq!(service_rows_for(6, 2), 2);
        assert_eq!(service_rows_for(6, 8), 4);
    }

    #[test]
    fn shows_overflow_line_when_footer_cannot_fit_all_services() {
        let services = BTreeMap::from([
            (
                "api".to_owned(),
                sample_service("api", ServiceState::Running),
            ),
            ("db".to_owned(), sample_service("db", ServiceState::Running)),
            (
                "web".to_owned(),
                sample_service("web", ServiceState::Running),
            ),
        ]);

        let lines = status_lines(&services, 2, 3);

        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0].segments[0].text,
            format!(
                "{:width$} {:name_width$} ",
                "running",
                "api",
                width = status_width(),
                name_width = 3
            )
        );
        assert_eq!(lines[1].segments[0].text, "... 2 more service(s)");
    }

    #[test]
    fn formats_status_labels_from_service_state() {
        assert_eq!(
            status_label(&sample_service("api", ServiceState::Starting)),
            "starting"
        );
        assert_eq!(
            status_label(&sample_service("api", ServiceState::Running)),
            "running"
        );
        assert_eq!(
            status_label(&sample_service("api", ServiceState::Stopped)),
            "stopped"
        );
        assert_eq!(
            status_label(&sample_service("api", ServiceState::Exited)),
            "exited"
        );
    }

    fn sample_service(name: &str, state: ServiceState) -> ServiceSnapshot {
        ServiceSnapshot {
            name: name.to_owned(),
            pid: Some(42),
            state,
            ready: true,
            cpu_percent: 12.5,
            memory_bytes: 32 * 1024 * 1024,
            last_exit_code: None,
        }
    }
}
