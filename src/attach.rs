use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Stdout, Write, stdout},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
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
const SERVICE_NAME_COLOR: Color = Color::DarkGrey;
const SERVICE_SHORT_NAME_COLOR: Color = Color::Magenta;

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

    let mut service_map = services
        .into_iter()
        .map(|service| (service.name.clone(), service))
        .collect::<BTreeMap<_, _>>();
    let service_count = service_map.len();
    let service_names = ServiceNames::from_services(&service_map);
    let mut terminal = AttachTerminal::new(max_name_width, service_count)?;

    for entry in recent_logs {
        terminal.print_log(&entry, &service_names)?;
    }
    terminal.draw_status(&service_map, &service_names)?;

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
                    &service_names,
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
                        terminal.print_log(&entry, &service_names)?;
                        if let InputMode::Command(buffer) = &input_mode {
                            terminal.draw_command_prompt(&service_map, &service_names, buffer)?;
                        }
                    }
                    ServerMessage::ServiceUpdate { service } => {
                        service_map.insert(service.name.clone(), service);
                        match &input_mode {
                            InputMode::Status => terminal.draw_status(&service_map, &service_names)?,
                            InputMode::Command(buffer) => {
                                terminal.draw_command_prompt(&service_map, &service_names, buffer)?
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

    fn print_log(&mut self, entry: &LogEntry, service_names: &ServiceNames) -> Result<()> {
        self.refresh_size()?;
        let log_bottom = self.log_bottom_row();
        queue!(
            self.stdout,
            MoveTo(0, log_bottom),
            Clear(ClearType::CurrentLine)
        )?;
        for segment in format_log_prefix_segments(
            &entry.service,
            service_names.short_name(&entry.service),
            self.max_name_width,
        ) {
            if let Some(color) = segment.color {
                queue!(self.stdout, SetForegroundColor(color))?;
            }
            queue!(self.stdout, Print(segment.text))?;
            if segment.color.is_some() {
                queue!(self.stdout, ResetColor)?;
            }
        }
        queue!(self.stdout, Print(&entry.line), Print("\n"))?;
        self.stdout.flush()?;
        Ok(())
    }

    fn draw_status(
        &mut self,
        services: &BTreeMap<String, ServiceSnapshot>,
        service_names: &ServiceNames,
    ) -> Result<()> {
        self.draw_footer(services, service_names, None)
    }

    fn draw_command_prompt(
        &mut self,
        services: &BTreeMap<String, ServiceSnapshot>,
        service_names: &ServiceNames,
        buffer: &str,
    ) -> Result<()> {
        self.draw_footer(services, service_names, Some(buffer))
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
        service_names: &ServiceNames,
        prompt: Option<&str>,
    ) -> Result<()> {
        self.update_service_count(services.len())?;
        self.refresh_size()?;
        let status_row = self.status_start_row();
        let status_lines = status_lines(
            services,
            service_names,
            self.service_rows as usize,
            self.max_name_width,
        );

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
            let (prompt_text, cursor_column) = render_command_prompt(buffer, self.columns as usize);
            queue!(
                self.stdout,
                MoveTo(0, command_row),
                SetForegroundColor(Color::White),
                Print(prompt_text),
                ResetColor,
                MoveTo(cursor_column as u16, command_row),
                Show
            )?;
        } else {
            queue!(self.stdout, Hide)?;
        }

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
    StopAll,
    StopService(String),
    StartAll,
    StartService(String),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SlashArgKind {
    None,
    Service,
}

const COMMAND_ALIASES: &[(&str, SlashArgKind)] = &[
    ("ra", SlashArgKind::None),
    ("restart-all", SlashArgKind::None),
    ("r", SlashArgKind::Service),
    ("restart", SlashArgKind::Service),
    ("stop-all", SlashArgKind::None),
    ("stop", SlashArgKind::Service),
    ("start-all", SlashArgKind::None),
    ("start", SlashArgKind::Service),
];

#[derive(Debug, Clone)]
struct ServiceNames {
    short_by_full: BTreeMap<String, String>,
    full_by_short: BTreeMap<String, String>,
}

impl ServiceNames {
    fn from_services(services: &BTreeMap<String, ServiceSnapshot>) -> Self {
        Self::from_names(services.keys().map(String::as_str))
    }

    fn from_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Self {
        let names = names.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let short_by_full = build_service_short_names(&names);
        let full_by_short = short_by_full
            .iter()
            .map(|(full_name, short_name)| (short_name.clone(), full_name.clone()))
            .collect();
        Self {
            short_by_full,
            full_by_short,
        }
    }

    fn short_name<'a>(&'a self, full_name: &str) -> &'a str {
        self.short_by_full
            .get(full_name)
            .map(String::as_str)
            .unwrap_or("")
    }

    fn resolve<'a>(&'a self, token: &str) -> Option<&'a str> {
        if let Some((full_name, _)) = self.short_by_full.get_key_value(token) {
            Some(full_name.as_str())
        } else {
            self.full_by_short.get(token).map(String::as_str)
        }
    }

    fn candidates<'a>(&'a self, prefix: &str) -> Vec<&'a str> {
        let short_matches = self
            .full_by_short
            .keys()
            .map(String::as_str)
            .filter(|candidate| candidate.starts_with(prefix))
            .collect::<Vec<_>>();
        if !short_matches.is_empty()
            && !prefix.chars().any(|character| !character.is_alphanumeric())
        {
            return short_matches;
        }

        let full_matches = self
            .short_by_full
            .keys()
            .map(String::as_str)
            .filter(|candidate| candidate.starts_with(prefix))
            .collect::<Vec<_>>();
        if full_matches.is_empty() {
            short_matches
        } else {
            full_matches
        }
    }
}

fn build_service_short_names(names: &[String]) -> BTreeMap<String, String> {
    let signatures = names
        .iter()
        .map(|name| (name.clone(), service_short_name_signature(name, names)))
        .collect::<BTreeMap<_, _>>();
    let full_names = names.iter().map(String::as_str).collect::<BTreeSet<_>>();

    names
        .iter()
        .map(|name| {
            let short_name =
                shortest_unique_short_name(name, &signatures[name], &signatures, &full_names);
            (name.clone(), short_name)
        })
        .collect()
}

fn service_short_name_signature(service_name: &str, all_names: &[String]) -> String {
    let shared_prefix_len = all_names
        .iter()
        .filter(|other_name| other_name.as_str() != service_name)
        .map(|other_name| shared_separator_prefix_len(service_name, other_name))
        .max()
        .unwrap_or(0);

    let signature_source = if shared_prefix_len > 0 {
        let distinctive_suffix = trim_leading_non_alphanumeric(&service_name[shared_prefix_len..]);
        if distinctive_suffix.is_empty() {
            service_name
        } else {
            distinctive_suffix
        }
    } else {
        service_name
    };

    sanitize_short_name_fragment(signature_source)
}

fn shortest_unique_short_name(
    service_name: &str,
    signature: &str,
    signatures: &BTreeMap<String, String>,
    full_names: &BTreeSet<&str>,
) -> String {
    let mut candidate = String::new();
    for character in signature.chars() {
        candidate.push(character);

        let conflicts_with_full_name =
            candidate != service_name && full_names.contains(candidate.as_str());
        let conflicts_with_other_signature = candidate != service_name
            && signatures.iter().any(|(other_name, other_signature)| {
                other_name != service_name && other_signature.starts_with(&candidate)
            });
        if !conflicts_with_full_name && !conflicts_with_other_signature {
            return candidate;
        }
    }

    service_name.to_owned()
}

fn shared_separator_prefix_len(left: &str, right: &str) -> usize {
    let shared_prefix_len = longest_common_prefix_len(left, right);
    left[..shared_prefix_len]
        .char_indices()
        .filter_map(|(index, character)| {
            (!character.is_alphanumeric()).then_some(index + character.len_utf8())
        })
        .last()
        .unwrap_or(0)
}

fn longest_common_prefix_len(left: &str, right: &str) -> usize {
    let mut matched_bytes = 0;
    for ((index, left_character), right_character) in left.char_indices().zip(right.chars()) {
        if left_character != right_character {
            break;
        }
        matched_bytes = index + left_character.len_utf8();
    }
    matched_bytes
}

fn trim_leading_non_alphanumeric(value: &str) -> &str {
    let Some((index, _)) = value
        .char_indices()
        .find(|(_, character)| character.is_alphanumeric())
    else {
        return "";
    };
    &value[index..]
}

fn sanitize_short_name_fragment(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn autocomplete_colon_input(buffer: &str, service_names: &ServiceNames) -> Option<String> {
    let tail = buffer.strip_prefix(':')?;
    let command_end = tail
        .find(|character: char| character.is_whitespace())
        .unwrap_or(tail.len());
    let command = &tail[..command_end];
    let rest = &tail[command_end..];

    if rest.is_empty() {
        return autocomplete_command_only(command);
    }

    let mut completed_command = command.to_owned();
    let arg_kind = match command_arg_kind(command) {
        Some(arg_kind) => arg_kind,
        None => {
            let completion = complete_token(command, command_candidates(command))?;
            if completion == command {
                return None;
            }
            completed_command = completion;
            command_arg_kind(&completed_command)?
        }
    };

    if completed_command != command {
        return Some(format!(":{completed_command}{rest}"));
    }

    match arg_kind {
        SlashArgKind::None => None,
        SlashArgKind::Service => autocomplete_service_argument(command, rest, service_names),
    }
}

fn autocomplete_command_only(command: &str) -> Option<String> {
    if matches!(command_arg_kind(command), Some(SlashArgKind::Service)) {
        return Some(format!(":{command} "));
    }
    if command_arg_kind(command).is_some() {
        return None;
    }

    let completion = complete_token(command, command_candidates(command))?;
    if completion == command {
        None
    } else if matches!(command_arg_kind(&completion), Some(SlashArgKind::Service)) {
        Some(format!(":{completion} "))
    } else {
        Some(format!(":{completion}"))
    }
}

fn autocomplete_service_argument(
    command: &str,
    rest: &str,
    service_names: &ServiceNames,
) -> Option<String> {
    let argument_start = rest
        .find(|character: char| !character.is_whitespace())
        .unwrap_or(rest.len());
    let spacing = &rest[..argument_start];
    let arguments = &rest[argument_start..];

    if arguments.is_empty() {
        let completion = complete_token("", service_names.candidates(""))?;
        if completion.is_empty() {
            return None;
        }
        let unique = service_names
            .candidates(&completion)
            .into_iter()
            .all(|name| name == completion);
        let suffix = if unique { " " } else { "" };
        return Some(format!(":{command}{spacing}{completion}{suffix}"));
    }

    if arguments.chars().last().is_some_and(char::is_whitespace)
        || arguments.contains(char::is_whitespace)
    {
        return None;
    }

    let completion = complete_token(arguments, service_names.candidates(arguments))?;
    if completion == arguments {
        let unique = service_names
            .candidates(arguments)
            .into_iter()
            .all(|name| name == arguments);
        if unique {
            Some(format!(":{command}{spacing}{arguments} "))
        } else {
            None
        }
    } else {
        let unique = service_names
            .candidates(&completion)
            .into_iter()
            .all(|name| name == completion);
        let suffix = if unique { " " } else { "" };
        Some(format!(":{command}{spacing}{completion}{suffix}"))
    }
}

fn command_arg_kind(command: &str) -> Option<SlashArgKind> {
    COMMAND_ALIASES
        .iter()
        .find_map(|(alias, arg_kind)| (*alias == command).then_some(*arg_kind))
}

fn command_candidates(prefix: &str) -> Vec<&'static str> {
    COMMAND_ALIASES
        .iter()
        .map(|(alias, _)| *alias)
        .filter(|alias| alias.starts_with(prefix))
        .collect()
}

fn complete_token<'a>(token: &str, candidates: Vec<&'a str>) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }
    if candidates.len() == 1 {
        return Some(candidates[0].to_owned());
    }

    let prefix = longest_common_prefix(&candidates);
    (prefix.len() > token.len()).then_some(prefix)
}

fn longest_common_prefix(candidates: &[&str]) -> String {
    let Some(first) = candidates.first() else {
        return String::new();
    };
    let mut prefix = (*first).to_owned();
    for candidate in &candidates[1..] {
        let shared_len = prefix
            .chars()
            .zip(candidate.chars())
            .take_while(|(left, right)| left == right)
            .map(|(character, _)| character.len_utf8())
            .sum();
        prefix.truncate(shared_len);
        if prefix.is_empty() {
            break;
        }
    }
    prefix
}

fn status_lines(
    services: &BTreeMap<String, ServiceSnapshot>,
    service_names: &ServiceNames,
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
        .map(|service| {
            let mut segments = vec![StatusSegment {
                text: format!("{:width$} ", status_label(service), width = status_width()),
                color: Some(state_color(service)),
            }];
            segments.extend(service_name_segments(
                &service.name,
                service_names.short_name(&service.name),
                0,
                name_width.saturating_sub(service.name.len()),
                " ",
            ));
            segments.push(StatusSegment {
                text: format!(
                    "{:>5.1}% {}",
                    service.cpu_percent,
                    format_memory(service.memory_bytes)
                ),
                color: Some(Color::DarkGrey),
            });
            StatusLine { segments }
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

fn format_log_prefix_segments(
    service_name: &str,
    short_name: &str,
    max_name_width: usize,
) -> Vec<StatusSegment> {
    service_name_segments(
        service_name,
        short_name,
        max_name_width.saturating_sub(service_name.len()),
        0,
        ">  ",
    )
}

fn service_name_segments(
    service_name: &str,
    short_name: &str,
    left_padding: usize,
    right_padding: usize,
    suffix: &str,
) -> Vec<StatusSegment> {
    let mut segments = Vec::new();
    if left_padding > 0 {
        segments.push(StatusSegment {
            text: " ".repeat(left_padding),
            color: Some(SERVICE_NAME_COLOR),
        });
    }

    let highlighted_characters = highlighted_service_name_characters(service_name, short_name);
    let mut current_color = Some(SERVICE_NAME_COLOR);
    let mut current_text = String::new();

    for (index, character) in service_name.chars().enumerate() {
        let color = Some(if highlighted_characters[index] {
            SERVICE_SHORT_NAME_COLOR
        } else {
            SERVICE_NAME_COLOR
        });
        if !current_text.is_empty() && color != current_color {
            segments.push(StatusSegment {
                text: std::mem::take(&mut current_text),
                color: current_color,
            });
        }
        current_color = color;
        current_text.push(character);
    }
    if !current_text.is_empty() {
        segments.push(StatusSegment {
            text: current_text,
            color: current_color,
        });
    }

    if right_padding > 0 {
        segments.push(StatusSegment {
            text: " ".repeat(right_padding),
            color: Some(SERVICE_NAME_COLOR),
        });
    }
    if !suffix.is_empty() {
        segments.push(StatusSegment {
            text: suffix.to_owned(),
            color: Some(SERVICE_NAME_COLOR),
        });
    }

    segments
}

fn highlighted_service_name_characters(service_name: &str, short_name: &str) -> Vec<bool> {
    let service_characters = service_name.chars().collect::<Vec<_>>();
    let mut highlighted = vec![false; service_characters.len()];
    let mut search_end = service_characters.len();

    for short_character in short_name.chars().rev() {
        let Some(index) = service_characters[..search_end]
            .iter()
            .rposition(|character| *character == short_character)
        else {
            return vec![false; service_characters.len()];
        };
        highlighted[index] = true;
        search_end = index;
    }

    highlighted
}

fn truncate_ascii(value: &str, max_len: usize) -> String {
    value.chars().take(max_len).collect()
}

fn render_command_prompt(buffer: &str, max_len: usize) -> (String, usize) {
    if max_len == 0 {
        return (String::new(), 0);
    }

    let characters = buffer.chars().collect::<Vec<_>>();
    let visible_len = max_len.saturating_sub(1);
    if characters.len() <= visible_len {
        return (buffer.to_owned(), characters.len());
    }

    let start = characters.len().saturating_sub(visible_len);
    (characters[start..].iter().collect(), visible_len)
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
    service_names: &ServiceNames,
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
                InputMode::Status => {
                    handle_status_key(key_event, input_mode, terminal, services, service_names)
                }
                InputMode::Command(buffer) => {
                    handle_command_key(
                        key_event,
                        buffer,
                        terminal,
                        services,
                        service_names,
                        socket_path,
                    )
                    .await?;
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
    service_names: &ServiceNames,
) -> Result<AttachAction> {
    if matches!(key_event.code, KeyCode::Char(':')) {
        *input_mode = InputMode::Command(":".to_owned());
        terminal.draw_command_prompt(services, service_names, ":")?;
    }
    Ok(AttachAction::Continue)
}

async fn handle_command_key(
    key_event: KeyEvent,
    buffer: &mut String,
    terminal: &mut AttachTerminal,
    services: &BTreeMap<String, ServiceSnapshot>,
    service_names: &ServiceNames,
    socket_path: &Path,
) -> Result<()> {
    match key_event.code {
        KeyCode::Esc => {
            buffer.clear();
            terminal.draw_status(services, service_names)?;
        }
        KeyCode::Backspace => {
            buffer.pop();
            if buffer.is_empty() {
                terminal.draw_status(services, service_names)?;
            } else {
                terminal.draw_command_prompt(services, service_names, buffer)?;
            }
        }
        KeyCode::Tab => {
            if let Some(completed) = autocomplete_colon_input(buffer, service_names) {
                *buffer = completed;
                terminal.draw_command_prompt(services, service_names, buffer)?;
            }
        }
        KeyCode::Enter => {
            let command_input = buffer.trim().to_owned();
            buffer.clear();
            terminal.draw_status(services, service_names)?;

            if command_input == ":" || command_input.is_empty() {
                return Ok(());
            }

            let line = match parse_colon_command(&command_input, service_names) {
                Ok(command) => match execute_slash_command(socket_path, command).await {
                    Ok(message) => message,
                    Err(error) => format!("command failed: {error}"),
                },
                Err(error) => format!("command failed: {error}"),
            };
            terminal.print_log(
                &LogEntry {
                    service: "pm".to_owned(),
                    line,
                },
                service_names,
            )?;
            terminal.draw_status(services, service_names)?;
        }
        KeyCode::Char(character) => {
            if !key_event.modifiers.contains(KeyModifiers::CONTROL) {
                buffer.push(character);
                terminal.draw_command_prompt(services, service_names, buffer)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_colon_command(input: &str, service_names: &ServiceNames) -> Result<SlashCommand> {
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
            (Some(service), None) => Ok(SlashCommand::RestartService(
                resolve_service_token(service, service_names)?.to_owned(),
            )),
            (None, None) => bail!("`:{name}` requires a service name"),
            _ => bail!("`:{name}` accepts exactly one service name"),
        },
        "stop-all" => {
            if parts.next().is_some() {
                bail!("`:{name}` does not take arguments");
            }
            Ok(SlashCommand::StopAll)
        }
        "stop" => match (parts.next(), parts.next()) {
            (Some(service), None) => Ok(SlashCommand::StopService(
                resolve_service_token(service, service_names)?.to_owned(),
            )),
            (None, None) => bail!("`:{name}` requires a service name"),
            _ => bail!("`:{name}` accepts exactly one service name"),
        },
        "start-all" => {
            if parts.next().is_some() {
                bail!("`:{name}` does not take arguments");
            }
            Ok(SlashCommand::StartAll)
        }
        "start" => match (parts.next(), parts.next()) {
            (Some(service), None) => Ok(SlashCommand::StartService(
                resolve_service_token(service, service_names)?.to_owned(),
            )),
            (None, None) => bail!("`:{name}` requires a service name"),
            _ => bail!("`:{name}` accepts exactly one service name"),
        },
        _ => bail!("unknown command `:{name}`"),
    }
}

fn resolve_service_token<'a>(token: &str, service_names: &'a ServiceNames) -> Result<&'a str> {
    service_names
        .resolve(token)
        .with_context(|| format!("unknown service `{token}`"))
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
        SlashCommand::StopAll => ClientMessage::AllAction {
            action: ProcessAction::Stop,
        },
        SlashCommand::StopService(service) => ClientMessage::ProcessAction {
            service,
            action: ProcessAction::Stop,
        },
        SlashCommand::StartAll => ClientMessage::AllAction {
            action: ProcessAction::Start,
        },
        SlashCommand::StartService(service) => ClientMessage::ProcessAction {
            service,
            action: ProcessAction::Start,
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
        SERVICE_NAME_COLOR, SERVICE_SHORT_NAME_COLOR, ServiceNames, SlashCommand,
        autocomplete_colon_input, format_log_prefix, format_memory,
        highlighted_service_name_characters, parse_colon_command, render_command_prompt,
        service_rows_for, status_label, status_lines, status_width,
    };
    use crate::ipc::{ServiceSnapshot, ServiceState};
    use crossterm::style::Color;

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
    fn renders_command_prompt_cursor() {
        assert_eq!(render_command_prompt(":r fw", 32), (":r fw".to_owned(), 5));
        assert_eq!(
            render_command_prompt(":restart forge_web", 8),
            ("rge_web".to_owned(), 7)
        );
    }

    #[test]
    fn parses_restart_commands() {
        let service_names = ServiceNames::from_names(["forge_web"]);
        assert_eq!(
            parse_colon_command(":ra", &service_names).unwrap(),
            SlashCommand::RestartAll
        );
        assert_eq!(
            parse_colon_command(":restart-all", &service_names).unwrap(),
            SlashCommand::RestartAll
        );
        assert_eq!(
            parse_colon_command(":r forge_web", &service_names).unwrap(),
            SlashCommand::RestartService("forge_web".to_owned())
        );
        assert_eq!(
            parse_colon_command(":restart forge_web", &service_names).unwrap(),
            SlashCommand::RestartService("forge_web".to_owned())
        );
    }

    #[test]
    fn parses_start_and_stop_commands() {
        let service_names = ServiceNames::from_names(["forge_web"]);
        assert_eq!(
            parse_colon_command(":start-all", &service_names).unwrap(),
            SlashCommand::StartAll
        );
        assert_eq!(
            parse_colon_command(":start forge_web", &service_names).unwrap(),
            SlashCommand::StartService("forge_web".to_owned())
        );
        assert_eq!(
            parse_colon_command(":stop-all", &service_names).unwrap(),
            SlashCommand::StopAll
        );
        assert_eq!(
            parse_colon_command(":stop forge_web", &service_names).unwrap(),
            SlashCommand::StopService("forge_web".to_owned())
        );
    }

    #[test]
    fn autocompletes_command_names() {
        let service_names = ServiceNames::from_names(std::iter::empty::<&str>());
        assert_eq!(
            autocomplete_colon_input(":re", &service_names),
            Some(":restart ".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":restart-a", &service_names),
            Some(":restart-all".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":start-a", &service_names),
            Some(":start-all".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":stop-a", &service_names),
            Some(":stop-all".to_owned())
        );
    }

    #[test]
    fn adds_space_after_exact_restart_command() {
        let service_names = ServiceNames::from_names(std::iter::empty::<&str>());
        assert_eq!(
            autocomplete_colon_input(":r", &service_names),
            Some(":r ".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":start", &service_names),
            Some(":start ".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":stop", &service_names),
            Some(":stop ".to_owned())
        );
    }

    #[test]
    fn autocompletes_restart_service_argument() {
        let services = BTreeMap::from([
            (
                "api".to_owned(),
                sample_service("api", ServiceState::Running),
            ),
            (
                "worker".to_owned(),
                sample_service("worker", ServiceState::Running),
            ),
        ]);
        let service_names = ServiceNames::from_services(&services);

        assert_eq!(
            autocomplete_colon_input(":r w", &service_names),
            Some(":r w ".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":start wo", &service_names),
            Some(":start worker ".to_owned())
        );
        assert_eq!(
            autocomplete_colon_input(":stop wo", &service_names),
            Some(":stop worker ".to_owned())
        );
        assert_eq!(autocomplete_colon_input(":restart ", &service_names), None);

        let single_service = BTreeMap::from([(
            "api".to_owned(),
            sample_service("api", ServiceState::Running),
        )]);
        let single_service_names = ServiceNames::from_services(&single_service);
        assert_eq!(
            autocomplete_colon_input(":restart ", &single_service_names),
            Some(":restart a ".to_owned())
        );
    }

    #[test]
    fn does_not_complete_when_second_argument_is_present() {
        let services = BTreeMap::from([(
            "api".to_owned(),
            sample_service("api", ServiceState::Running),
        )]);
        let service_names = ServiceNames::from_services(&services);
        assert_eq!(
            autocomplete_colon_input(":r api extra", &service_names),
            None
        );
    }

    #[test]
    fn computes_unique_service_short_names() {
        let service_names = ServiceNames::from_names([
            "db",
            "forge_auth",
            "forge_git_worker",
            "forge_web",
            "forgecommander",
        ]);

        assert_eq!(service_names.short_name("db"), "d");
        assert_eq!(service_names.short_name("forge_auth"), "a");
        assert_eq!(service_names.short_name("forge_git_worker"), "g");
        assert_eq!(service_names.short_name("forge_web"), "w");
        assert_eq!(service_names.short_name("forgecommander"), "f");
    }

    #[test]
    fn parses_service_shorthands() {
        let service_names = ServiceNames::from_names([
            "db",
            "forge_auth",
            "forge_git_worker",
            "forge_web",
            "forgecommander",
        ]);

        assert_eq!(
            parse_colon_command(":r w", &service_names).unwrap(),
            SlashCommand::RestartService("forge_web".to_owned())
        );
        assert_eq!(
            parse_colon_command(":start a", &service_names).unwrap(),
            SlashCommand::StartService("forge_auth".to_owned())
        );
        assert_eq!(
            parse_colon_command(":stop f", &service_names).unwrap(),
            SlashCommand::StopService("forgecommander".to_owned())
        );
    }

    #[test]
    fn keeps_prefix_based_short_names_for_plain_shared_prefixes() {
        let service_names = ServiceNames::from_names(["api", "auth", "worker"]);

        assert_eq!(service_names.short_name("api"), "ap");
        assert_eq!(service_names.short_name("auth"), "au");
        assert_eq!(service_names.short_name("worker"), "w");
    }

    #[test]
    fn keeps_deterministic_prefixes_for_long_plain_shared_prefixes() {
        let service_names = ServiceNames::from_names(["forgecommander", "forgeconnect", "worker"]);

        assert_eq!(service_names.short_name("forgecommander"), "forgecom");
        assert_eq!(service_names.short_name("forgeconnect"), "forgecon");
        assert_eq!(service_names.short_name("worker"), "w");
    }

    #[test]
    fn highlights_trimmed_prefix_short_names_at_their_rightmost_match() {
        let highlighted = highlighted_service_name_characters("forge_git_worker", "g");
        let highlighted_positions = highlighted
            .into_iter()
            .enumerate()
            .filter_map(|(index, is_highlighted)| is_highlighted.then_some(index))
            .collect::<Vec<_>>();

        assert_eq!(highlighted_positions, vec![6]);
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
        let service_names = ServiceNames::from_services(&services);

        let lines = status_lines(&services, &service_names, 10, 6);

        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0].segments[0].text,
            format!("{:width$} ", "running", width = status_width())
        );
        assert_eq!(lines[0].segments[0].color, Some(Color::Green));
        assert!(
            lines[0]
                .segments
                .iter()
                .any(|segment| segment.text == "a"
                    && segment.color == Some(SERVICE_SHORT_NAME_COLOR))
        );
        assert!(
            lines[0]
                .segments
                .iter()
                .any(|segment| segment.color == Some(SERVICE_NAME_COLOR))
        );
        assert_eq!(
            lines[0]
                .segments
                .iter()
                .map(|segment| segment.text.as_str())
                .collect::<String>(),
            format!(
                "{:width$} {:name_width$} {:>5.1}% {}",
                "running",
                "api",
                12.5,
                "32.0 MiB",
                width = status_width(),
                name_width = 6
            )
        );
        assert_eq!(
            lines[1]
                .segments
                .iter()
                .map(|segment| segment.text.as_str())
                .collect::<String>(),
            format!(
                "{:width$} {:name_width$} {:>5.1}% {}",
                "starting",
                "worker",
                12.5,
                "32.0 MiB",
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
        let service_names = ServiceNames::from_services(&services);

        let lines = status_lines(&services, &service_names, 2, 3);

        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0]
                .segments
                .iter()
                .map(|segment| segment.text.as_str())
                .collect::<String>(),
            format!(
                "{:width$} {:name_width$} {:>5.1}% {}",
                "running",
                "api",
                12.5,
                "32.0 MiB",
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
