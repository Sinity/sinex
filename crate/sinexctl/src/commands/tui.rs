use base64::Engine;
use clap::{Args, ValueEnum};
use color_eyre::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    style::Print,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
};
use sinex_primitives::query::{
    EventQuery, EventQueryResult, QueryResultEvent, SortDirection, TimeRange,
};
use sinex_primitives::rpc::dlq::{DlqMessagePeek, DlqPeekResponse};
use sinex_primitives::rpc::lifecycle::LifecycleStatusResponse;
use sinex_primitives::rpc::ops::Operation as OpsOperation;
use sinex_primitives::rpc::privacy::PrivateModeStateResponse;
use sinex_primitives::rpc::replay::{ReplayOperation, ReplayState};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, ActionSideEffect, EventCardListView,
    EventCardView, OperationView, PrivacyStateKind, SinexObjectKind, SourceCoverageContinuity,
    SourceCoverageReadiness, SourceCoverageView,
};
use std::io;
use std::time::Instant;
use time::Duration;

use crate::client::GatewayClient;
use crate::fmt::{format_bytes, format_heartbeat_age};
use sinex_primitives::rpc::coordination::InstanceInfo;
use sinex_primitives::rpc::dlq::DlqListResponse;

/// Launch interactive TUI dashboard
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Launch dashboard
    sinexctl tui

    # Start on specific tab
    sinexctl tui --tab modules
    sinexctl tui --tab events

    # Custom refresh interval
    sinexctl tui --refresh 10

    # Disable auto-refresh
    sinexctl tui --refresh 0

KEYBOARD SHORTCUTS:
    Tab/←/→    Switch between tabs
    q/Esc      Quit
    r          Refresh data now
    j/↓        Next item
    k/↑        Previous item
")]
pub struct TuiCommand {
    /// Starting tab (dashboard, operations, modules, sources, events, dlq)
    #[arg(long, value_enum, default_value_t = Tab::Dashboard)]
    tab: Tab,

    /// Auto-refresh interval in seconds (0 to disable)
    #[arg(long, default_value = "5")]
    refresh: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Tab {
    Dashboard,
    #[value(alias = "ops")]
    Operations,
    #[value(alias = "module")]
    Modules,
    #[value(alias = "source")]
    Sources,
    #[value(alias = "event")]
    Events,
    Dlq,
}

struct App {
    current_tab: Tab,
    should_quit: bool,
    client: GatewayClient,
    refresh_interval: u64,

    // Live data
    modules: Vec<InstanceInfo>,
    dlq_stats: Option<DlqListResponse>,
    dlq_peek: Option<DlqPeekResponse>,
    ops_operations: Vec<OpsOperation>,
    replay_operations: Vec<ReplayOperation>,
    lifecycle_status: Option<LifecycleStatusResponse>,
    private_mode: Option<PrivateModeStateResponse>,
    source_coverage: Vec<SourceCoverageView>,
    recent_events: Vec<EventCardView>,
    recent_event_rows: Vec<QueryResultEvent>,
    gateway_version: String,

    // State
    loading: bool,
    last_refresh: Instant,
    error: Option<String>,
    selected_index: usize,
    show_help: bool,
    copy_menu_open: bool,
    copy_index: usize,
    payload_raw: bool,
    feedback: Option<String>,
}

impl App {
    fn new(client: GatewayClient, start_tab: Tab, refresh_interval: u64) -> Self {
        Self {
            current_tab: start_tab,
            should_quit: false,
            client,
            refresh_interval,
            modules: Vec::new(),
            dlq_stats: None,
            dlq_peek: None,
            ops_operations: Vec::new(),
            replay_operations: Vec::new(),
            lifecycle_status: None,
            private_mode: None,
            source_coverage: Vec::new(),
            recent_events: Vec::new(),
            recent_event_rows: Vec::new(),
            gateway_version: String::from("unknown"),
            loading: false,
            last_refresh: Instant::now()
                .checked_sub(std::time::Duration::from_secs(refresh_interval + 1))
                .unwrap_or(Instant::now()),
            error: None,
            selected_index: 0,
            show_help: false,
            copy_menu_open: false,
            copy_index: 0,
            payload_raw: false,
            feedback: None,
        }
    }

    fn next_tab(&mut self) {
        let next = match self.current_tab {
            Tab::Dashboard => Tab::Operations,
            Tab::Operations => Tab::Modules,
            Tab::Modules => Tab::Sources,
            Tab::Sources => Tab::Events,
            Tab::Events => Tab::Dlq,
            Tab::Dlq => Tab::Dashboard,
        };
        self.switch_tab(next);
    }

    fn previous_tab(&mut self) {
        let previous = match self.current_tab {
            Tab::Dashboard => Tab::Dlq,
            Tab::Operations => Tab::Dashboard,
            Tab::Modules => Tab::Operations,
            Tab::Sources => Tab::Modules,
            Tab::Events => Tab::Sources,
            Tab::Dlq => Tab::Events,
        };
        self.switch_tab(previous);
    }

    fn switch_tab(&mut self, tab: Tab) {
        self.current_tab = tab;
        self.selected_index = 0;
        self.copy_menu_open = false;
        self.copy_index = 0;
    }

    fn select_next(&mut self) {
        if self.copy_menu_open {
            self.select_next_copy_action();
            return;
        }
        let max_index = self.current_list_len().saturating_sub(1);
        if self.selected_index < max_index {
            self.selected_index += 1;
        }
    }

    fn select_previous(&mut self) {
        if self.copy_menu_open {
            self.select_previous_copy_action();
            return;
        }
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    fn current_list_len(&self) -> usize {
        match self.current_tab {
            Tab::Dashboard => 0,
            Tab::Operations => operations_room_cards(self).len(),
            Tab::Modules => self.modules.len(),
            Tab::Sources => self.source_coverage.len(),
            Tab::Events => self.recent_events.len(),
            Tab::Dlq => 0, // DLQ shows stats, not a navigable list
        }
    }

    fn clamp_selection(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            self.selected_index = 0;
            self.copy_menu_open = false;
            self.copy_index = 0;
        } else if self.selected_index >= len {
            self.selected_index = len - 1;
        }
    }

    fn selected_event_card(&self) -> Option<&EventCardView> {
        self.recent_events.get(self.selected_index)
    }

    fn selected_event_row(&self) -> Option<&QueryResultEvent> {
        self.recent_event_rows.get(self.selected_index)
    }

    fn selected_copy_actions(&self) -> Vec<EventCopyAction> {
        self.selected_event_card().map_or_else(Vec::new, |card| {
            event_copy_actions(card, self.selected_event_row())
        })
    }

    fn select_next_copy_action(&mut self) {
        let max_index = self.selected_copy_actions().len().saturating_sub(1);
        if self.copy_index < max_index {
            self.copy_index += 1;
        }
    }

    fn select_previous_copy_action(&mut self) {
        if self.copy_index > 0 {
            self.copy_index -= 1;
        }
    }

    fn toggle_copy_menu(&mut self) {
        if self.current_tab != Tab::Events || self.recent_events.is_empty() {
            self.feedback = Some("No selected event to copy from.".to_string());
            self.copy_menu_open = false;
            return;
        }
        self.copy_menu_open = !self.copy_menu_open;
        self.copy_index = 0;
        if self.copy_menu_open {
            self.feedback = Some("Copy menu open; select an item and press Enter.".to_string());
        }
    }

    fn toggle_payload_mode(&mut self) {
        if self.current_tab == Tab::Events {
            self.payload_raw = !self.payload_raw;
            let mode = if self.payload_raw {
                "raw JSON"
            } else {
                "pretty"
            };
            self.feedback = Some(format!("Payload renderer: {mode}."));
        }
    }

    fn should_auto_refresh(&self) -> bool {
        self.refresh_interval > 0
            && self.last_refresh.elapsed().as_secs() >= self.refresh_interval
            && !self.loading
    }

    async fn refresh(&mut self) {
        self.loading = true;
        self.error = None;

        // Fetch gateway version — abort refresh on connectivity failure.
        match self.client.version().await {
            Ok(v) => self.gateway_version = v,
            Err(e) => {
                self.error = Some(format!("Failed to connect: {e}"));
                self.loading = false;
                return;
            }
        }

        self.refresh_runtime_and_dlq().await;
        self.refresh_operations_and_state().await;
        self.refresh_sources_and_events().await;

        self.loading = false;
        self.last_refresh = Instant::now();
    }

    async fn refresh_runtime_and_dlq(&mut self) {
        match self.client.list_runtime(None).await {
            Ok(modules) => self.modules = modules,
            Err(e) => {
                self.error = Some(format!("Failed to fetch modules: {e}"));
            }
        }
        match self.client.dlq_list().await {
            Ok(stats) => self.dlq_stats = Some(stats),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch DLQ: {e}"));
                }
            }
        }
        match self.client.dlq_peek(Some(5)).await {
            Ok(peek) => self.dlq_peek = Some(peek),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch DLQ previews: {e}"));
                }
            }
        }
    }

    async fn refresh_operations_and_state(&mut self) {
        match self.client.ops_list(None, None, Some(10)).await {
            Ok(operations) => self.ops_operations = operations,
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch operations: {e}"));
                }
            }
        }
        match self.client.replay_list_filtered(None, None, Some(10)).await {
            Ok(operations) => self.replay_operations = operations,
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch replay operations: {e}"));
                }
            }
        }
        match self.client.lifecycle_status().await {
            Ok(status) => self.lifecycle_status = Some(status),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch lifecycle status: {e}"));
                }
            }
        }
        match self.client.private_mode_status().await {
            Ok(state) => self.private_mode = Some(state),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch private-mode status: {e}"));
                }
            }
        }
    }

    async fn refresh_sources_and_events(&mut self) {
        match self.client.sources_status_view().await {
            Ok(resp) => {
                self.source_coverage = resp.payload.sources;
                self.clamp_selection();
            }
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch source status: {e}"));
                }
            }
        }

        let query = EventQuery {
            time_range: TimeRange::new(Some(Timestamp::now() - Duration::hours(1)), None).ok(),
            limit: 50,
            direction: SortDirection::Desc,
            ..Default::default()
        };
        match self.client.query_events(query).await {
            Ok(EventQueryResult::Events { events, .. }) => {
                self.recent_events = EventCardListView::from_query_events(&events).cards;
                self.recent_event_rows = events;
                self.clamp_selection();
            }
            Ok(_) => {} // aggregation result, shouldn't happen
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch events: {e}"));
                }
                self.recent_event_rows.clear();
            }
        }
    }
}

impl TuiCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        // Setup terminal
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Create app
        let mut app = App::new(client.clone(), self.tab, self.refresh);

        // Initial refresh
        app.refresh().await;

        // Run the TUI
        let res = run_app(&mut terminal, &mut app).await;

        // Restore terminal
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        res
    }
}

async fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
) -> Result<()>
where
    B::Error: Send + Sync + 'static,
{
    loop {
        terminal.draw(|f| ui(f, app))?;

        // Check for auto-refresh
        if app.should_auto_refresh() {
            app.refresh().await;
        }

        // Poll for events with short timeout for responsive UI
        if event::poll(std::time::Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => {
                    if app.copy_menu_open {
                        app.copy_menu_open = false;
                        app.feedback = Some("Copy menu closed.".to_string());
                    } else {
                        app.should_quit = true;
                    }
                }
                KeyCode::Tab | KeyCode::Right => {
                    app.next_tab();
                }
                KeyCode::BackTab | KeyCode::Left => {
                    app.previous_tab();
                }
                KeyCode::Char('r') => {
                    app.refresh().await;
                }
                KeyCode::Char('?') => {
                    app.show_help = !app.show_help;
                }
                KeyCode::Char('c') => {
                    app.toggle_copy_menu();
                }
                KeyCode::Char('p') => {
                    app.toggle_payload_mode();
                }
                KeyCode::Enter | KeyCode::Char('y') => {
                    copy_selected_action(app);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.select_next();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    app.select_previous();
                }
                KeyCode::Char('1') => app.switch_tab(Tab::Dashboard),
                KeyCode::Char('2') => app.switch_tab(Tab::Operations),
                KeyCode::Char('3') => app.switch_tab(Tab::Modules),
                KeyCode::Char('4') => app.switch_tab(Tab::Sources),
                KeyCode::Char('5') => app.switch_tab(Tab::Events),
                KeyCode::Char('6') => app.switch_tab(Tab::Dlq),
                _ => {}
            }
        }

        if app.should_quit {
            break;
        }
    }
    Ok(())
}

fn copy_selected_action(app: &mut App) {
    if !app.copy_menu_open {
        return;
    }
    let actions = app.selected_copy_actions();
    let Some(action) = actions.get(app.copy_index) else {
        app.feedback = Some("No copy action selected.".to_string());
        return;
    };
    if let Some(value) = action.value.as_deref() {
        match copy_to_terminal_clipboard(value) {
            Ok(()) => {
                app.feedback = Some(format!(
                    "Copied {} via OSC52 clipboard request.",
                    action.label
                ));
                app.copy_menu_open = false;
            }
            Err(error) => {
                app.feedback = Some(format!("Copy failed for {}: {error}", action.label));
            }
        }
    } else {
        let reason = action
            .disabled_reason
            .as_deref()
            .unwrap_or("copy action is unavailable");
        app.feedback = Some(format!("Cannot copy {}: {reason}", action.label));
    }
}

fn copy_to_terminal_clipboard(text: &str) -> io::Result<()> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
    let sequence = format!("\x1b]52;c;{encoded}\x07");
    let mut stdout = io::stdout();
    execute!(stdout, Print(sequence))
}

fn ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tab bar
            Constraint::Min(0),    // Content
            Constraint::Length(3), // Status bar
        ])
        .split(f.area());

    // Tab bar
    render_tabs(f, chunks[0], app);

    // Content area
    match app.current_tab {
        Tab::Dashboard => render_dashboard(f, chunks[1], app),
        Tab::Operations => render_operations(f, chunks[1], app),
        Tab::Modules => render_modules(f, chunks[1], app),
        Tab::Sources => render_sources(f, chunks[1], app),
        Tab::Events => render_events(f, chunks[1], app),
        Tab::Dlq => render_dlq(f, chunks[1], app),
    }

    // Status bar
    render_status_bar(f, chunks[2], app);

    if app.show_help {
        render_help_overlay(f, f.area());
    }
}

fn render_tabs(f: &mut Frame, area: Rect, app: &App) {
    let tabs = [
        ("1:Dashboard", Tab::Dashboard),
        ("2:Ops", Tab::Operations),
        ("3:Modules", Tab::Modules),
        ("4:Sources", Tab::Sources),
        ("5:Events", Tab::Events),
        ("6:DLQ", Tab::Dlq),
    ];

    let mut tab_spans = vec![];
    for (i, (name, tab)) in tabs.iter().enumerate() {
        let style = if *tab == app.current_tab {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };
        tab_spans.push(Span::styled(format!(" {name} "), style));
        if i < tabs.len() - 1 {
            tab_spans.push(Span::raw(" │ "));
        }
    }

    let title = if app.loading {
        "Sinex CLI [loading...]"
    } else {
        "Sinex CLI"
    };

    let tabs_widget = Paragraph::new(Line::from(tab_spans))
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(tabs_widget, area);
}

fn render_status_bar(f: &mut Frame, area: Rect, app: &App) {
    let refresh_info = if app.refresh_interval > 0 {
        let elapsed = app.last_refresh.elapsed().as_secs();
        let next_in = app.refresh_interval.saturating_sub(elapsed);
        format!("Auto-refresh in {next_in}s")
    } else {
        "Auto-refresh: off".to_string()
    };

    let status_text = if let Some(feedback) = &app.feedback {
        format!("{feedback} | c:copy p:payload r:refresh ?:help q:quit")
    } else if let Some(err) = &app.error {
        format!("Error: {err} | Press 'r' to retry")
    } else {
        format!(
            "Gateway v{} | {} | ↑↓/jk:navigate Tab/←→:switch c:copy p:payload r:refresh ?:help q:quit",
            app.gateway_version, refresh_info
        )
    };

    let style = if app.feedback.is_some() {
        Style::default().fg(Color::Yellow)
    } else if app.error.is_some() {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let status = Paragraph::new(status_text)
        .style(style)
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(status, area);
}

fn render_dashboard(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Left: System overview
    // Consider a module healthy if it has a recent heartbeat.
    let healthy_modules = app
        .modules
        .iter()
        .filter(|n| {
            n.last_heartbeat
                .is_some_and(|hb| (Timestamp::now() - hb).whole_seconds() < 60)
        })
        .count();
    let total_modules = app.modules.len();
    let dlq_total = app.dlq_stats.as_ref().map_or(0, |s| s.total_messages);
    let events_count = app.recent_events.len();

    let overview_items = vec![
        ListItem::new(format!("Gateway Version: {}", app.gateway_version)),
        ListItem::new(""),
        ListItem::new(format!(
            "Healthy Modules: {healthy_modules}/{total_modules}"
        )),
        ListItem::new(format!("Recent Events (1h): {events_count}")),
        ListItem::new(format!(
            "DLQ Messages: {}",
            if dlq_total > 0 {
                format!("{dlq_total} ⚠")
            } else {
                "0 ✓".to_string()
            }
        )),
    ];

    let overview = List::new(overview_items)
        .block(
            Block::default()
                .title("System Overview")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::White));
    f.render_widget(overview, chunks[0]);

    // Right: RuntimeModule list
    let module_items: Vec<ListItem> = app
        .modules
        .iter()
        .map(|n| {
            let has_recent_heartbeat = n
                .last_heartbeat
                .is_some_and(|hb| (Timestamp::now() - hb).whole_seconds() < 60);
            let status_icon = if has_recent_heartbeat { "●" } else { "○" };
            let color = if has_recent_heartbeat {
                Color::Green
            } else {
                Color::Red
            };
            let leader = if n.is_leader { " [leader]" } else { "" };
            let name = n.hostname.as_deref().unwrap_or(&n.instance_id);
            ListItem::new(format!(
                "{} {} ({}){}",
                status_icon, name, n.module_kind, leader
            ))
            .style(Style::default().fg(color))
        })
        .collect();

    let runtime_list = List::new(if module_items.is_empty() {
        vec![ListItem::new("No modules registered")]
    } else {
        module_items
    })
    .block(Block::default().title("Modules").borders(Borders::ALL));
    f.render_widget(runtime_list, chunks[1]);
}

fn render_operations(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    let cards = operations_room_cards(app);
    let items: Vec<ListItem> = cards
        .iter()
        .enumerate()
        .map(|(index, card)| {
            let style = if index == app.selected_index {
                Style::default()
                    .fg(operation_card_color(card))
                    .add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(operation_card_color(card))
            };
            ListItem::new(format!(
                "{:<10} {:<10} {}",
                card.authority,
                card.phase,
                truncate_chars(&card.title, 58)
            ))
            .style(style)
        })
        .collect();
    let empty_label = if app.loading {
        "Loading operations..."
    } else if app.error.is_some() {
        "Operations unavailable; see status footer"
    } else {
        "No operation read models available"
    };
    let list = List::new(if items.is_empty() {
        vec![ListItem::new(empty_label)]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!("Operations Room ({} cards)", cards.len()))
            .borders(Borders::ALL),
    );
    f.render_widget(list, chunks[0]);

    let Some(card) = cards.get(app.selected_index) else {
        f.render_widget(
            Paragraph::new("Select an operation card to inspect authority and next actions.")
                .block(
                    Block::default()
                        .title("Authority Grammar")
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
        return;
    };
    render_operation_card_detail(f, chunks[1], card);
}

fn render_operation_card_detail(f: &mut Frame, area: Rect, card: &OperationRoomCard) {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("Authority  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                card.authority.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("Phase      ", Style::default().fg(Color::DarkGray)),
            Span::raw(card.phase.clone()),
        ]),
        Line::from(vec![
            Span::styled("Progress   ", Style::default().fg(Color::DarkGray)),
            Span::raw(card.progress.clone()),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Affected Refs",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];
    if card.affected_refs.is_empty() {
        lines.push(Line::from("none reported"));
    } else {
        for ref_ in card.affected_refs.iter().take(8) {
            lines.push(Line::from(ref_.clone()));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Caveats",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    if card.caveats.is_empty() {
        lines.push(Line::from("none"));
    } else {
        for caveat in card.caveats.iter().take(8) {
            lines.push(Line::from(caveat.clone()));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Actions",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    for action in &card.actions {
        lines.push(Line::from(format!(
            "{} [{}] — {}",
            action.label,
            action_state_label(action.state),
            action.command
        )));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Audit",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    for audit_ref in &card.audit_refs {
        lines.push(Line::from(audit_ref.clone()));
    }

    f.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(card.title.clone())
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        area,
    );
}

#[derive(Debug, Clone)]
struct OperationRoomCard {
    title: String,
    authority: String,
    phase: String,
    progress: String,
    affected_refs: Vec<String>,
    caveats: Vec<String>,
    actions: Vec<OperationRoomAction>,
    audit_refs: Vec<String>,
}

#[derive(Debug, Clone)]
struct OperationRoomAction {
    label: String,
    state: ActionAvailabilityState,
    command: String,
}

impl OperationRoomAction {
    fn new(
        label: impl Into<String>,
        state: ActionAvailabilityState,
        command: impl Into<String>,
    ) -> Self {
        Self {
            label: label.into(),
            state,
            command: command.into(),
        }
    }
}

fn operations_room_cards(app: &App) -> Vec<OperationRoomCard> {
    let mut cards = Vec::new();
    cards.extend(
        app.replay_operations
            .iter()
            .take(6)
            .map(replay_operation_card),
    );
    cards.extend(app.ops_operations.iter().take(6).map(ops_operation_card));
    cards.push(dlq_operation_card(app));
    if let Some(card) = automaton_dlq_card(app) {
        cards.push(card);
    }
    cards.push(lifecycle_operation_card(app));
    cards.push(state_snapshot_operation_card());
    cards.push(privacy_operation_card(app));
    cards
}

fn replay_operation_card(operation: &ReplayOperation) -> OperationRoomCard {
    let progress = format!(
        "{} / {} events, batch {}",
        operation.checkpoint.processed_events,
        operation.checkpoint.total_events,
        operation.checkpoint.batch_number
    );
    let mut actions = vec![
        OperationRoomAction::new(
            "monitor",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay watch {}", operation.operation_id),
        ),
        OperationRoomAction::new(
            "status",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay status {}", operation.operation_id),
        ),
    ];
    match operation.state {
        ReplayState::Planning => actions.push(OperationRoomAction::new(
            "preview",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay preview {}", operation.operation_id),
        )),
        ReplayState::Previewed => actions.push(OperationRoomAction::new(
            "confirm",
            ActionAvailabilityState::Dangerous,
            format!("sinexctl ops replay approve {}", operation.operation_id),
        )),
        ReplayState::Approved => actions.push(OperationRoomAction::new(
            "execute",
            ActionAvailabilityState::Dangerous,
            format!("sinexctl ops replay execute {}", operation.operation_id),
        )),
        ReplayState::Executing | ReplayState::Cancelling | ReplayState::Committing => {
            actions.push(OperationRoomAction::new(
                "cancel",
                ActionAvailabilityState::Dangerous,
                format!(
                    "sinexctl ops replay cancel {} --reason <reason>",
                    operation.operation_id
                ),
            ));
        }
        ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => {}
    }
    OperationRoomCard {
        title: format!("ops replay {}", operation.operation_id),
        authority: "write".to_string(),
        phase: format!("{:?}", operation.state).to_lowercase(),
        progress,
        affected_refs: replay_scope_refs(operation),
        caveats: replay_caveats(operation),
        actions,
        audit_refs: vec![format!("sinexctl ops audit {}", operation.operation_id)],
    }
}

fn replay_scope_refs(operation: &ReplayOperation) -> Vec<String> {
    let scope = &operation.scope;
    let mut refs = vec![format!("source: {}", scope.source_name)];
    if let Some((start, end)) = &scope.time_window {
        refs.push(format!("time: {start} -> {end}"));
    }
    if let Some(materials) = &scope.material_filter {
        refs.push(format!("materials: {}", materials.len()));
    }
    if let Some(source_id) = &scope.source_id {
        refs.push(format!("source: {source_id}"));
    }
    if let Some(source_material_id) = &scope.source_material_id {
        refs.push(format!("source-material: {source_material_id}"));
    }
    if let Some(parser_id) = &scope.parser_id {
        refs.push(format!("parser: {parser_id}"));
    }
    refs
}

fn replay_caveats(operation: &ReplayOperation) -> Vec<String> {
    let mut caveats = Vec::new();
    if operation.scope.is_staged_source_scope() {
        caveats.push("staged-source replay: inspect source readiness before execute".to_string());
    }
    if !operation.state.is_terminal()
        && matches!(
            operation.state,
            ReplayState::Previewed | ReplayState::Approved | ReplayState::Executing
        )
    {
        caveats.push("mutating replay phase: confirmation/audit trail required".to_string());
    }
    if let Some(error) = &operation.error_details {
        caveats.push(format!("error: {}", truncate_chars(error, 96)));
    }
    caveats
}

fn ops_operation_card(operation: &OpsOperation) -> OperationRoomCard {
    let view = OperationView::from_rpc(
        operation.id.clone(),
        &operation.operation_type,
        operation.operator.clone(),
        operation.result_status,
        operation.duration_ms,
        operation.result_message.clone(),
        operation.scope.clone(),
        operation.preview_summary.clone(),
    );

    OperationRoomCard {
        title: format!("operation {} ({})", view.id, view.kind),
        authority: "ops".to_string(),
        phase: view.status.to_string(),
        progress: view
            .duration_ms
            .map_or_else(|| "duration unknown".to_string(), |ms| format!("{ms}ms")),
        affected_refs: view
            .scope
            .as_ref()
            .map_or_else(Vec::new, |scope| vec![summarize_json_scope(scope)]),
        caveats: view
            .result_message
            .as_ref()
            .map_or_else(Vec::new, |message| vec![message.clone()]),
        actions: view
            .actions
            .iter()
            .filter_map(operation_room_action_from_availability)
            .collect(),
        audit_refs: vec![format!("sinexctl ops audit {}", view.id)],
    }
}

fn operation_room_action_from_availability(
    action: &ActionAvailability,
) -> Option<OperationRoomAction> {
    let command = action.command_hint.as_ref()?;
    let state = match (action.state, action.side_effect) {
        (
            ActionAvailabilityState::Enabled,
            ActionSideEffect::Write | ActionSideEffect::Admin | ActionSideEffect::Destructive,
        ) => ActionAvailabilityState::Dangerous,
        (state, _) => state,
    };
    Some(OperationRoomAction::new(
        action.label.to_lowercase(),
        state,
        command.clone(),
    ))
}

fn dlq_operation_card(app: &App) -> OperationRoomCard {
    let stats = app.dlq_stats.as_ref();
    let total = stats.map_or(0, |stats| stats.total_messages);
    let bytes = stats.map_or(0, |stats| stats.total_bytes);
    let mut caveats = Vec::new();
    if total > 0 {
        caveats.push(
            "requeue/purge is mutating; inspect peek output and source readiness first".to_string(),
        );
    }
    OperationRoomCard {
        title: "raw-ingest DLQ".to_string(),
        authority: if total > 0 { "admin" } else { "read" }.to_string(),
        phase: if total > 0 { "blocked" } else { "clear" }.to_string(),
        progress: format!("{total} message(s), {}", format_bytes(bytes)),
        affected_refs: stats.map_or_else(Vec::new, |stats| {
            vec![format!("seq {}..{}", stats.first_seq, stats.last_seq)]
        }),
        caveats,
        actions: vec![
            OperationRoomAction::new(
                "peek",
                ActionAvailabilityState::Enabled,
                "sinexctl ops dlq peek --limit 10",
            ),
            OperationRoomAction::new(
                "requeue",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops dlq requeue --all",
            ),
            OperationRoomAction::new(
                "purge",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops dlq purge --confirm",
            ),
        ],
        audit_refs: vec!["sinexctl ops dlq list".to_string()],
    }
}

fn automaton_dlq_card(app: &App) -> Option<OperationRoomCard> {
    let message = app
        .dlq_peek
        .as_ref()?
        .messages
        .iter()
        .find(|message| is_automaton_material_dlq(message))?;
    Some(OperationRoomCard {
        title: "automaton telemetry DLQ material gap".to_string(),
        authority: "admin".to_string(),
        phase: "blocked".to_string(),
        progress: format!(
            "sample seq {}, retry {}",
            message.sequence, message.retry_count
        ),
        affected_refs: vec![
            format!("subject: {}", message.subject),
            format!(
                "original: {}",
                message.original_subject.as_deref().unwrap_or("unknown")
            ),
            format!(
                "failed event sample: {}",
                truncate_chars(&message.payload_preview, 96)
            ),
        ],
        caveats: vec![
            "first-class DLQ class: likely missing source-material registration for derived telemetry".to_string(),
            "requeue will probably re-DLQ until the Source Readiness Cockpit row is fixed".to_string(),
            "downstream projections may miss automaton telemetry until repaired".to_string(),
        ],
        actions: vec![
            OperationRoomAction::new(
                "inspect source",
                ActionAvailabilityState::Enabled,
                "sinexctl tui --tab sources",
            ),
            OperationRoomAction::new(
                "peek",
                ActionAvailabilityState::Enabled,
                "sinexctl ops dlq peek --limit 10",
            ),
            OperationRoomAction::new(
                "requeue after repair",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops dlq requeue --all",
            ),
        ],
        audit_refs: vec!["Ref #1241 automaton telemetry DLQ verification".to_string()],
    })
}

fn is_automaton_material_dlq(message: &DlqMessagePeek) -> bool {
    let haystack = format!(
        "{} {} {}",
        message.subject,
        message.original_subject.as_deref().unwrap_or_default(),
        message.payload_preview
    )
    .to_ascii_lowercase();
    haystack.contains("derived")
        && (haystack.contains("source_material")
            || haystack.contains("source material")
            || haystack.contains("material"))
}

fn lifecycle_operation_card(app: &App) -> OperationRoomCard {
    let affected_refs = app
        .lifecycle_status
        .as_ref()
        .map_or_else(Vec::new, |status| {
            status
                .tiers
                .iter()
                .map(|tier| {
                    format!(
                        "{:?}: {} event(s), {} source(s)",
                        tier.tier, tier.event_count, tier.distinct_sources
                    )
                })
                .collect()
        });
    let total_events = app
        .lifecycle_status
        .as_ref()
        .map_or(0, |status| status.total_events);
    OperationRoomCard {
        title: "ops lifecycle archive/restore/tombstone".to_string(),
        authority: "admin".to_string(),
        phase: "guarded".to_string(),
        progress: format!("{total_events} event(s) across lifecycle tiers"),
        affected_refs,
        caveats: vec![
            "archive/restore supports dry-run; tombstone is destructive and preview/approve gated"
                .to_string(),
        ],
        actions: vec![
            OperationRoomAction::new(
                "archive dry-run",
                ActionAvailabilityState::Enabled,
                "sinexctl ops lifecycle archive --limit 1000",
            ),
            OperationRoomAction::new(
                "restore dry-run",
                ActionAvailabilityState::Enabled,
                "sinexctl ops lifecycle restore <event-id>...",
            ),
            OperationRoomAction::new(
                "tombstone preview",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops lifecycle tombstone preview <operation-id>",
            ),
            OperationRoomAction::new(
                "tombstone approve",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops lifecycle tombstone approve <operation-id>",
            ),
        ],
        audit_refs: vec!["sinexctl ops lifecycle status".to_string()],
    }
}

fn state_snapshot_operation_card() -> OperationRoomCard {
    OperationRoomCard {
        title: "state snapshot and restore drill".to_string(),
        authority: "admin".to_string(),
        phase: "target".to_string(),
        progress: "snapshot is read-only; restore requires explicit plan".to_string(),
        affected_refs: vec!["runtime state bundle".to_string()],
        caveats: vec!["restore drill must not look like navigation; require explicit target directory and plan review".to_string()],
        actions: vec![
            OperationRoomAction::new(
                "snapshot",
                ActionAvailabilityState::Enabled,
                "sinexctl ops state snapshot",
            ),
            OperationRoomAction::new(
                "inspect",
                ActionAvailabilityState::Enabled,
                "sinexctl ops state inspect --archive <archive>",
            ),
            OperationRoomAction::new(
                "restore plan",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops state restore --archive <archive> --target-dir <dir> --dry-run",
            ),
        ],
        audit_refs: vec!["state snapshot artifact path".to_string()],
    }
}

fn privacy_operation_card(app: &App) -> OperationRoomCard {
    let state = app.private_mode.as_ref().map(|response| &response.state);
    let enabled = state.is_some_and(|state| state.enabled);
    let mut affected_refs = state.map_or_else(Vec::new, |state| {
        let mut refs = vec![format!("actor: {}", state.actor)];
        if state.affected_source_classes.is_empty() {
            refs.push("source classes: all/default".to_string());
        } else {
            refs.push(format!(
                "source classes: {}",
                state.affected_source_classes.join(", ")
            ));
        }
        if let Some(operation_id) = &state.updated_by_operation_id {
            refs.push(format!("operation: {operation_id}"));
        }
        refs
    });
    if affected_refs.is_empty() {
        affected_refs.push("private-mode state unavailable".to_string());
    }
    OperationRoomCard {
        title: "privacy export/delete/redact authority".to_string(),
        authority: "write".to_string(),
        phase: if enabled { "private-mode" } else { "normal" }.to_string(),
        progress: if enabled {
            "private mode enabled".to_string()
        } else {
            "private mode disabled".to_string()
        },
        affected_refs,
        caveats: vec![
            "privacy export requires explicit scope".to_string(),
            "delete/redact remain target operations until concrete command surfaces land"
                .to_string(),
        ],
        actions: vec![
            OperationRoomAction::new(
                "audit",
                ActionAvailabilityState::Enabled,
                "sinexctl privacy audit",
            ),
            OperationRoomAction::new(
                "export scoped",
                ActionAvailabilityState::Dangerous,
                "sinexctl privacy export --since 24h --source <source> --output <file>",
            ),
            OperationRoomAction::new(
                "delete",
                ActionAvailabilityState::Target,
                "not implemented: privacy delete",
            ),
            OperationRoomAction::new(
                "redact",
                ActionAvailabilityState::Target,
                "not implemented: privacy redact",
            ),
        ],
        audit_refs: vec!["sinexctl privacy private-mode status".to_string()],
    }
}

fn operation_card_color(card: &OperationRoomCard) -> Color {
    if card.phase.contains("failed") || card.phase.contains("blocked") {
        Color::Red
    } else if card
        .actions
        .iter()
        .any(|action| matches!(action.state, ActionAvailabilityState::Dangerous))
    {
        Color::Yellow
    } else {
        Color::White
    }
}

fn summarize_json_scope(scope: &serde_json::Value) -> String {
    match scope {
        serde_json::Value::Object(map) => {
            let keys = map.keys().take(6).cloned().collect::<Vec<_>>().join(", ");
            format!("scope keys: {keys}")
        }
        other => truncate_chars(&other.to_string(), 96),
    }
}

fn render_modules(f: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .modules
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let has_recent_heartbeat = n
                .last_heartbeat
                .is_some_and(|hb| (Timestamp::now() - hb).whole_seconds() < 60);
            let status_icon = if has_recent_heartbeat { "●" } else { "○" };
            let color = if has_recent_heartbeat {
                Color::Green
            } else {
                Color::Red
            };
            let style = if i == app.selected_index {
                Style::default().fg(color).add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(color)
            };
            let name = n.hostname.as_deref().unwrap_or(&n.instance_id);
            let leader = if n.is_leader { " [leader]" } else { "" };
            let heartbeat_str = n
                .last_heartbeat
                .as_ref()
                .map_or_else(|| "none".to_string(), format_heartbeat_age);
            ListItem::new(format!(
                "{} {} | Type: {} | Heartbeat: {}{}",
                status_icon, name, n.module_kind, heartbeat_str, leader
            ))
            .style(style)
        })
        .collect();

    let list = List::new(if items.is_empty() {
        vec![ListItem::new("No modules registered")]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!("Modules ({} total)", app.modules.len()))
            .borders(Borders::ALL),
    );
    f.render_widget(list, area);
}

fn render_sources(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(52), Constraint::Percentage(48)])
        .split(area);

    let items: Vec<ListItem> = app
        .source_coverage
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let state = source_cockpit_state(source);
            let style = if index == app.selected_index {
                Style::default()
                    .fg(source_state_color(state))
                    .add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(source_state_color(state))
            };
            let event_types = event_types_summary(source);
            let actions = source
                .actions
                .iter()
                .filter(|action| action.state == ActionAvailabilityState::Enabled)
                .count();
            ListItem::new(format!(
                "{:<10} {:<14} {} | ns {} | type {} | mat {} evt {} | gaps {} | act {}",
                source_state_label(state),
                continuity_label(source.continuity),
                truncate_chars(&source.source_id, 34),
                source.namespace,
                truncate_chars(&event_types, 24),
                source.material_count,
                source.event_count,
                source.gaps.len(),
                actions
            ))
            .style(style)
        })
        .collect();

    let empty_label = if app.loading {
        "Loading source status..."
    } else if app.error.is_some() {
        "Source status unavailable; see status footer"
    } else {
        "No source status records"
    };
    let list = List::new(if items.is_empty() {
        vec![ListItem::new(empty_label)]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!(
                "Sources ({} coverage rows)",
                app.source_coverage.len()
            ))
            .borders(Borders::ALL),
    );
    f.render_widget(list, chunks[0]);
    render_source_detail(f, chunks[1], app);
}

fn render_source_detail(f: &mut Frame, area: Rect, app: &App) {
    let Some(source) = app.source_coverage.get(app.selected_index) else {
        let message = if app.loading {
            "Loading selected source..."
        } else if app.error.is_some() {
            "Source detail unavailable while refresh has errors."
        } else {
            "Select a source to inspect."
        };
        f.render_widget(
            Paragraph::new(message)
                .block(
                    Block::default()
                        .title("Source Detail")
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: true }),
            area,
        );
        return;
    };

    let state = source_cockpit_state(source);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("State  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                source_state_label(state),
                Style::default()
                    .fg(source_state_color(state))
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("Source  ", Style::default().fg(Color::DarkGray)),
            Span::raw(source.source_id.clone()),
        ]),
        Line::from(vec![
            Span::styled("Namespace  ", Style::default().fg(Color::DarkGray)),
            Span::raw(source.namespace.clone()),
        ]),
        Line::from(vec![
            Span::styled("Readiness  ", Style::default().fg(Color::DarkGray)),
            Span::raw(readiness_label(source.readiness)),
        ]),
        Line::from(vec![
            Span::styled("Continuity  ", Style::default().fg(Color::DarkGray)),
            Span::raw(continuity_label(source.continuity)),
        ]),
        Line::from(vec![
            Span::styled("Counts  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} material(s), {} event(s), {} binding(s)",
                source.material_count, source.event_count, source.binding_count
            )),
        ]),
        Line::from(vec![
            Span::styled("Privacy  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{}/{}{}",
                source.privacy.tier,
                source.privacy.context,
                if source.privacy.proposed {
                    " (proposed)"
                } else {
                    ""
                }
            )),
        ]),
        Line::from(vec![
            Span::styled("Event Types  ", Style::default().fg(Color::DarkGray)),
            Span::raw(event_types_summary(source)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Gap Explanation",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];

    if source.gaps.is_empty() {
        lines.push(Line::from(format!(
            "No DTO-reported continuity gaps; {} material(s), {} event(s)",
            source.material_count, source.event_count
        )));
    } else {
        lines.push(Line::from(format!(
            "{} DTO-reported gap(s)",
            source.gaps.len()
        )));
        for gap in source.gaps.iter().take(4) {
            lines.push(Line::from(format!("{}: {}", gap.kind, gap.message)));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Caveats",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    if source.caveats.is_empty() {
        lines.push(Line::from("none"));
    } else {
        for caveat in source.caveats.iter().take(8) {
            lines.push(Line::from(format!("{} {}", caveat.id, caveat.message)));
            if let Some(ref_) = &caveat.ref_ {
                lines.push(Line::from(format!("  ref: {}", ref_.id)));
            }
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Actions",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    if source.actions.is_empty() {
        lines.push(Line::from("none"));
    } else {
        for action in source.actions.iter().take(8) {
            lines.push(Line::from(format!(
                "{} [{}] {}",
                action.label,
                source_action_state_label(action.state),
                action
                    .command_hint
                    .as_deref()
                    .or(action.rpc_method.as_deref())
                    .unwrap_or(&action.id)
            )));
            if let Some(reason) = &action.reason {
                lines.push(Line::from(format!("  reason: {reason}")));
            }
        }
    }
    if matches!(state, SourceCockpitState::Unparsed) {
        lines.push(Line::from(
            "Explore Bridge [target] - staged-but-unparsed material (#1062)",
        ));
    }
    if matches!(state, SourceCockpitState::Drift) {
        lines.push(Line::from("sinexctl sources drift"));
    }

    f.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title("Source Detail")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        area,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceCockpitState {
    Ready,
    Proposed,
    MaterialOnly,
    EventOnly,
    MissingMaterial,
    MissingEvents,
    MissingBinding,
    Drift,
    Unparsed,
    Blocked,
    Unknown,
}

fn source_cockpit_state(source: &SourceCoverageView) -> SourceCockpitState {
    if source_has_drift_caveat(source) {
        return SourceCockpitState::Drift;
    }
    if source
        .actions
        .iter()
        .any(|action| action.state == ActionAvailabilityState::Disabled)
    {
        return SourceCockpitState::Blocked;
    }
    if source_has_unparsed_caveat(source) || (source.material_count > 0 && source.event_count == 0)
    {
        return SourceCockpitState::Unparsed;
    }
    match (source.readiness, source.continuity) {
        (SourceCoverageReadiness::Ready, SourceCoverageContinuity::Active) => {
            SourceCockpitState::Ready
        }
        (SourceCoverageReadiness::Proposed, _) => SourceCockpitState::Proposed,
        (SourceCoverageReadiness::MissingMaterial, _) => SourceCockpitState::MissingMaterial,
        (SourceCoverageReadiness::MissingEvents, _) => SourceCockpitState::MissingEvents,
        (SourceCoverageReadiness::MissingBinding, _) => SourceCockpitState::MissingBinding,
        (_, SourceCoverageContinuity::MaterialOnly) => SourceCockpitState::MaterialOnly,
        (_, SourceCoverageContinuity::EventOnly) => SourceCockpitState::EventOnly,
        (_, SourceCoverageContinuity::Gapped) => SourceCockpitState::Unparsed,
        (_, SourceCoverageContinuity::Unknown) => SourceCockpitState::Unknown,
    }
}

fn source_has_drift_caveat(source: &SourceCoverageView) -> bool {
    source.caveats.iter().any(|caveat| {
        matches!(
            caveat.id.as_str(),
            "parser.version_drift"
                | "source.shape_changed"
                | "parser.required_field_missing"
                | "parser.field_type_changed"
        )
    })
}

fn source_has_unparsed_caveat(source: &SourceCoverageView) -> bool {
    source.caveats.iter().any(|caveat| {
        matches!(
            caveat.id.as_str(),
            "material.staged_unparsed" | "parser.no_binding" | "parser.jobs_untracked"
        )
    })
}

fn source_state_label(state: SourceCockpitState) -> &'static str {
    match state {
        SourceCockpitState::Ready => "ready",
        SourceCockpitState::Proposed => "proposed",
        SourceCockpitState::MaterialOnly => "material-only",
        SourceCockpitState::EventOnly => "event-only",
        SourceCockpitState::MissingMaterial => "missing-material",
        SourceCockpitState::MissingEvents => "missing-events",
        SourceCockpitState::MissingBinding => "missing-binding",
        SourceCockpitState::Drift => "drift",
        SourceCockpitState::Unparsed => "unparsed",
        SourceCockpitState::Blocked => "blocked",
        SourceCockpitState::Unknown => "unknown",
    }
}

fn source_state_color(state: SourceCockpitState) -> Color {
    match state {
        SourceCockpitState::Ready => Color::Green,
        SourceCockpitState::Proposed
        | SourceCockpitState::MaterialOnly
        | SourceCockpitState::EventOnly
        | SourceCockpitState::MissingMaterial
        | SourceCockpitState::MissingEvents => Color::Yellow,
        SourceCockpitState::MissingBinding
        | SourceCockpitState::Drift
        | SourceCockpitState::Unparsed
        | SourceCockpitState::Blocked => Color::Red,
        SourceCockpitState::Unknown => Color::DarkGray,
    }
}

fn readiness_label(readiness: SourceCoverageReadiness) -> &'static str {
    match readiness {
        SourceCoverageReadiness::Ready => "ready",
        SourceCoverageReadiness::Proposed => "proposed",
        SourceCoverageReadiness::MissingMaterial => "missing-material",
        SourceCoverageReadiness::MissingEvents => "missing-events",
        SourceCoverageReadiness::MissingBinding => "missing-binding",
    }
}

fn continuity_label(continuity: SourceCoverageContinuity) -> &'static str {
    match continuity {
        SourceCoverageContinuity::Active => "active",
        SourceCoverageContinuity::MaterialOnly => "material-only",
        SourceCoverageContinuity::EventOnly => "event-only",
        SourceCoverageContinuity::Gapped => "gapped",
        SourceCoverageContinuity::Unknown => "unknown",
    }
}

fn event_types_summary(source: &SourceCoverageView) -> String {
    match source.event_types.len() {
        0 => "-".to_string(),
        1 => source.event_types[0].clone(),
        n => format!("{} (+{})", source.event_types[0], n - 1),
    }
}

fn source_action_state_label(state: ActionAvailabilityState) -> &'static str {
    match state {
        ActionAvailabilityState::Enabled => "enabled",
        ActionAvailabilityState::Disabled => "disabled",
        ActionAvailabilityState::Target => "target",
        ActionAvailabilityState::Loading => "loading",
        ActionAvailabilityState::Dangerous => "dangerous",
        ActionAvailabilityState::Partial => "partial",
        ActionAvailabilityState::Unavailable => "unavailable",
    }
}

fn render_events(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
        .split(area);

    let items: Vec<ListItem> = app
        .recent_events
        .iter()
        .enumerate()
        .map(|(i, card)| {
            let style = if i == app.selected_index {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            let timestamp = card.timestamp.original.map_or_else(
                || "unknown".to_string(),
                |ts| {
                    ts.format(time::macros::format_description!(
                        "[hour]:[minute]:[second]"
                    ))
                    .unwrap_or_else(|_| "invalid".to_string())
                },
            );
            let snippet = truncate_chars(&card.summary, 60);
            ListItem::new(format!(
                "{} [{}] {} - {}",
                timestamp, card.source.raw, card.event_type, snippet
            ))
            .style(style)
        })
        .collect();

    let empty_label = if app.loading {
        "Loading recent events..."
    } else if app.error.is_some() {
        "Recent events unavailable; see status footer"
    } else {
        "No recent events in the last hour"
    };
    let title = if app.loading && !app.recent_events.is_empty() {
        format!(
            "Recent Events ({} in last hour, refreshing)",
            app.recent_events.len()
        )
    } else {
        format!("Recent Events ({} in last hour)", app.recent_events.len())
    };
    let list = List::new(if items.is_empty() {
        vec![ListItem::new(empty_label)]
    } else {
        items
    })
    .block(Block::default().title(title).borders(Borders::ALL));
    f.render_widget(list, chunks[0]);

    render_event_inspector(f, chunks[1], app);
    if app.copy_menu_open {
        render_copy_menu(f, chunks[1], app);
    }
}

fn render_event_inspector(f: &mut Frame, area: Rect, app: &App) {
    let Some(card) = app.recent_events.get(app.selected_index) else {
        let message = if app.loading {
            "Loading selected event..."
        } else if app.error.is_some() {
            "Event inspector unavailable while refresh has errors."
        } else {
            "Select an event to inspect."
        };
        let panel = Paragraph::new(message)
            .block(Block::default().title("Inspector").borders(Borders::ALL))
            .wrap(Wrap { trim: true });
        f.render_widget(panel, area);
        return;
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("ID  ", Style::default().fg(Color::DarkGray)),
            Span::raw(card.ref_.id.clone()),
        ]),
        Line::from(vec![
            Span::styled("Type  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} / {}", card.source.raw, card.event_type)),
        ]),
        Line::from(vec![
            Span::styled("Time  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_event_time(card)),
        ]),
        Line::from(vec![
            Span::styled("Privacy  ", Style::default().fg(Color::DarkGray)),
            Span::raw(privacy_state_label(card.privacy_state.state)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Summary",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(card.summary.clone()),
        Line::from(""),
        Line::from(Span::styled(
            if app.payload_raw {
                "Payload Raw JSON"
            } else {
                "Payload"
            },
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];

    lines.extend(payload_lines(
        card,
        app.selected_event_row(),
        app.payload_raw,
    ));
    lines.extend([
        Line::from(""),
        Line::from(Span::styled(
            "Refs",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ]);

    if card.material_refs.is_empty() && card.trace_refs.is_empty() {
        lines.push(Line::from("none"));
    } else {
        for ref_ in card.material_refs.iter().chain(card.trace_refs.iter()) {
            lines.push(Line::from(format!(
                "{}:{} {}",
                object_kind_label(&ref_.kind),
                ref_.id,
                ref_.label.as_deref().unwrap_or("")
            )));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Actions",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    for action in &card.actions {
        let state = action_state_label(action.state);
        let reason = action
            .reason
            .as_deref()
            .map_or_else(String::new, |reason| format!(" — {reason}"));
        lines.push(Line::from(format!(
            "{} [{}]{}",
            action.label, state, reason
        )));
    }
    lines.push(Line::from(
        "Context pack [target] — disabled until context packs land (#1095)",
    ));

    if !card.caveats.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Caveats",
            Style::default().add_modifier(Modifier::BOLD),
        )));
        for caveat in &card.caveats {
            lines.push(Line::from(format!("{}: {}", caveat.id, caveat.message)));
        }
    }

    let panel = Paragraph::new(lines)
        .block(Block::default().title("Inspector").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    f.render_widget(panel, area);
}

fn render_copy_menu(f: &mut Frame, area: Rect, app: &App) {
    let width = area.width.saturating_sub(4).max(1);
    let height = app
        .selected_copy_actions()
        .len()
        .saturating_add(4)
        .min(area.height as usize)
        .max(6) as u16;
    let popup = Rect {
        x: area.x + 2.min(area.width),
        y: area.y + 2.min(area.height),
        width,
        height,
    };
    let actions = app.selected_copy_actions();
    let items = if actions.is_empty() {
        vec![ListItem::new("No copyable event selected")]
    } else {
        actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                let state = if let Some(reason) = &action.disabled_reason {
                    format!("disabled: {reason}")
                } else {
                    "copyable".to_string()
                };
                let style = if index == app.copy_index {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else if action.disabled_reason.is_some() {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                };
                ListItem::new(format!("{} — {state}", action.label)).style(style)
            })
            .collect()
    };
    f.render_widget(Clear, popup);
    f.render_widget(
        List::new(items).block(
            Block::default()
                .title("Copy Menu (Enter/y copies, Esc closes)")
                .borders(Borders::ALL),
        ),
        popup,
    );
}

fn payload_lines(
    card: &EventCardView,
    row: Option<&QueryResultEvent>,
    raw: bool,
) -> Vec<Line<'static>> {
    let payload = row
        .map(|row| &row.event.payload)
        .or(card.payload_preview.as_ref());
    let Some(payload) = payload else {
        return vec![Line::from("payload unavailable")];
    };

    let rendered = if raw {
        serde_json::to_string_pretty(payload)
    } else {
        render_pretty_payload(payload)
    };
    match rendered {
        Ok(text) => text
            .lines()
            .take(12)
            .map(|line| Line::from(truncate_chars(line, 96)))
            .collect(),
        Err(error) => vec![
            Line::from(format!("payload rendering failed: {error}")),
            Line::from("press p to switch to raw JSON"),
        ],
    }
}

fn render_pretty_payload(payload: &serde_json::Value) -> serde_json::Result<String> {
    match payload {
        serde_json::Value::Object(map) => {
            let mut lines = Vec::new();
            for (key, value) in map.iter().take(12) {
                let value = match value {
                    serde_json::Value::String(value) => value.clone(),
                    other => serde_json::to_string(other)?,
                };
                lines.push(format!("{key}: {}", truncate_chars(&value, 84)));
            }
            if map.len() > 12 {
                lines.push(format!("... {} more field(s)", map.len() - 12));
            }
            Ok(lines.join("\n"))
        }
        other => serde_json::to_string_pretty(other),
    }
}

#[derive(Debug, Clone)]
struct EventCopyAction {
    label: String,
    value: Option<String>,
    disabled_reason: Option<String>,
}

impl EventCopyAction {
    fn available(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: Some(value.into()),
            disabled_reason: None,
        }
    }

    fn disabled(label: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: None,
            disabled_reason: Some(reason.into()),
        }
    }
}

fn event_copy_actions(
    card: &EventCardView,
    row: Option<&QueryResultEvent>,
) -> Vec<EventCopyAction> {
    let mut actions = Vec::new();
    let event_id = &card.ref_.id;
    if event_id == "unpersisted" {
        actions.push(EventCopyAction::disabled(
            "event id",
            "event has no stable persisted id",
        ));
        actions.push(EventCopyAction::disabled(
            "trace command",
            "event has no stable persisted id",
        ));
        actions.push(EventCopyAction::disabled(
            "explain command",
            "event has no stable persisted id",
        ));
    } else {
        actions.push(EventCopyAction::available("event id", event_id.clone()));
        actions.push(EventCopyAction::available(
            "trace command",
            format!("sinexctl events trace {event_id}"),
        ));
        actions.push(EventCopyAction::available(
            "citation",
            format!("sinex:event:{event_id}"),
        ));
        actions.push(EventCopyAction::available(
            "inspect command",
            format!("sinexctl events inspect {event_id}"),
        ));
    }

    actions.push(EventCopyAction::available(
        "reselect query",
        format!(
            "sinexctl events query --source {} --event-type {} -n 20",
            card.source.raw, card.event_type
        ),
    ));
    if let Some(row) = row {
        let event_json = serde_json::to_string_pretty(&row.event)
            .unwrap_or_else(|error| format!("event JSON render failed: {error}"));
        actions.push(EventCopyAction::available("event JSON", event_json));
        let payload_json = serde_json::to_string_pretty(&row.event.payload)
            .unwrap_or_else(|error| format!("payload JSON render failed: {error}"));
        actions.push(EventCopyAction::available("payload JSON", payload_json));
    } else {
        actions.push(EventCopyAction::disabled(
            "event JSON",
            "raw query event is unavailable",
        ));
        actions.push(EventCopyAction::disabled(
            "payload JSON",
            "raw query event is unavailable",
        ));
    }

    if let Some(anchor) = card
        .material_refs
        .iter()
        .find(|ref_| matches!(ref_.kind, SinexObjectKind::MaterialAnchor))
    {
        actions.push(EventCopyAction::available(
            "source anchor",
            anchor.id.clone(),
        ));
    } else {
        actions.push(EventCopyAction::disabled(
            "source anchor",
            "selected event has no material anchor",
        ));
    }

    actions.push(EventCopyAction::disabled(
        "context pack",
        "target-only; tracked by #1095",
    ));
    actions
}

fn privacy_state_label(state: PrivacyStateKind) -> &'static str {
    match state {
        PrivacyStateKind::RawVisible => "raw visible",
        PrivacyStateKind::MetadataOnly => "metadata only",
        PrivacyStateKind::Redacted => "redacted",
        PrivacyStateKind::Suppressed => "suppressed",
        PrivacyStateKind::PermissionDenied => "permission denied",
        PrivacyStateKind::PolicyBlocked => "policy blocked",
        PrivacyStateKind::TombstonePending => "tombstone pending",
        PrivacyStateKind::ExportRestricted => "export restricted",
    }
}

fn action_state_label(state: ActionAvailabilityState) -> &'static str {
    match state {
        ActionAvailabilityState::Enabled => "current",
        ActionAvailabilityState::Disabled => "disabled",
        ActionAvailabilityState::Target => "target",
        ActionAvailabilityState::Loading => "loading",
        ActionAvailabilityState::Dangerous => "dangerous",
        ActionAvailabilityState::Partial => "partial",
        ActionAvailabilityState::Unavailable => "unavailable",
    }
}

fn object_kind_label(kind: &SinexObjectKind) -> &'static str {
    match kind {
        SinexObjectKind::Event => "event",
        SinexObjectKind::SourceDriver => "source",
        SinexObjectKind::SourceMaterial => "source-material",
        SinexObjectKind::MaterialAnchor => "material-anchor",
        SinexObjectKind::Document => "document",
        SinexObjectKind::DocumentChunk => "document-chunk",
        SinexObjectKind::Task => "task",
        SinexObjectKind::SemanticLane => "semantic-lane",
        SinexObjectKind::SemanticEntity => "semantic-entity",
        SinexObjectKind::SemanticRelation => "semantic-relation",
        SinexObjectKind::Operation => "operation",
        SinexObjectKind::ReplayRun => "replay-run",
        SinexObjectKind::Snapshot => "snapshot",
        SinexObjectKind::DlqMessage => "dlq-message",
        SinexObjectKind::ContextPack => "context-pack",
        SinexObjectKind::MomentCandidate => "moment-candidate",
        SinexObjectKind::PrivacySession => "privacy-session",
        SinexObjectKind::Caveat => "caveat",
        SinexObjectKind::RpcMethod => "rpc-method",
        SinexObjectKind::Command => "command",
    }
}

fn format_event_time(card: &EventCardView) -> String {
    card.timestamp.original.map_or_else(
        || "unknown".to_string(),
        |ts| {
            ts.format(time::macros::format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second]"
            ))
            .unwrap_or_else(|_| "invalid".to_string())
        },
    )
}

fn render_help_overlay(f: &mut Frame, area: Rect) {
    let width = area.width.saturating_mul(2) / 3;
    let height = 13;
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    let popup = Rect {
        x,
        y,
        width,
        height: height.min(area.height),
    };
    let lines = vec![
        Line::from("Tab / arrows: switch workbench surface"),
        Line::from("j/k or up/down: move selection"),
        Line::from("c: open copy menu for selected event"),
        Line::from("Enter/y: copy selected copy-menu item"),
        Line::from("p: toggle pretty/raw payload renderer"),
        Line::from("r: refresh all panes"),
        Line::from("?: toggle this help panel"),
        Line::from("q or Esc: quit"),
        Line::from(""),
        Line::from("Operations cards label current, dangerous, target, and disabled actions."),
        Line::from("Copy feedback appears in the status bar."),
        Line::from("Context-pack action is target-only until #1095."),
    ];
    f.render_widget(Clear, popup);
    f.render_widget(
        Paragraph::new(lines)
            .block(Block::default().title("Help").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        popup,
    );
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let end = input
        .char_indices()
        .nth(keep)
        .map_or(input.len(), |(index, _)| index);
    format!("{}...", &input[..end])
}

fn render_dlq(f: &mut Frame, area: Rect, app: &App) {
    let (block_title, items) = match &app.dlq_stats {
        Some(stats) if stats.total_messages > 0 => {
            let title = format!("Raw Ingest DLQ ({} messages) ⚠", stats.total_messages);
            let items = vec![
                ListItem::new(format!("Total Messages: {}", stats.total_messages))
                    .style(Style::default().fg(Color::Yellow)),
                ListItem::new(format!("Total Size: {}", format_bytes(stats.total_bytes))),
                ListItem::new(format!("First Sequence: {}", stats.first_seq)),
                ListItem::new(format!("Last Sequence: {}", stats.last_seq)),
                ListItem::new(""),
                ListItem::new("Use 'sinexctl ops dlq peek' to inspect raw-ingest failures."),
                ListItem::new("Use 'sinexctl ops dlq requeue --all' to retry."),
            ];
            (title, items)
        }
        Some(_) => {
            let title = "Raw Ingest DLQ (empty) ✓".to_string();
            let items = vec![
                ListItem::new("No messages in the raw-ingest DLQ.")
                    .style(Style::default().fg(Color::Green)),
                ListItem::new(""),
                ListItem::new("Messages that fail raw ingest appear here."),
            ];
            (title, items)
        }
        None => {
            let title = "Raw Ingest DLQ".to_string();
            let items = vec![ListItem::new("Loading...")];
            (title, items)
        }
    };

    let list = List::new(items).block(Block::default().title(block_title).borders(Borders::ALL));
    f.render_widget(list, area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientConfig;
    use ratatui::backend::TestBackend;
    use sinex_primitives::domain::OperationStatus;
    use sinex_primitives::views::{
        CaveatView, CoverageGapView, EventSourceView, EventTimestampView, PrivacyStateView,
        SinexObjectRef, SourcePrivacyPosture,
    };
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn ux_mk3_source_state_matrix_snapshot() -> TestResult<()> {
        let rows = [
            coverage_fixture(
                "ux.runtime.ready",
                SourceCoverageReadiness::Ready,
                SourceCoverageContinuity::Active,
                Vec::new(),
                12,
                Vec::new(),
                Vec::new(),
            ),
            coverage_fixture(
                "ux.runtime.material-only",
                SourceCoverageReadiness::Ready,
                SourceCoverageContinuity::MaterialOnly,
                Vec::new(),
                0,
                Vec::new(),
                Vec::new(),
            ),
            coverage_fixture(
                "ux.runtime.drift",
                SourceCoverageReadiness::Ready,
                SourceCoverageContinuity::Active,
                vec![caveat("parser.version_drift", "parser version drift")],
                30,
                Vec::new(),
                Vec::new(),
            ),
            coverage_fixture(
                "ux.runtime.unparsed",
                SourceCoverageReadiness::MissingEvents,
                SourceCoverageContinuity::MaterialOnly,
                vec![caveat(
                    "material.staged_unparsed",
                    "material staged but not parsed",
                )],
                0,
                vec![CoverageGapView {
                    kind: "material-only".to_string(),
                    message: "material has not produced events".to_string(),
                }],
                Vec::new(),
            ),
            coverage_fixture(
                "ux.runtime.blocked",
                SourceCoverageReadiness::MissingBinding,
                SourceCoverageContinuity::Unknown,
                vec![caveat(
                    "policy.raw_material_blocked",
                    "policy blocks raw material",
                )],
                0,
                Vec::new(),
                vec![ActionAvailability::read(
                    "sources.readiness",
                    "Readiness",
                    ActionAvailabilityState::Disabled,
                )
                .with_reason("binding unavailable")],
            ),
        ];
        let matrix = rows
            .iter()
            .map(|source| {
                serde_json::json!({
                    "fixture": source.source_id,
                    "readiness": readiness_label(source.readiness),
                    "continuity": continuity_label(source.continuity),
                    "cockpit_state": source_state_label(source_cockpit_state(source)),
                    "caveats": source.caveats.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();

        insta::assert_json_snapshot!("ux_mk3_source_state_matrix", matrix);
        Ok(())
    }

    #[sinex_test]
    async fn source_detail_renders_shared_coverage_actions() -> TestResult<()> {
        let mut terminal = Terminal::new(TestBackend::new(96, 24))?;
        let app = App {
            current_tab: Tab::Sources,
            should_quit: false,
            client: GatewayClient::new(ClientConfig {
                token: Some("fixture-token".to_string()),
                ..ClientConfig::default()
            })?,
            refresh_interval: 0,
            modules: Vec::new(),
            dlq_stats: None,
            dlq_peek: None,
            ops_operations: Vec::new(),
            replay_operations: Vec::new(),
            lifecycle_status: None,
            private_mode: None,
            source_coverage: vec![coverage_fixture(
                "ux.runtime.actions",
                SourceCoverageReadiness::Ready,
                SourceCoverageContinuity::Gapped,
                vec![caveat("parser.jobs_untracked", "parser jobs are untracked")],
                4,
                vec![CoverageGapView {
                    kind: "gapped".to_string(),
                    message: "latest material has no parsed event".to_string(),
                }],
                vec![
                    ActionAvailability::read(
                        "sources.readiness",
                        "Readiness",
                        ActionAvailabilityState::Enabled,
                    )
                    .with_command_hint("sinexctl sources readiness ux.runtime.actions"),
                    ActionAvailability::read(
                        "sources.continuity",
                        "Continuity",
                        ActionAvailabilityState::Target,
                    )
                    .with_rpc_method("sources.continuity"),
                ],
            )],
            recent_events: Vec::new(),
            recent_event_rows: Vec::new(),
            gateway_version: "fixture".to_string(),
            loading: false,
            last_refresh: Instant::now(),
            error: None,
            selected_index: 0,
            show_help: false,
            copy_menu_open: false,
            copy_index: 0,
            payload_raw: false,
            feedback: None,
        };

        terminal.draw(|f| render_source_detail(f, f.area(), &app))?;

        let rendered = buffer_to_text(terminal.backend().buffer());
        assert!(rendered.contains("Readiness [enabled]"));
        assert!(rendered.contains("sinexctl sources readiness ux.runtime.actions"));
        assert!(rendered.contains("Continuity [target] sources.continuity"));
        assert!(rendered.contains("latest material has no parsed event"));
        Ok(())
    }

    #[sinex_test]
    async fn ux_mk3_event_card_view_dto_snapshot() -> TestResult<()> {
        let cards = vec![
            event_card_fixture(
                "ux.event.full_provenance",
                PrivacyStateKind::RawVisible,
                vec![
                    SinexObjectRef::new(SinexObjectKind::MaterialAnchor, "material:fixture:42")
                        .with_label("fixture.csv:42"),
                ],
                Vec::new(),
            ),
            event_card_fixture(
                "ux.event.redacted",
                PrivacyStateKind::Redacted,
                vec![
                    SinexObjectRef::new(SinexObjectKind::MaterialAnchor, "material:fixture:secret")
                        .with_label("redacted fixture"),
                ],
                vec![CaveatView {
                    id: "privacy.redacted".to_string(),
                    message: "payload field redacted by fixture policy".to_string(),
                    ref_: None,
                }],
            ),
            event_card_fixture(
                "ux.event.missing_material_anchor",
                PrivacyStateKind::MetadataOnly,
                Vec::new(),
                vec![CaveatView {
                    id: "event.missing_material_anchor".to_string(),
                    message: "event has no material anchor reference".to_string(),
                    ref_: None,
                }],
            ),
        ];

        insta::assert_json_snapshot!("ux_mk3_event_card_view_dtos", cards);
        Ok(())
    }

    #[sinex_test]
    async fn ux_mk3_operations_room_terminal_grid_snapshot() -> TestResult<()> {
        let card = OperationRoomCard {
            title: "operation ux.operation.failed/audited".to_string(),
            authority: "admin".to_string(),
            phase: "failed".to_string(),
            progress: "42 / 100 events, batch 3".to_string(),
            affected_refs: vec![
                "source: fixture.replay".to_string(),
                "source-material: material-fixture".to_string(),
            ],
            caveats: vec![
                "mutating replay phase: confirmation/audit trail required".to_string(),
                "error: fixture replay failed after preview".to_string(),
            ],
            actions: vec![
                OperationRoomAction::new(
                    "status",
                    ActionAvailabilityState::Enabled,
                    "sinexctl ops replay status op-fixture",
                ),
                OperationRoomAction::new(
                    "execute",
                    ActionAvailabilityState::Dangerous,
                    "sinexctl ops replay execute op-fixture",
                ),
                OperationRoomAction::new(
                    "context pack",
                    ActionAvailabilityState::Target,
                    "not implemented: context pack",
                ),
            ],
            audit_refs: vec!["sinexctl ops audit op-fixture".to_string()],
        };
        let mut terminal = Terminal::new(TestBackend::new(84, 22))?;
        terminal.draw(|f| render_operation_card_detail(f, f.area(), &card))?;

        insta::assert_snapshot!(
            "ux_mk3_operations_room_terminal_grid",
            buffer_to_text(terminal.backend().buffer())
        );
        Ok(())
    }

    #[sinex_test]
    async fn operation_room_ops_card_uses_shared_operation_actions() -> TestResult<()> {
        let operation = OpsOperation {
            id: "op-fixture".to_string(),
            operation_type: "replay".to_string(),
            operator: "operator.local".to_string(),
            scope: Some(serde_json::json!({"source": "fixture"})),
            result_status: OperationStatus::Failed,
            result_message: Some("done".to_string()),
            preview_summary: Some(serde_json::json!({"events": 12})),
            duration_ms: Some(42),
        };

        let card = ops_operation_card(&operation);
        let actions = card
            .actions
            .iter()
            .map(|action| (action.label.as_str(), action.state, action.command.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(card.title, "operation op-fixture (replay)");
        assert!(actions.contains(&(
            "show",
            ActionAvailabilityState::Enabled,
            "sinexctl ops get op-fixture",
        )));
        assert!(actions.contains(&(
            "cancel",
            ActionAvailabilityState::Disabled,
            "sinexctl ops cancel op-fixture",
        )));
        assert!(actions.contains(&(
            "replay",
            ActionAvailabilityState::Dangerous,
            "sinexctl ops replay submit --ref-op op-fixture",
        )));
        Ok(())
    }

    fn coverage_fixture(
        id: &str,
        readiness: SourceCoverageReadiness,
        continuity: SourceCoverageContinuity,
        caveats: Vec<CaveatView>,
        event_count: i64,
        gaps: Vec<CoverageGapView>,
        actions: Vec<ActionAvailability>,
    ) -> SourceCoverageView {
        SourceCoverageView {
            source_id: id.to_string(),
            namespace: "ux-mk3".to_string(),
            event_types: vec!["ux-mk3/event.fixture".to_string()],
            readiness,
            continuity,
            last_material_at: Some(Timestamp::UNIX_EPOCH),
            last_event_at: Some(Timestamp::UNIX_EPOCH),
            material_count: 1,
            event_count,
            binding_count: 1,
            live_binding_count: 1,
            proposed_binding_count: 0,
            gaps,
            caveats,
            privacy: SourcePrivacyPosture {
                tier: "sensitive".to_string(),
                context: "metadata".to_string(),
                proposed: false,
            },
            actions,
        }
    }

    fn caveat(code: &str, message: &str) -> CaveatView {
        CaveatView {
            id: code.to_string(),
            message: message.to_string(),
            ref_: Some(SinexObjectRef::new(SinexObjectKind::Caveat, code)),
        }
    }

    fn event_card_fixture(
        id: &str,
        privacy: PrivacyStateKind,
        material_refs: Vec<SinexObjectRef>,
        caveats: Vec<CaveatView>,
    ) -> EventCardView {
        EventCardView {
            ref_: SinexObjectRef::new(SinexObjectKind::Event, id),
            timestamp: EventTimestampView {
                original: Some(Timestamp::UNIX_EPOCH),
                ingested: Some(Timestamp::UNIX_EPOCH),
                quality: "fixture".to_string(),
            },
            source: EventSourceView {
                family: "ux-mk3".to_string(),
                raw: "fixture.source".to_string(),
                source_ref: Some(SinexObjectRef::new(
                    SinexObjectKind::SourceDriver,
                    "ux.fixture-source",
                )),
            },
            event_type: "ux.fixture".to_string(),
            summary: id.to_string(),
            payload_preview: Some(serde_json::json!({
                "fixture": id,
                "stable": true
            })),
            material_refs,
            privacy_state: PrivacyStateView {
                state: privacy,
                reason: Some("ux fixture".to_string()),
            },
            caveats,
            trace_refs: vec![SinexObjectRef::new(
                SinexObjectKind::ReplayRun,
                "replay-fixture",
            )],
            projection_badges: vec!["ux-mk3".to_string()],
            actions: vec![
                ActionAvailability::read("trace", "Trace", ActionAvailabilityState::Enabled)
                    .with_command_hint(format!("sinexctl events trace {id}")),
                ActionAvailability {
                    id: "redact".to_string(),
                    label: "Redact".to_string(),
                    state: ActionAvailabilityState::Target,
                    reason: Some("target-only fixture".to_string()),
                    command_hint: None,
                    rpc_method: None,
                    side_effect: ActionSideEffect::Destructive,
                    requires_confirmation: true,
                    dry_run_available: true,
                    audit_output_ref: None,
                },
            ],
        }
    }

    fn buffer_to_text(buffer: &ratatui::buffer::Buffer) -> String {
        let width = usize::from(buffer.area.width);
        buffer
            .content()
            .chunks(width)
            .map(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
