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
use sinex_primitives::rpc::sources::{
    SourceReadiness, SourceReadinessStatus, SourcesReadinessListRequest,
};
use sinex_primitives::sources::continuity::{SourceContinuityReport, SourcesContinuityListRequest};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    ActionAvailabilityState, EventCardListView, EventCardView, PrivacyStateKind, SinexObjectKind,
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
    sinexctl tui --tab nodes
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
    /// Starting tab (dashboard, nodes, sources, events, dlq)
    #[arg(long, value_enum, default_value_t = Tab::Dashboard)]
    tab: Tab,

    /// Auto-refresh interval in seconds (0 to disable)
    #[arg(long, default_value = "5")]
    refresh: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Tab {
    Dashboard,
    #[value(alias = "node")]
    Nodes,
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
    nodes: Vec<InstanceInfo>,
    dlq_stats: Option<DlqListResponse>,
    source_readiness: Vec<SourceReadiness>,
    source_continuity: Vec<SourceContinuityReport>,
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
            nodes: Vec::new(),
            dlq_stats: None,
            source_readiness: Vec::new(),
            source_continuity: Vec::new(),
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
            Tab::Dashboard => Tab::Nodes,
            Tab::Nodes => Tab::Sources,
            Tab::Sources => Tab::Events,
            Tab::Events => Tab::Dlq,
            Tab::Dlq => Tab::Dashboard,
        };
        self.switch_tab(next);
    }

    fn previous_tab(&mut self) {
        let previous = match self.current_tab {
            Tab::Dashboard => Tab::Dlq,
            Tab::Nodes => Tab::Dashboard,
            Tab::Sources => Tab::Nodes,
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
            Tab::Nodes => self.nodes.len(),
            Tab::Sources => self.source_readiness.len(),
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

        // Fetch gateway version
        match self.client.version().await {
            Ok(v) => self.gateway_version = v,
            Err(e) => {
                self.error = Some(format!("Failed to connect: {e}"));
                self.loading = false;
                return;
            }
        }

        // Fetch nodes
        match self.client.list_nodes(None).await {
            Ok(nodes) => self.nodes = nodes,
            Err(e) => {
                self.error = Some(format!("Failed to fetch nodes: {e}"));
            }
        }

        // Fetch DLQ info
        match self.client.dlq_list().await {
            Ok(stats) => self.dlq_stats = Some(stats),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch DLQ: {e}"));
                }
            }
        }

        // Fetch source readiness/cockpit data
        match self
            .client
            .sources_readiness_list(SourcesReadinessListRequest::default())
            .await
        {
            Ok(resp) => {
                self.source_readiness = resp.sources;
                self.clamp_selection();
            }
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch source readiness: {e}"));
                }
            }
        }
        match self
            .client
            .sources_continuity_list(SourcesContinuityListRequest::default())
            .await
        {
            Ok(resp) => self.source_continuity = resp.reports,
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch source continuity: {e}"));
                }
            }
        }

        // Fetch recent events (last hour, 50 events)
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

        self.loading = false;
        self.last_refresh = Instant::now();
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
                    copy_selected_action(app)?;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.select_next();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    app.select_previous();
                }
                KeyCode::Char('1') => app.switch_tab(Tab::Dashboard),
                KeyCode::Char('2') => app.switch_tab(Tab::Nodes),
                KeyCode::Char('3') => app.switch_tab(Tab::Sources),
                KeyCode::Char('4') => app.switch_tab(Tab::Events),
                KeyCode::Char('5') => app.switch_tab(Tab::Dlq),
                _ => {}
            }
        }

        if app.should_quit {
            break;
        }
    }
    Ok(())
}

fn copy_selected_action(app: &mut App) -> Result<()> {
    if !app.copy_menu_open {
        return Ok(());
    }
    let actions = app.selected_copy_actions();
    let Some(action) = actions.get(app.copy_index) else {
        app.feedback = Some("No copy action selected.".to_string());
        return Ok(());
    };
    match action.value.as_deref() {
        Some(value) => match copy_to_terminal_clipboard(value) {
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
        },
        None => {
            let reason = action
                .disabled_reason
                .as_deref()
                .unwrap_or("copy action is unavailable");
            app.feedback = Some(format!("Cannot copy {}: {reason}", action.label));
        }
    }
    Ok(())
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
        Tab::Nodes => render_nodes(f, chunks[1], app),
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
        ("2:Nodes", Tab::Nodes),
        ("3:Sources", Tab::Sources),
        ("4:Events", Tab::Events),
        ("5:DLQ", Tab::Dlq),
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
    // Consider node healthy if it has a heartbeat (we'll derive active status from heartbeat age later)
    let healthy_nodes = app
        .nodes
        .iter()
        .filter(|n| {
            n.last_heartbeat
                .is_some_and(|hb| (Timestamp::now() - hb).whole_seconds() < 60)
        })
        .count();
    let total_nodes = app.nodes.len();
    let dlq_total = app.dlq_stats.as_ref().map_or(0, |s| s.total_messages);
    let events_count = app.recent_events.len();

    let overview_items = vec![
        ListItem::new(format!("Gateway Version: {}", app.gateway_version)),
        ListItem::new(""),
        ListItem::new(format!("Healthy Nodes: {healthy_nodes}/{total_nodes}")),
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

    // Right: Node list
    let node_items: Vec<ListItem> = app
        .nodes
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
                status_icon, name, n.node_type, leader
            ))
            .style(Style::default().fg(color))
        })
        .collect();

    let nodes_list = List::new(if node_items.is_empty() {
        vec![ListItem::new("No nodes registered")]
    } else {
        node_items
    })
    .block(Block::default().title("Nodes").borders(Borders::ALL));
    f.render_widget(nodes_list, chunks[1]);
}

fn render_nodes(f: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .nodes
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
                status_icon, name, n.node_type, heartbeat_str, leader
            ))
            .style(style)
        })
        .collect();

    let list = List::new(if items.is_empty() {
        vec![ListItem::new("No nodes registered")]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!("Nodes ({} total)", app.nodes.len()))
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
        .source_readiness
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let state = source_cockpit_state(source);
            let continuity = source_continuity_for(app, &source.source_family);
            let gaps = continuity.map_or(0, |report| report.gaps.len());
            let style = if index == app.selected_index {
                Style::default()
                    .fg(source_state_color(state))
                    .add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(source_state_color(state))
            };
            let parser = source
                .parser_id
                .as_ref()
                .map_or_else(|| "-".to_string(), std::string::ToString::to_string);
            ListItem::new(format!(
                "{:<10} {:<12} {} | parser {} | mat {} evt {} | gaps {}",
                source_state_label(state),
                source.source_family,
                truncate_chars(&source.source_identifier, 34),
                parser,
                source.material_count,
                source
                    .parsed_event_count
                    .map_or_else(|| "-".to_string(), |count| count.to_string()),
                gaps
            ))
            .style(style)
        })
        .collect();

    let empty_label = if app.loading {
        "Loading source readiness..."
    } else if app.error.is_some() {
        "Source readiness unavailable; see status footer"
    } else {
        "No source readiness records"
    };
    let list = List::new(if items.is_empty() {
        vec![ListItem::new(empty_label)]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!(
                "Sources ({} readiness rows)",
                app.source_readiness.len()
            ))
            .borders(Borders::ALL),
    );
    f.render_widget(list, chunks[0]);
    render_source_detail(f, chunks[1], app);
}

fn render_source_detail(f: &mut Frame, area: Rect, app: &App) {
    let Some(source) = app.source_readiness.get(app.selected_index) else {
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
    let continuity = source_continuity_for(app, &source.source_family);
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
            Span::raw(source.source_identifier.clone()),
        ]),
        Line::from(vec![
            Span::styled("Family  ", Style::default().fg(Color::DarkGray)),
            Span::raw(source.source_family.clone()),
        ]),
        Line::from(vec![
            Span::styled("Parser  ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                source
                    .parser_id
                    .as_ref()
                    .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string),
            ),
        ]),
        Line::from(vec![
            Span::styled("Fresh  ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                source
                    .freshness_seconds
                    .map_or_else(|| "unknown".to_string(), |secs| format!("{secs}s")),
            ),
        ]),
        Line::from(vec![
            Span::styled("Counts  ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} material(s), {} parsed event(s)",
                source.material_count,
                source
                    .parsed_event_count
                    .map_or_else(|| "unknown".to_string(), |count| count.to_string())
            )),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Gap Explanation",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];

    match continuity {
        Some(report) if !report.gaps.is_empty() => {
            let gap = &report.gaps[0];
            lines.push(Line::from(format!(
                "{} gap(s), first range {} -> {}",
                report.gaps.len(),
                gap.from_ts,
                gap.to_ts
            )));
            lines.push(Line::from(format!(
                "Affected source: {}",
                report.source_family
            )));
            lines.push(Line::from(format!(
                "Likely cause: {}",
                gap.attribution.as_deref().unwrap_or("continuity gap")
            )));
            lines.push(Line::from(format!(
                "Supporting evidence: {} material(s), {} event(s)",
                report.material_count, report.event_count
            )));
            lines.push(Line::from(format!(
                "Explain command: sinexctl sources explain-gap {} --at {}",
                report.source_family.as_str(),
                gap.from_ts
            )));
        }
        Some(report) => {
            lines.push(Line::from(format!(
                "No measured continuity gaps; {} material(s), {} event(s)",
                report.material_count, report.event_count
            )));
        }
        None => {
            lines.push(Line::from(
                "No continuity report for this source family yet.",
            ));
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
            lines.push(Line::from(format!(
                "{} [{:?}] {}",
                caveat.code, caveat.severity, caveat.message
            )));
            if let Some(evidence) = &caveat.evidence_ref {
                lines.push(Line::from(format!("  evidence: {evidence}")));
            }
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Next Commands",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(format!(
        "sinexctl sources readiness {} --family {}",
        source.source_identifier, source.source_family
    )));
    lines.push(Line::from(format!(
        "sinexctl sources continuity --source {}",
        source.source_identifier
    )));
    lines.push(Line::from(format!(
        "sinexctl sources continuity {}",
        source.source_family
    )));
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
    Partial,
    Stale,
    Missing,
    Drift,
    Unparsed,
    Blocked,
    Unknown,
}

fn source_cockpit_state(source: &SourceReadiness) -> SourceCockpitState {
    if source_has_drift_caveat(source) {
        return SourceCockpitState::Drift;
    }
    if source_has_unparsed_caveat(source)
        || (source.material_count > 0 && source.parsed_event_count == Some(0))
    {
        return SourceCockpitState::Unparsed;
    }
    match source.status {
        SourceReadinessStatus::Available => SourceCockpitState::Ready,
        SourceReadinessStatus::Partial => SourceCockpitState::Partial,
        SourceReadinessStatus::Stale => SourceCockpitState::Stale,
        SourceReadinessStatus::Missing => SourceCockpitState::Missing,
        SourceReadinessStatus::Blocked | SourceReadinessStatus::Disabled => {
            SourceCockpitState::Blocked
        }
        SourceReadinessStatus::Error | SourceReadinessStatus::Unknown => {
            SourceCockpitState::Unknown
        }
    }
}

fn source_has_drift_caveat(source: &SourceReadiness) -> bool {
    source.caveats.iter().any(|caveat| {
        matches!(
            caveat.code.as_str(),
            "parser.version_drift"
                | "source.shape_changed"
                | "parser.required_field_missing"
                | "parser.field_type_changed"
        )
    })
}

fn source_has_unparsed_caveat(source: &SourceReadiness) -> bool {
    source.caveats.iter().any(|caveat| {
        matches!(
            caveat.code.as_str(),
            "material.staged_unparsed" | "parser.no_binding" | "parser.jobs_untracked"
        )
    })
}

fn source_state_label(state: SourceCockpitState) -> &'static str {
    match state {
        SourceCockpitState::Ready => "ready",
        SourceCockpitState::Partial => "partial",
        SourceCockpitState::Stale => "stale",
        SourceCockpitState::Missing => "missing",
        SourceCockpitState::Drift => "drift",
        SourceCockpitState::Unparsed => "unparsed",
        SourceCockpitState::Blocked => "blocked",
        SourceCockpitState::Unknown => "unknown",
    }
}

fn source_state_color(state: SourceCockpitState) -> Color {
    match state {
        SourceCockpitState::Ready => Color::Green,
        SourceCockpitState::Partial | SourceCockpitState::Stale => Color::Yellow,
        SourceCockpitState::Missing
        | SourceCockpitState::Drift
        | SourceCockpitState::Unparsed
        | SourceCockpitState::Blocked => Color::Red,
        SourceCockpitState::Unknown => Color::DarkGray,
    }
}

fn source_continuity_for<'a>(
    app: &'a App,
    source_family: &str,
) -> Option<&'a SourceContinuityReport> {
    app.source_continuity
        .iter()
        .find(|report| report.source_family.as_str() == source_family)
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
            format!("sinexctl trace {event_id}"),
        ));
        actions.push(EventCopyAction::available(
            "citation",
            format!("sinex:event:{event_id}"),
        ));
        actions.push(EventCopyAction::available(
            "explain command",
            format!("sinexctl explain {event_id}"),
        ));
    }

    actions.push(EventCopyAction::available(
        "reselect query",
        format!(
            "sinexctl query --source {} --event-type {} -n 20",
            card.source.raw, card.event_type
        ),
    ));
    match row {
        Some(row) => {
            let event_json = serde_json::to_string_pretty(&row.event)
                .unwrap_or_else(|error| format!("event JSON render failed: {error}"));
            actions.push(EventCopyAction::available("event JSON", event_json));
            let payload_json = serde_json::to_string_pretty(&row.event.payload)
                .unwrap_or_else(|error| format!("payload JSON render failed: {error}"));
            actions.push(EventCopyAction::available("payload JSON", payload_json));
        }
        None => {
            actions.push(EventCopyAction::disabled(
                "event JSON",
                "raw query event is unavailable",
            ));
            actions.push(EventCopyAction::disabled(
                "payload JSON",
                "raw query event is unavailable",
            ));
        }
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
        SinexObjectKind::SourceUnit => "source-unit",
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
    let height = 12;
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
                ListItem::new("Use 'sinexctl dlq peek' to inspect raw-ingest failures."),
                ListItem::new("Use 'sinexctl dlq requeue --all' to retry."),
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
