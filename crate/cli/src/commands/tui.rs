use clap::Args;
use color_eyre::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use sinex_primitives::temporal::Timestamp;
use std::io;
use std::time::Instant;
use time::Duration;

use crate::client::GatewayClient;
use crate::fmt::format_heartbeat_age;
use crate::model::search::{SearchQuery, SearchResult};
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
    /// Starting tab (dashboard, replay, events, dlq)
    #[arg(long, default_value = "dashboard")]
    tab: String,

    /// Auto-refresh interval in seconds (0 to disable)
    #[arg(long, default_value = "5")]
    refresh: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Dashboard,
    Nodes,
    Events,
    Dlq,
}

impl From<&str> for Tab {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "nodes" | "node" => Tab::Nodes,
            "events" | "event" => Tab::Events,
            "dlq" => Tab::Dlq,
            _ => Tab::Dashboard,
        }
    }
}

struct App {
    current_tab: Tab,
    should_quit: bool,
    client: GatewayClient,
    refresh_interval: u64,

    // Live data
    nodes: Vec<InstanceInfo>,
    dlq_stats: Option<DlqListResponse>,
    recent_events: Vec<SearchResult>,
    gateway_version: String,

    // State
    loading: bool,
    last_refresh: Instant,
    error: Option<String>,
    selected_index: usize,
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
            recent_events: Vec::new(),
            gateway_version: String::from("unknown"),
            loading: false,
            last_refresh: Instant::now()
                .checked_sub(std::time::Duration::from_secs(refresh_interval + 1))
                .unwrap_or(Instant::now()),
            error: None,
            selected_index: 0,
        }
    }

    fn next_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::Dashboard => Tab::Nodes,
            Tab::Nodes => Tab::Events,
            Tab::Events => Tab::Dlq,
            Tab::Dlq => Tab::Dashboard,
        };
        self.selected_index = 0;
    }

    fn previous_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::Dashboard => Tab::Dlq,
            Tab::Nodes => Tab::Dashboard,
            Tab::Events => Tab::Nodes,
            Tab::Dlq => Tab::Events,
        };
        self.selected_index = 0;
    }

    fn select_next(&mut self) {
        let max_index = self.current_list_len().saturating_sub(1);
        if self.selected_index < max_index {
            self.selected_index += 1;
        }
    }

    fn select_previous(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    fn current_list_len(&self) -> usize {
        match self.current_tab {
            Tab::Dashboard => 0,
            Tab::Nodes => self.nodes.len(),
            Tab::Events => self.recent_events.len(),
            Tab::Dlq => 0, // DLQ shows stats, not a navigable list
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

        // Fetch recent events (last hour, 50 events)
        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: Some(Timestamp::now() - Duration::hours(1)),
            end_time: None,
            limit: 50,
            offset: 0,
        };
        match self.client.search_events(query).await {
            Ok(events) => self.recent_events = events,
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(format!("Failed to fetch events: {e}"));
                }
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
        let mut app = App::new(client.clone(), Tab::from(self.tab.as_str()), self.refresh);

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
) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, app))?;

        // Check for auto-refresh
        if app.should_auto_refresh() {
            app.refresh().await;
        }

        // Poll for events with short timeout for responsive UI
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            app.should_quit = true;
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
                        KeyCode::Down | KeyCode::Char('j') => {
                            app.select_next();
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            app.select_previous();
                        }
                        KeyCode::Char('1') => app.current_tab = Tab::Dashboard,
                        KeyCode::Char('2') => app.current_tab = Tab::Nodes,
                        KeyCode::Char('3') => app.current_tab = Tab::Events,
                        KeyCode::Char('4') => app.current_tab = Tab::Dlq,
                        _ => {}
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }
    }
    Ok(())
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
        Tab::Events => render_events(f, chunks[1], app),
        Tab::Dlq => render_dlq(f, chunks[1], app),
    }

    // Status bar
    render_status_bar(f, chunks[2], app);
}

fn render_tabs(f: &mut Frame, area: Rect, app: &App) {
    let tabs = [
        ("1:Dashboard", Tab::Dashboard),
        ("2:Nodes", Tab::Nodes),
        ("3:Events", Tab::Events),
        ("4:DLQ", Tab::Dlq),
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

    let status_text = if let Some(err) = &app.error {
        format!("Error: {err} | Press 'r' to retry")
    } else {
        format!(
            "Gateway v{} | {} | ↑↓/jk:navigate Tab/←→:switch r:refresh q:quit",
            app.gateway_version, refresh_info
        )
    };

    let style = if app.error.is_some() {
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
        .filter(|n| n.last_heartbeat.is_some())
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
            let has_heartbeat = n.last_heartbeat.is_some();
            let status_icon = if has_heartbeat { "●" } else { "○" };
            let color = if has_heartbeat {
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
            let has_heartbeat = n.last_heartbeat.is_some();
            let status_icon = if has_heartbeat { "●" } else { "○" };
            let color = if has_heartbeat {
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

fn render_events(f: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .recent_events
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let style = if i == app.selected_index {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            let timestamp = e
                .timestamp
                .format(time::macros::format_description!(
                    "[hour]:[minute]:[second]"
                ))
                .unwrap_or_else(|_| "invalid".to_string());
            let snippet = if e.snippet.len() > 60 {
                format!("{}...", &e.snippet[..57])
            } else {
                e.snippet.clone()
            };
            ListItem::new(format!(
                "{} [{}] {} - {}",
                timestamp, e.source, e.event_type, snippet
            ))
            .style(style)
        })
        .collect();

    let list = List::new(if items.is_empty() {
        vec![ListItem::new("No recent events")]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!(
                "Recent Events ({} in last hour)",
                app.recent_events.len()
            ))
            .borders(Borders::ALL),
    );
    f.render_widget(list, area);
}

fn render_dlq(f: &mut Frame, area: Rect, app: &App) {
    let (block_title, items) = match &app.dlq_stats {
        Some(stats) if stats.total_messages > 0 => {
            let title = format!("Dead Letter Queue ({} messages) ⚠", stats.total_messages);
            let items = vec![
                ListItem::new(format!("Total Messages: {}", stats.total_messages))
                    .style(Style::default().fg(Color::Yellow)),
                ListItem::new(format!("Total Size: {} bytes", stats.total_bytes)),
                ListItem::new(format!("First Sequence: {}", stats.first_seq)),
                ListItem::new(format!("Last Sequence: {}", stats.last_seq)),
                ListItem::new(""),
                ListItem::new("Use 'sinexctl dlq peek' to inspect messages."),
                ListItem::new("Use 'sinexctl dlq requeue --all' to retry."),
            ];
            (title, items)
        }
        Some(_) => {
            let title = "Dead Letter Queue (empty) ✓".to_string();
            let items = vec![
                ListItem::new("No messages in DLQ.").style(Style::default().fg(Color::Green)),
                ListItem::new(""),
                ListItem::new("Messages that fail processing appear here."),
            ];
            (title, items)
        }
        None => {
            let title = "Dead Letter Queue".to_string();
            let items = vec![ListItem::new("Loading...")];
            (title, items)
        }
    };

    let list = List::new(items).block(Block::default().title(block_title).borders(Borders::ALL));
    f.render_widget(list, area);
}
