use clap::Args;
use color_eyre::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use std::io;

use crate::client::GatewayClient;

/// Launch interactive TUI dashboard
#[derive(Debug, Args)]
pub struct TuiCommand {
    /// Starting tab (dashboard, replay, events, dlq)
    #[arg(long, default_value = "dashboard")]
    tab: String,
}

enum Tab {
    Dashboard,
    Replay,
    Events,
    Dlq,
}

impl From<&str> for Tab {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "replay" => Tab::Replay,
            "events" => Tab::Events,
            "dlq" => Tab::Dlq,
            _ => Tab::Dashboard,
        }
    }
}

struct App {
    current_tab: usize,
    should_quit: bool,
    client: GatewayClient,
}

impl App {
    fn new(client: GatewayClient, start_tab: Tab) -> Self {
        let current_tab = match start_tab {
            Tab::Dashboard => 0,
            Tab::Replay => 1,
            Tab::Events => 2,
            Tab::Dlq => 3,
        };
        Self {
            current_tab,
            should_quit: false,
            client,
        }
    }

    fn next_tab(&mut self) {
        self.current_tab = (self.current_tab + 1) % 4;
    }

    fn previous_tab(&mut self) {
        self.current_tab = if self.current_tab > 0 {
            self.current_tab - 1
        } else {
            3
        };
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
        let mut app = App::new(client.clone(), Tab::from(self.tab.as_str()));

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
                            // Refresh data
                        }
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
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(f.area());

    // Tab bar
    let tabs = vec!["Dashboard", "Replay", "Events", "DLQ"];
    let mut tab_spans = vec![];
    for (i, t) in tabs.iter().enumerate() {
        let style = if i == app.current_tab {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };
        tab_spans.push(Span::styled(format!(" {} ", t), style));
        if i < tabs.len() - 1 {
            tab_spans.push(Span::raw(" │ "));
        }
    }

    let tabs_widget = Paragraph::new(Line::from(tab_spans))
        .block(Block::default().borders(Borders::ALL).title("Sinex CLI"));
    f.render_widget(tabs_widget, chunks[0]);

    // Content area
    match app.current_tab {
        0 => render_dashboard(f, chunks[1]),
        1 => render_replay(f, chunks[1]),
        2 => render_events(f, chunks[1]),
        3 => render_dlq(f, chunks[1]),
        _ => {}
    }
}

fn render_dashboard(f: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Dashboard")
        .borders(Borders::ALL);

    let items = vec![
        ListItem::new("System Status: Running"),
        ListItem::new("Active Nodes: 3"),
        ListItem::new("Events Today: 1,234"),
        ListItem::new("DLQ Messages: 0"),
        ListItem::new(""),
        ListItem::new("Press 'r' to refresh"),
        ListItem::new("Press Tab to switch tabs"),
        ListItem::new("Press 'q' to quit"),
    ];

    let list = List::new(items)
        .block(block)
        .style(Style::default().fg(Color::White));

    f.render_widget(list, area);
}

fn render_replay(f: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Replay Operations")
        .borders(Borders::ALL);

    let items = vec![
        ListItem::new("No active replay operations"),
        ListItem::new(""),
        ListItem::new("Use 'sinexctl replay plan' to create a replay"),
    ];

    let list = List::new(items)
        .block(block)
        .style(Style::default().fg(Color::White));

    f.render_widget(list, area);
}

fn render_events(f: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Event Stream")
        .borders(Borders::ALL);

    let paragraph = Paragraph::new("Real-time event stream coming soon...")
        .block(block)
        .style(Style::default().fg(Color::White));

    f.render_widget(paragraph, area);
}

fn render_dlq(f: &mut Frame, area: ratatui::layout::Rect) {
    let block = Block::default()
        .title("Dead Letter Queue")
        .borders(Borders::ALL);

    let items = vec![
        ListItem::new("No messages in DLQ"),
        ListItem::new(""),
        ListItem::new("Messages that fail processing appear here"),
    ];

    let list = List::new(items)
        .block(block)
        .style(Style::default().fg(Color::White));

    f.render_widget(list, area);
}
