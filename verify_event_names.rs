
use sinex_events_terminal::*;

fn main() {
    println\!("Event name verification:");
    println\!("Atuin: {}", CommandExecutedAtuin::EVENT_NAME);
    println\!("Shell History: {}", ShellHistoryCommand::EVENT_NAME);  
    println\!("Kitty Started: {}", KittyCommandExecuted::EVENT_NAME);
    println\!("Kitty Completed: {}", KittyCommandCompleted::EVENT_NAME);
    println\!("Kitty Scrollback: {}", KittyScrollbackIncremental::EVENT_NAME);
}

