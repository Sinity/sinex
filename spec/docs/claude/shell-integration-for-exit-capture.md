# Shell Integration for Terminal Exit Capture

Since we cannot reliably capture scrollback when a window closes (the terminal is already gone by the time we get the event), here are shell integration options that users can add to their `.zshrc` or `.bashrc`:

## Option 1: Exit Hook (Most Reliable)

### For Zsh:
```bash
# Add to ~/.zshrc
_sinex_capture_on_exit() {
    if [[ -n "$KITTY_WINDOW_ID" ]] && command -v kitty >/dev/null 2>&1; then
        # Capture scrollback in background to not delay exit
        (
            scrollback=$(kitty @ get-text --match "id:$KITTY_WINDOW_ID" --extent all 2>/dev/null)
            if [[ -n "$scrollback" ]]; then
                # Send to Sinex API endpoint (adjust URL as needed)
                echo "$scrollback" | curl -s -X POST http://localhost:9091/api/scrollback \
                    -H "Content-Type: text/plain" \
                    -H "X-Window-ID: $KITTY_WINDOW_ID" \
                    -H "X-Trigger: shell_exit" \
                    -d @- >/dev/null 2>&1
            fi
        ) &
        # Don't wait for background job
        disown
    fi
}

# Zsh exit hook
zshexit() {
    _sinex_capture_on_exit
}
```

### For Bash:
```bash
# Add to ~/.bashrc
_sinex_capture_on_exit() {
    if [[ -n "$KITTY_WINDOW_ID" ]] && command -v kitty >/dev/null 2>&1; then
        # Capture scrollback in background to not delay exit
        (
            scrollback=$(kitty @ get-text --match "id:$KITTY_WINDOW_ID" --extent all 2>/dev/null)
            if [[ -n "$scrollback" ]]; then
                # Send to Sinex API endpoint (adjust URL as needed)
                echo "$scrollback" | curl -s -X POST http://localhost:9091/api/scrollback \
                    -H "Content-Type: text/plain" \
                    -H "X-Window-ID: $KITTY_WINDOW_ID" \
                    -H "X-Trigger: shell_exit" \
                    -d @- >/dev/null 2>&1
            fi
        ) &
    fi
}

# Bash exit trap
trap '_sinex_capture_on_exit' EXIT
```

## Option 2: Capture After Long Commands

Capture scrollback after commands that run for more than N seconds:

```bash
# For both Zsh and Bash
_sinex_command_timer_start() {
    _sinex_command_start_time=$SECONDS
}

_sinex_command_timer_stop() {
    local duration=$(($SECONDS - ${_sinex_command_start_time:-0}))
    
    # Capture if command took more than 5 seconds
    if [[ $duration -gt 5 ]] && [[ -n "$KITTY_WINDOW_ID" ]]; then
        (
            sleep 0.5  # Let output settle
            kitty @ get-text --match "id:$KITTY_WINDOW_ID" --extent all 2>/dev/null | \
                curl -s -X POST http://localhost:9091/api/scrollback \
                    -H "Content-Type: text/plain" \
                    -H "X-Window-ID: $KITTY_WINDOW_ID" \
                    -H "X-Trigger: long_command" \
                    -H "X-Duration: $duration" \
                    -d @- >/dev/null 2>&1
        ) &
        disown
    fi
}

# Zsh hooks
if [[ -n "$ZSH_VERSION" ]]; then
    preexec() { _sinex_command_timer_start }
    precmd() { _sinex_command_timer_stop }
fi

# Bash hooks
if [[ -n "$BASH_VERSION" ]]; then
    trap '_sinex_command_timer_start' DEBUG
    PROMPT_COMMAND="_sinex_command_timer_stop; $PROMPT_COMMAND"
fi
```

## Option 3: Capture Before Clear

Detect and capture before clear commands:

```bash
# Override clear command
clear() {
    if [[ -n "$KITTY_WINDOW_ID" ]]; then
        # Capture current scrollback before clearing
        (
            kitty @ get-text --match "id:$KITTY_WINDOW_ID" --extent all 2>/dev/null | \
                curl -s -X POST http://localhost:9091/api/scrollback \
                    -H "Content-Type: text/plain" \
                    -H "X-Window-ID: $KITTY_WINDOW_ID" \
                    -H "X-Trigger: pre_clear" \
                    -d @- >/dev/null 2>&1
        ) &
    fi
    # Run actual clear
    command clear "$@"
}

# Also handle Ctrl+L in Zsh
if [[ -n "$ZSH_VERSION" ]]; then
    _sinex_clear_screen() {
        clear
    }
    zle -N _sinex_clear_screen
    bindkey '^L' _sinex_clear_screen
fi
```

## Option 4: All-in-One Integration

Complete integration with all features:

```bash
# Add to ~/.zshrc or ~/.bashrc
if command -v kitty >/dev/null 2>&1; then
    # Configuration
    SINEX_API_URL="${SINEX_API_URL:-http://localhost:9091/api/scrollback}"
    SINEX_LONG_CMD_THRESHOLD="${SINEX_LONG_CMD_THRESHOLD:-5}"
    
    # Helper function to capture and send scrollback
    _sinex_capture_scrollback() {
        local trigger="$1"
        local extra_headers="$2"
        
        if [[ -n "$KITTY_WINDOW_ID" ]]; then
            (
                scrollback=$(kitty @ get-text --match "id:$KITTY_WINDOW_ID" --extent all 2>/dev/null)
                if [[ -n "$scrollback" ]]; then
                    curl_cmd=(
                        curl -s -X POST "$SINEX_API_URL"
                        -H "Content-Type: text/plain"
                        -H "X-Window-ID: $KITTY_WINDOW_ID"
                        -H "X-Trigger: $trigger"
                    )
                    
                    # Add extra headers if provided
                    if [[ -n "$extra_headers" ]]; then
                        while IFS= read -r header; do
                            curl_cmd+=(-H "$header")
                        done <<< "$extra_headers"
                    fi
                    
                    echo "$scrollback" | "${curl_cmd[@]}" -d @- >/dev/null 2>&1
                fi
            ) &
            disown
        fi
    }
    
    # Exit capture
    _sinex_capture_on_exit() {
        _sinex_capture_scrollback "shell_exit"
    }
    
    # Command timing
    _sinex_command_timer_start() {
        _sinex_command_start_time=$SECONDS
        _sinex_last_command="$1"
    }
    
    _sinex_command_timer_stop() {
        local duration=$(($SECONDS - ${_sinex_command_start_time:-0}))
        
        if [[ $duration -gt $SINEX_LONG_CMD_THRESHOLD ]]; then
            sleep 0.5  # Let output settle
            _sinex_capture_scrollback "long_command" "X-Duration: $duration
X-Command: ${_sinex_last_command:-unknown}"
        fi
    }
    
    # Clear override
    clear() {
        _sinex_capture_scrollback "pre_clear"
        command clear "$@"
    }
    
    # Shell-specific hooks
    if [[ -n "$ZSH_VERSION" ]]; then
        # Zsh hooks
        zshexit() { _sinex_capture_on_exit }
        preexec() { _sinex_command_timer_start "$1" }
        precmd() { _sinex_command_timer_stop }
        
        # Ctrl+L binding
        _sinex_clear_screen() { clear }
        zle -N _sinex_clear_screen
        bindkey '^L' _sinex_clear_screen
    elif [[ -n "$BASH_VERSION" ]]; then
        # Bash hooks
        trap '_sinex_capture_on_exit' EXIT
        trap '_sinex_command_timer_start "$BASH_COMMAND"' DEBUG
        PROMPT_COMMAND="_sinex_command_timer_stop; ${PROMPT_COMMAND:-}"
    fi
    
    echo "Sinex terminal capture integration loaded"
fi
```

## Installation

1. Choose which option(s) you want
2. Add to your shell RC file (`~/.zshrc` or `~/.bashrc`)
3. Adjust the `SINEX_API_URL` if your API endpoint is different
4. Reload your shell: `source ~/.zshrc` or `source ~/.bashrc`

## Notes

- All captures run in background to avoid slowing down your shell
- The API endpoint needs to be implemented to receive the scrollback data
- Scrollback is sent as plain text in the request body
- Headers identify the trigger type and window ID
- Exit capture is most reliable but requires shell configuration
- Clear detection works well for manual clears
- Long command capture helps preserve output from important operations