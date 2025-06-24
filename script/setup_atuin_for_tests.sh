#!/usr/bin/env bash
# Setup script for enabling real Atuin testing in Sinex
set -euo pipefail

echo "Setting up Atuin for real integration testing..."

# Check if Atuin is already installed
if ! command -v atuin &> /dev/null; then
    echo "Installing Atuin..."
    curl --proto '=https' --tlsv1.2 -LsSf https://setup.atuin.sh | sh
    
    # Source the new shell environment
    if [ -f ~/.bashrc ]; then
        source ~/.bashrc
    elif [ -f ~/.zshrc ]; then
        source ~/.zshrc
    fi
else
    echo "Atuin already installed: $(atuin --version)"
fi

# Initialize Atuin if not already done
if [ ! -f ~/.local/share/atuin/history.db ]; then
    echo "Initializing Atuin database..."
    atuin init
fi

# Check if database has sufficient test data
history_count=$(atuin search --limit 1000 | wc -l || echo "0")
echo "Current Atuin history entries: $history_count"

if [ "$history_count" -lt 10 ]; then
    echo "Populating Atuin with test data..."
    
    # Generate diverse test commands that Atuin will capture
    test_commands=(
        "echo 'sinex-test-basic-command'"
        "ls -la /tmp"
        "pwd"
        "date +%Y-%m-%d"
        "whoami"
        "uname -a"
        "echo 'command with spaces and special chars: !@#$%'"
        "sleep 0.1"
        "true"
        "false"
        "echo 'multi-word command with long output line that exceeds normal terminal width'"
        "cd /tmp && ls && cd -"
    )
    
    for cmd in "${test_commands[@]}"; do
        echo "Executing: $cmd"
        eval "$cmd" || true  # Continue even if command fails
        sleep 0.1  # Small delay to ensure distinct timestamps
    done
    
    echo "Test data populated. New entry count: $(atuin search --limit 1000 | wc -l)"
else
    echo "Sufficient test data already present"
fi

echo "Atuin setup complete!"
echo "Database location: ~/.local/share/atuin/history.db"
echo "Test data entries: $(atuin search --limit 1000 | wc -l)"
echo ""
echo "Run tests with: cargo test atuin -- --include-ignored"
