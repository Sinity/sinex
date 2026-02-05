#!/usr/bin/env python3
import subprocess
import json
import sys
import os
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Set

# Configuration
BINARY_PATH = ".sinex/target/debug/xtask"

# Regex patterns for commands that are safe to execute fully
SAFE_EXECUTION_PATTERNS = [
    r"^deps.*",
    r"^history (list|last|stats|tests|diagnostics)",
    r"^jobs (list|active|status|output)",
    r"^db status",
    r"^db schema info",
    r"^infra status",
    r"^infra env",
    r"^contracts info",
    r"^check$",  # Basic check is safe
    r"^run list",
    r"^status",
    r"^docs build",
    r"^xtr patterns", # Safe read-only
    r"^xtr completions", # Safe
    r"^xtr tls check", # Safe read-only
]

# Commands to strictly avoid running even if they match a pattern (just in case)
BLOCKLIST_PATTERNS = [
    r".*reset.*",
    r".*prune.*",
    r".*stop.*",
    r".*start.*", # Interactive or state changing
    r".*watch.*",
]

GLOBAL_FLAGS = ["--json", "--format human", "--format silent"]

class XtaskRunner:
    def __init__(self, binary_path: str):
        self.binary_path = binary_path
        if not os.path.exists(self.binary_path):
            print(f"Binary not found at {self.binary_path}, attempting cargo run...")
            self.base_cmd = ["cargo", "run", "-q", "-p", "xtask", "--"]
        else:
            self.base_cmd = [self.binary_path]

    def log(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{timestamp}] {message}")
        sys.stdout.flush()

    def run_cmd(self, args: List[str], description: str = "") -> tuple[int, str, str]:
        full_cmd = self.base_cmd + args
        cmd_str = " ".join(full_cmd)
        
        print(f"\n{'='*80}")
        print(f"EXEC: {cmd_str}")
        if description:
            print(f"DESC: {description}")
        print(f"TIME: {datetime.now().isoformat()}")
        print(f"{'-'*80}")
        sys.stdout.flush()

        start_time = time.time()
        
        try:
            # Set a timeout to prevent hanging on interactive commands if logic fails
            process = subprocess.Popen(
                full_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors='replace'
            )
            # 300s timeout for most commands (docs build can be slow)
            try:
                stdout, stderr = process.communicate(timeout=300)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                print("\nTIMEOUT EXPIRED - Process killed")

            if stdout:
                print(stdout, end='')
            if stderr:
                print(stderr, file=sys.stderr, end='')
                
            duration = time.time() - start_time
            print(f"\n{'-'*80}")
            print(f"EXIT: {process.returncode} (duration: {duration:.2f}s)")
            print(f"{'='*80}\n")
            
            return process.returncode, stdout, stderr
        except Exception as e:
            self.log(f"ERROR executing {cmd_str}: {e}")
            return -1, "", str(e)

    def discover_commands(self) -> Dict[str, Any]:
        self.log("Discovering commands...")
        code, out, err = self.run_cmd(["--list-commands", "--json"], "Discovery")
        if code != 0:
            self.log("Failed to discover commands")
            sys.exit(1)
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            self.log("Failed to parse discovery JSON")
            sys.exit(1)

    def is_safe(self, cmd_path_str: str) -> bool:
        # Check blocklist first
        for pattern in BLOCKLIST_PATTERNS:
            if re.match(pattern, cmd_path_str):
                return False
        
        # Check safe list
        for pattern in SAFE_EXECUTION_PATTERNS:
            if re.match(pattern, cmd_path_str):
                return True
        return False

    def process_command(self, node: Dict[str, Any], parent_path: List[str]):
        name = node['name']
        current_path = parent_path + [name]
        cmd_path_str = " ".join(current_path)
        
        # Determine execution strategy
        should_execute = self.is_safe(cmd_path_str)
        
        if should_execute:
            self.log(f"Executing SAFE command: {cmd_path_str}")
            # Run base command
            self.run_cmd(current_path, f"Base execution: {cmd_path_str}")
            
            # Fuzz flags
            args = node.get('args', [])
            for arg in args:
                if arg.get('global'): continue # Skip global flags loop for now
                
                flag_name = f"--{arg['long']}" if arg.get('long') else f"-{arg['short']}"
                
                test_args = current_path[:]
                test_args.append(flag_name)
                
                desc = f"Flag test: {flag_name}"
                
                if arg.get('takes_value'):
                    possibles = arg.get('possible_values', [])
                    if possibles:
                        # Test with the first valid value
                        val = possibles[0]
                        test_args.append(val)
                        desc += f"={val}"
                        self.run_cmd(test_args, desc)
                    else:
                        # Unknown value required - skip execution to avoid error, 
                        # or run with --help to verify parsing?
                        # User wants "everything", but running with missing arg fails.
                        # Running with dummy arg might fail validation.
                        # Fallback to --help check for this specific flag combination?
                        test_args.append("--help")
                        self.run_cmd(test_args, f"Check parsing: {flag_name} (requires value)")
                else:
                    # Boolean flag - safe to run if command is safe
                    self.run_cmd(test_args, desc)

        else:
            # Not safe to execute fully -> just check help
            self.log(f"Checking help (unsafe/unknown): {cmd_path_str}")
            self.run_cmd(current_path + ["--help"], f"Help check: {cmd_path_str}")

        # Recurse
        for sub in node.get('subcommands', []):
            self.process_command(sub, current_path)

    def run(self):
        # 1. Global flags check (on root)
        self.log("Testing Global Flags on Root")
        for flag in GLOBAL_FLAGS:
            args = flag.split()
            self.run_cmd(args + ["--version"], f"Global flag check: {flag}")

        # 2. Discovery
        data = self.discover_commands()
        commands = data.get('commands', [])
        
        # 3. Walk and Execute
        for cmd in commands:
            self.process_command(cmd, [])

if __name__ == "__main__":
    runner = XtaskRunner(BINARY_PATH)
    runner.run()
