# Terminal Capture Improvements

## Current Implementation Analysis

### Scrollback Storage
- Currently stores `scrollback_text` directly in the database (in event payload)
- Default max is 10,000 lines per capture
- With average line length of ~80 chars, that's ~800KB per capture
- Text compresses well in PostgreSQL (TOAST compression)
- File storage is optional (`save_to_files` config option)

### Missing Features
1. **No exit hooks** - scrollback is lost when terminal closes
2. **No clear command handling** - scrollback is lost when user clears
3. **No incremental capture** - always captures full scrollback

## Proposed Improvements

### 1. Database-First Storage (Your Suggestion)
```toml
[event.terminal_scrollback]
save_to_files = false  # Disable file storage
max_scrollback_lines = 10000
# Store directly in database - text compresses well
```

### 2. Exit Hook Implementation
We need to capture scrollback before terminal closes:

#### Option A: Shell Exit Trap
```bash
# In .zshrc/.bashrc
trap 'kitty @ get-text --match id:$KITTY_WINDOW_ID | curl -X POST http://localhost:9001/capture' EXIT
```

#### Option B: Monitor Terminal Close Events
- Use window manager events (already captured)
- When `window.closed` event has kitty class, trigger immediate scrollback capture
- Race condition: terminal might close before capture completes

#### Option C: Kitty Hook
```conf
# In kitty.conf
close_on_child_death no  # Keep window open briefly
# Use kitty's remote control to capture before close
```

### 3. Clear Command Handling
When user runs `clear`:
- Shell command capture (via Atuin) detects `clear` command
- Trigger immediate scrollback capture before it's cleared
- Mark capture with `pre_clear: true` metadata

### 4. Incremental Capture
Instead of capturing full scrollback every time:
- Track last captured line number per window
- Only capture new lines since last capture
- Store as incremental events with references to previous captures
- Reconstruct full scrollback by following chain

## Implementation Plan

### Phase 1: Database-Only Storage
- Change default config to `save_to_files = false`
- Document that scrollback is stored in database
- Add compression stats to monitor storage impact

### Phase 2: Exit Hooks
- Implement window manager integration
- When `window.closed` with kitty class → immediate capture attempt
- Add shell exit trap as backup method

### Phase 3: Clear Detection
- Monitor for `clear` command in shell history
- Trigger immediate pre-clear capture
- Add metadata to distinguish pre-clear captures

### Phase 4: Incremental Capture
- Track capture positions per window
- Implement incremental capture logic
- Add event linking for reconstruction

## Storage Considerations

### Current Approach (Full Text in DB)
- **Pros**: 
  - Simple queries
  - Full-text search capability
  - PostgreSQL handles compression
  - No external dependencies
- **Cons**:
  - Large payloads (but compressed)
  - Duplicated content between captures

### Alternative: Content-Addressed Storage
- Store unique scrollback content in separate table
- Events reference content by hash
- Automatic deduplication
- Similar to git-annex but for text

### Recommendation
Stay with database storage for now because:
1. Text compresses extremely well (often 10:1 or better)
2. 10K lines ≈ 800KB uncompressed → ~80KB compressed
3. Full-text search is valuable
4. Simpler architecture
5. Can migrate to content-addressed later if needed