# Sinex Architecture Diagrams

This directory contains comprehensive visual documentation of the Sinex system architecture, implementation status, and design patterns.

## 📊 Available Diagrams

### System Architecture
- **`system_architecture.mmd`** - Complete system overview showing all components and their implementation status
- **`crate_dependencies.dot`** - Rust crate structure and dependency relationships

### Data Flow & Processing  
- **`event_flow.mmd`** - Sequence diagram showing how events flow through the system
- **`data_lifecycle.mmd`** - State diagram showing event processing states and transitions
- **`database_schema.dot`** - Database tables, relationships, and constraints

### Implementation Status
- **`implementation_roadmap.mmd`** - Gantt chart showing completed, current, and planned development phases

## 🎨 Rendering Diagrams

Use the provided script to render all diagrams:

```bash
# Render all diagrams to multiple formats
./render.sh

# Check dependencies  
./render.sh check

# Render only specific types
./render.sh graphviz  # .dot files only
./render.sh mermaid   # .mmd files only

# Clean up generated files
./render.sh clean
```

### Output Formats

Each diagram is rendered to:
- **SVG** - Primary format, scalable and web-friendly
- **PNG** - High-resolution (300 DPI) for documents  
- **PDF** - Print-ready format
- **Responsive SVG** - No fixed dimensions for web embedding

### Dependencies

- **Graphviz** - For .dot file rendering (`nix shell nixpkgs#graphviz`)
- **Mermaid CLI** - For .mmd file rendering (`npm install -g @mermaid-js/mermaid-cli`)

## 📁 File Organization

### Mermaid Files (.mmd)
```
system_architecture.mmd    - System overview with implementation status
event_flow.mmd            - Event processing sequence
data_lifecycle.mmd        - Event state transitions  
implementation_roadmap.mmd - Development timeline
```

### Graphviz Files (.dot)
```
database_schema.dot       - Database structure and relationships
crate_dependencies.dot    - Rust crate dependency graph
```

## 🎯 Diagram Usage

### In Documentation
Embed diagrams in markdown:
```markdown
![System Architecture](diagram/system_architecture.svg)
```

### In Presentations
Use PNG or PDF formats for slides and documents.

### In Web Interfaces
Use responsive SVG files for dynamic web content.

## 🔄 Updating Diagrams

When the architecture changes:

1. **Update source files** (.mmd, .dot) to reflect changes
2. **Re-render** using `./render.sh`  
3. **Update implementation status** using color coding:
   - 🟢 Green: Implemented and working
   - 🟡 Yellow: Partially implemented  
   - 🔴 Red: Planned but not implemented
4. **Commit changes** including both source and rendered files

## 🎨 Color Coding Standards

### Implementation Status
- **#90EE90** (Light Green) - ✅ Fully implemented
- **#FFD700** (Gold) - 🟡 Partially implemented  
- **#FFB6C1** (Light Pink) - ❌ Not implemented
- **#87CEEB** (Sky Blue) - Database/infrastructure

### Component Types
- **Solid lines** - Active data flow
- **Dashed lines** - Planned/future connections
- **Bold borders** - Core system components
- **Thin borders** - Supporting components

## 📚 Related Documentation

- **System Architecture**: `../STAD.md` - Detailed technical architecture
- **Implementation Status**: `../docs/claude/TIM_IMPLEMENTATION_STATUS_ANALYSIS.md`
- **Database Design**: `../../migration/` - SQL schema definitions
- **Vision Document**: `../VISION.md` - High-level goals and philosophy