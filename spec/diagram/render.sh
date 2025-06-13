#!/usr/bin/env bash
# Sinex Diagram Rendering Script
# Renders Mermaid and Graphviz diagrams to multiple formats

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🎨 Rendering Sinex Architecture Diagrams..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check dependencies
check_deps() {
    local missing=()
    
    if ! command -v dot >/dev/null 2>&1; then
        missing+=("graphviz")
    fi
    
    if ! command -v mmdc >/dev/null 2>&1; then
        echo -e "${YELLOW}Warning: mermaid-cli not found. Install with: npm install -g @mermaid-js/mermaid-cli${NC}"
        echo -e "${YELLOW}Skipping Mermaid diagram rendering...${NC}"
    fi
    
    if [ ${#missing[@]} -ne 0 ]; then
        echo -e "${RED}Missing dependencies: ${missing[*]}${NC}"
        echo -e "${BLUE}Install with: nix shell nixpkgs#graphviz${NC}"
        exit 1
    fi
}

# Render Graphviz diagrams
render_graphviz() {
    echo -e "${BLUE}Rendering Graphviz diagrams...${NC}"
    
    for dot_file in *.dot; do
        if [ -f "$dot_file" ]; then
            base=$(basename "$dot_file" .dot)
            echo "  📊 $dot_file"
            
            # SVG (primary format)
            dot -Tsvg "$dot_file" -o "${base}.svg"
            
            # PNG (high DPI)
            dot -Tpng -Gdpi=300 "$dot_file" -o "${base}.png"
            
            # PDF for documents
            dot -Tpdf "$dot_file" -o "${base}.pdf"
        fi
    done
}

# Render Mermaid diagrams
render_mermaid() {
    if ! command -v mmdc >/dev/null 2>&1; then
        return 0
    fi
    
    echo -e "${BLUE}Rendering Mermaid diagrams...${NC}"
    
    for mmd_file in *.mmd; do
        if [ -f "$mmd_file" ]; then
            base=$(basename "$mmd_file" .mmd)
            echo "  🧜 $mmd_file"
            
            # SVG (primary format)
            mmdc -i "$mmd_file" -o "${base}.svg" -b white
            
            # PNG (high DPI)
            mmdc -i "$mmd_file" -o "${base}.png" -b white -s 2
            
            # PDF
            mmdc -i "$mmd_file" -o "${base}.pdf" -b white
        fi
    done
}

# Generate responsive SVGs for web
make_responsive() {
    echo -e "${BLUE}Creating responsive SVGs...${NC}"
    
    for svg_file in *.svg; do
        if [ -f "$svg_file" ]; then
            base=$(basename "$svg_file" .svg)
            # Remove fixed width/height for responsive sizing
            sed 's/width="[^"]*"//g; s/height="[^"]*"//g' "$svg_file" > "${base}_responsive.svg"
        fi
    done
}

# Clean up old outputs
clean() {
    echo -e "${YELLOW}Cleaning old outputs...${NC}"
    rm -f *.svg *.png *.pdf *_responsive.svg
}

# Main execution
main() {
    echo "Working directory: $SCRIPT_DIR"
    
    case "${1:-all}" in
        clean)
            clean
            ;;
        check)
            check_deps
            echo -e "${GREEN}All dependencies available!${NC}"
            ;;
        graphviz|dot)
            check_deps
            render_graphviz
            ;;
        mermaid|mmd)
            render_mermaid
            ;;
        responsive)
            make_responsive
            ;;
        all)
            check_deps
            clean
            render_graphviz
            render_mermaid
            make_responsive
            echo -e "${GREEN}✅ All diagrams rendered successfully!${NC}"
            ;;
        *)
            echo "Usage: $0 [all|clean|check|graphviz|mermaid|responsive]"
            echo "  all        - Render all diagrams (default)"
            echo "  clean      - Remove generated files"
            echo "  check      - Check dependencies"
            echo "  graphviz   - Render only Graphviz (.dot) files"
            echo "  mermaid    - Render only Mermaid (.mmd) files" 
            echo "  responsive - Generate responsive SVGs"
            exit 1
            ;;
    esac
}

main "$@"