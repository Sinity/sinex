#!/usr/bin/env bash
# VM Snapshot Management for Sinex Test Suite
# Agent Alpha - VM Infrastructure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM_DIR="${SCRIPT_DIR}/vm-images"
SNAPSHOT_DIR="${VM_DIR}/snapshots"

# Configuration
DEFAULT_VM_NAME="sinex-test-base"
BASE_CONFIG="${SCRIPT_DIR}/test-scenarios/basic-flow.nix"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')] $*${NC}" >&2
}

warn() {
    echo -e "${YELLOW}[WARN] $*${NC}" >&2
}

error() {
    echo -e "${RED}[ERROR] $*${NC}" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS] $*${NC}" >&2
}

# Create necessary directories
setup_dirs() {
    mkdir -p "$VM_DIR" "$SNAPSHOT_DIR"
}

# Build VM image with qcow2 format
build_base_vm() {
    local vm_name="${1:-$DEFAULT_VM_NAME}"
    local config_file="${2:-$BASE_CONFIG}"
    
    log "Building base VM image: $vm_name"
    
    cd "$SCRIPT_DIR"
    
    # Build the VM with qcow2 support
    nix-build \
        --out-link "${VM_DIR}/${vm_name}" \
        --arg vmProfile '"standard"' \
        "$config_file"
    
    success "Base VM built: ${VM_DIR}/${vm_name}"
}

# Create a snapshot of running VM
create_snapshot() {
    local vm_name="${1:-$DEFAULT_VM_NAME}"
    local snapshot_name="${2:-base-initialized}"
    
    if [[ ! -f "${VM_DIR}/${vm_name}.qcow2" ]]; then
        error "VM image not found: ${VM_DIR}/${vm_name}.qcow2"
        return 1
    fi
    
    log "Creating snapshot '$snapshot_name' for VM '$vm_name'"
    
    # Use qemu-img to create internal snapshot
    qemu-img snapshot -c "$snapshot_name" "${VM_DIR}/${vm_name}.qcow2"
    
    # Also create external snapshot for cloning
    qemu-img create -f qcow2 -b "${VM_DIR}/${vm_name}.qcow2" \
        "${SNAPSHOT_DIR}/${snapshot_name}.qcow2"
    
    success "Snapshot created: $snapshot_name"
    list_snapshots "$vm_name"
}

# List available snapshots
list_snapshots() {
    local vm_name="${1:-$DEFAULT_VM_NAME}"
    
    if [[ ! -f "${VM_DIR}/${vm_name}.qcow2" ]]; then
        warn "VM image not found: ${VM_DIR}/${vm_name}.qcow2"
        return 1
    fi
    
    log "Snapshots for VM '$vm_name':"
    qemu-img snapshot -l "${VM_DIR}/${vm_name}.qcow2" || echo "  No snapshots found"
    
    log "External snapshots:"
    if [[ -d "$SNAPSHOT_DIR" ]]; then
        find "$SNAPSHOT_DIR" -name "*.qcow2" -printf "  %f\n" | sort
    fi
}

# Clone VM from snapshot for parallel testing
clone_vm() {
    local base_snapshot="${1:-base-initialized}"
    local clone_name="${2:-test-clone-$$}"
    local vm_profile="${3:-standard}"
    
    log "Cloning VM from snapshot '$base_snapshot' -> '$clone_name'"
    
    if [[ ! -f "${SNAPSHOT_DIR}/${base_snapshot}.qcow2" ]]; then
        error "Snapshot not found: ${SNAPSHOT_DIR}/${base_snapshot}.qcow2"
        return 1
    fi
    
    # Create copy-on-write clone
    qemu-img create -f qcow2 -b "${SNAPSHOT_DIR}/${base_snapshot}.qcow2" \
        "${VM_DIR}/${clone_name}.qcow2"
    
    success "VM cloned: ${clone_name}.qcow2"
    echo "$clone_name"
}

# Run VM from snapshot (for testing)
run_vm_from_snapshot() {
    local snapshot_name="${1:-base-initialized}"
    local vm_profile="${2:-standard}"
    
    log "Running VM from snapshot: $snapshot_name"
    
    if [[ ! -f "${SNAPSHOT_DIR}/${snapshot_name}.qcow2" ]]; then
        error "Snapshot not found: ${SNAPSHOT_DIR}/${snapshot_name}.qcow2"
        return 1
    fi
    
    # Create temporary clone for this run
    local temp_clone="temp-run-$$"
    clone_vm "$snapshot_name" "$temp_clone" "$vm_profile"
    
    # Run the VM (this would be integrated with test runner)
    log "VM ready to run with image: ${VM_DIR}/${temp_clone}.qcow2"
    
    # Cleanup function
    cleanup_temp_vm() {
        log "Cleaning up temporary VM: $temp_clone"
        rm -f "${VM_DIR}/${temp_clone}.qcow2"
    }
    
    trap cleanup_temp_vm EXIT
    
    # In real use, this would be called by the test runner
    echo "VM image path: ${VM_DIR}/${temp_clone}.qcow2"
}

# Initialize base VM and create first snapshot
init_base_vm() {
    local vm_name="${1:-$DEFAULT_VM_NAME}"
    
    log "Initializing base VM with snapshots..."
    
    setup_dirs
    build_base_vm "$vm_name"
    
    # TODO: Boot VM, wait for services, create snapshot
    # For now, create placeholder snapshot
    log "Creating base snapshot (placeholder - will boot VM in future)"
    
    # Convert raw image to qcow2 if needed
    if [[ -f "${VM_DIR}/${vm_name}/nixos.qcow2" ]]; then
        cp "${VM_DIR}/${vm_name}/nixos.qcow2" "${VM_DIR}/${vm_name}.qcow2"
    elif [[ -f "${VM_DIR}/${vm_name}/disk-image.qcow2" ]]; then
        cp "${VM_DIR}/${vm_name}/disk-image.qcow2" "${VM_DIR}/${vm_name}.qcow2"
    else
        # Create base qcow2 image
        qemu-img create -f qcow2 "${VM_DIR}/${vm_name}.qcow2" 4G
    fi
    
    create_snapshot "$vm_name" "base-initialized"
    
    success "Base VM initialization complete"
}

# Clean up old VMs and snapshots
cleanup() {
    log "Cleaning up VM artifacts..."
    
    # Remove temporary clones
    find "$VM_DIR" -name "temp-*.qcow2" -delete 2>/dev/null || true
    find "$VM_DIR" -name "test-clone-*.qcow2" -delete 2>/dev/null || true
    
    # Remove old result symlinks
    find "$SCRIPT_DIR" -name "result*" -type l -delete 2>/dev/null || true
    
    success "Cleanup complete"
}

# Show usage
usage() {
    cat << EOF
VM Snapshot Manager for Sinex Tests

Usage: $0 <command> [options]

Commands:
    init [vm-name]              Initialize base VM and create snapshot
    build [vm-name] [config]    Build base VM image
    snapshot <vm> <name>        Create snapshot of VM
    list [vm-name]              List snapshots for VM
    clone <snapshot> [name]     Clone VM from snapshot
    run <snapshot> [profile]    Run VM from snapshot
    cleanup                     Clean up temporary files

Examples:
    $0 init                                    # Initialize base VM
    $0 snapshot sinex-test-base ready-state   # Create named snapshot  
    $0 clone ready-state my-test-vm           # Clone for testing
    $0 list                                    # Show all snapshots
    $0 cleanup                                 # Clean up temp files

VM Profiles: minimal, standard, performance, large
EOF
}

# Main command dispatch
main() {
    case "${1:-help}" in
        init)
            init_base_vm "${2:-}"
            ;;
        build)
            build_base_vm "${2:-}" "${3:-}"
            ;;
        snapshot)
            if [[ $# -lt 3 ]]; then
                error "Usage: $0 snapshot <vm-name> <snapshot-name>"
                exit 1
            fi
            create_snapshot "$2" "$3"
            ;;
        list)
            list_snapshots "${2:-}"
            ;;
        clone)
            if [[ $# -lt 2 ]]; then
                error "Usage: $0 clone <snapshot-name> [clone-name] [vm-profile]"
                exit 1
            fi
            clone_vm "$2" "${3:-}" "${4:-}"
            ;;
        run)
            if [[ $# -lt 2 ]]; then
                error "Usage: $0 run <snapshot-name> [vm-profile]"
                exit 1
            fi
            run_vm_from_snapshot "$2" "${3:-}"
            ;;
        cleanup)
            cleanup
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            error "Unknown command: $1"
            usage
            exit 1
            ;;
    esac
}

# Check dependencies
check_deps() {
    local missing=()
    
    for cmd in qemu-img nix-build; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing dependencies: ${missing[*]}"
        exit 1
    fi
}

# Script entry point
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    check_deps
    main "$@"
fi