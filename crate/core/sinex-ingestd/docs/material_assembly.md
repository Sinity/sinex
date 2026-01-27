# Material Assembler Subsystem

The Material Assembler is responsible for reassembling large binary source materials from fragmented slices streamed via NATS JetStream.

## Architecture

The subsystem manages in-flight material assemblies with Write-Ahead Logging (WAL) for crash resilience and uses git-annex as the authoritative blob storage backend.

### State Machine

All assembly transitions are recorded in a WAL using the `WalEntry` enum:

- `Begin`: Initial metadata and material ID.
- `Slice`: Sequential byte slice successfully written to disk.
- `BufferedSlice`: Out-of-order slice buffered for later integration.
- `BufferedSliceTaken`: Buffered slice moved into the primary assembly file.
- `End`: Finalization trigger with expected content hash.

### Out-of-Order Handling

The assembler supports non-sequential delivery of byte slices:

1. **Sequential Arrival**: Slices matching the `expected_offset` are appended directly to the primary temporary file.
2. **Buffering**: Out-of-order slices are written to individual buffer files and tracked in a `BTreeMap<i64, PathBuf>`.
3. **Integration**: When a "gap-filling" slice arrives, the assembler integrated all subsequent sequential slices from the buffer map.

## Persistence & Durability

- **WAL Durability**: The WAL file is `fsync`'d after every entry to ensure state can be reconstructed after a crash.
- **Assembly Recovery**: On restart, the assembler replays the WAL to reconstruct the `AssemblerState` and resumes assembly from the last known offset.
- **Hasher Recovery**: Since hasher state is not serializable, recovery requires reading the entire assembly file to recompute the incremental hash.

## Finalization Flow

When an `End` message is received:

1. **Hash Verification**: Compares the assembled file's BLAKE3 hash against the expected hash.
2. **Git-Annex Import**: Imports the file into the local git-annex repository.
3. **Blob Registration**: Registers the resulting annex key in the database `core.blobs` table.
4. **Temporal Ledger**: Records the material-to-blob mapping in the immutable ledger.
5. **Cleanup**: Removes temporary assembly files and the associated WAL.

## Resource Management

- **Concurrency**: Per-material `Mutex` isolation via `DashMap` ensures that slow I/O for one material doesn't block other assemblies.
- **Limits**: Hardcoded limit of 50 concurrent assemblies (enforced via semaphores). Note that restored assemblies currently bypass this limit during the recovery phase.
