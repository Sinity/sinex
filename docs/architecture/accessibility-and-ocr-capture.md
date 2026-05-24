# Accessibility And OCR Capture

Status: design intent. AT-SPI2 (#396) and screen-region OCR (#391) are both
documented as feasible surfaces and have not yet been implemented. This record
defines the contracts those surfaces will satisfy once enabled, so that
producers, the privacy engine, and downstream synthesis (desktop-context
automaton, document layer, semantic search) can be designed against stable
expectations.

This is a "future capture" record, not an implementation tracker. Real work
is tracked in #391 and #396; the contracts here are the receiving shape.

## What This Owns

- Whitelist-filtered accessibility-tree reads via AT-SPI2: which windows/
  elements gained focus, which documents are loaded, what page/sheet is
  active, with strictly metadata-only content.
- Triggered screen-region OCR via `grim`/`slurp`/`tesseract`: blob ingestion
  of the captured image plus a derived OCR-text source material.
- Image-clipboard OCR: a derived OCR over images placed in the clipboard.
- LLM visual description (`sinexctl desktop describe-screen`): manual,
  on-demand multimodal description of the captured image.

## What This Does Not Own

- Continuous periodic screenshots. Explicitly out of scope: cost, privacy
  cost, and redundancy with title/accessibility signals. May ship as opt-in
  power-user config later; not a default.
- The AT-SPI2 contents of editor and chat surfaces. Whitelisting excludes
  message bodies, password fields, code editors, and any editable text.
- Synthesis into the desktop-context "current_state" object. That belongs to
  the desktop-context automaton; this record only provides inputs.
- Document-layer semantics. OCR text is source material; downstream parsing,
  embedding, and chunking flow through `docs/architecture/document-layer-v1.md`.

## Accessibility Tree

The Rust `atspi` crate is the chosen integration path. Feature-gated behind
`accessibility` in `sinex-desktop-ingestor`, opt-in via the NixOS module so
the dependency is absent when disabled.

| Event | Trigger | Used for |
| --- | --- | --- |
| `object:focus` / `object:state-changed:focused` | UI element gained focus | "what document/page is active" |
| `window:activate` / `window:deactivate` | Window-level focus | First-class signal for multi-document apps |
| `document:reload` | Browser/document app reloaded | URL refresh for browsers without native messaging |
| `document:content-changed` | Document content changed | PDF page turn, sheet switch |

### Whitelist Of App Classes

| Class | Captured | Not captured |
| --- | --- | --- |
| `firefox`, `chromium`, `qutebrowser` | Document title, URL via `Accessible.name` | Page body text, form contents |
| `evince`, `zathura`, `okular` | Document title, page number | Document text |
| `libreoffice*` | Document name, page/sheet count | Cell or paragraph contents |
| `signal-desktop`, `discord` | Conversation/window title only | Message bodies, attachment text |

Apps outside the whitelist are ignored even if they expose AT-SPI events.
Adding a new whitelisted class is a config change, not a code change.

### Event Shape

```rust
#[event_payload(source = "desktop.accessibility", event_type = "context.captured")]
pub struct AccessibilityContextPayload {
    pub app_class: String,
    pub app_name: String,
    pub element_type: String,    // "document", "dialog", "menu", "tab"
    pub element_name: String,    // privacy-engine sanitized
    pub element_role: String,    // AT-SPI role string
    pub depth: u32,              // tree depth from root
}
```

Every `element_name` value passes through the privacy engine with an
accessibility-specific context before leaving the capture process. The
context belongs in the admission/policy work in #1042.

### Realism Notes

- AT-SPI requires `AT_SPI_BUS_ADDRESS` and per-toolkit accessibility
  (`GTK_*` works by default; Qt needs `QT_ACCESSIBILITY=1`; Electron needs
  `--enable-accessibility`).
- Electron and terminal apps have poor AT-SPI support and are explicitly
  out of scope. Neovim and VS Code go through their own plugin/extension
  surfaces (see `desktop-capture-substrate.md`).
- The minimal viable prototype subscribes to `window:activate` only; richer
  document-content events are an enrichment on top.

## Screen-Region OCR

OCR is on-demand, never continuous. Two trigger modes:

| Mode | Trigger | Output |
| --- | --- | --- |
| `sinexctl desktop capture-region` | Hyprland keybind invokes `grim -g "$(slurp)"` plus `tesseract` | Image blob + OCR text material, `desktop.ocr/region.captured` event |
| Image clipboard | `clipboard.copied` with `content_type == "image"` | OCR text material linked to clipboard material, `desktop.ocr/clipboard_image.captured` |

### Source Material Pattern

OCR produces two `raw.source_material_registry` rows per capture:

1. The screenshot PNG blob (anchored to local BLAKE3 CAS).
2. The OCR text, derived via synthesis provenance (`from_parents([image_id])`).

The OCR event records both ids plus tesseract confidence, region coordinates,
and language pack used. This keeps OCR fully replayable: re-running tesseract
on the same image creates a superseding derivation, not a new "screenshot".

### Privacy

OCR output can contain anything visible on screen: passwords, names, private
messages. Every OCR text body goes through the privacy engine before storage,
and image-clipboard OCR is opt-in via config. The privacy admission work in
#1042 owns the eventual policy shape; this record commits only that OCR text
is never stored unfiltered.

When `screencast.state_changed` indicates an active screen share, OCR
should be suppressed by default — capturing what an audience sees and storing
it is a separate consent decision.

### LLM Visual Description

`sinexctl desktop describe-screen` is the speculative-but-realistic upgrade
path: capture the focused window, send to a local multimodal model (LLaVA via
Ollama), store the description as a synthesis event derived from the image
blob. Explicitly manual, never wired into the continuous pipeline. Cloud
APIs are out of scope on the same privacy grounds as continuous capture.

## Realism Check

These surfaces are realistic but not free:

- AT-SPI2 prototype: small (a `window:activate` subscriber under a feature
  flag). Realistic in a single sprint.
- Region OCR via grim/slurp/tesseract: the toolchain is already on NixOS;
  wiring it to source-material registration is a moderate change.
- Continuous screenshots: explicitly rejected for a v1.
- LLM description: parking spot, depends on the embedding/LLM runtime in
  `docs/architecture/embedding-runtime.md` for Ollama plumbing.

## Open Questions

- Whether OCR text should be embedded by default once the embedding runtime
  is ready. Default expectation: yes, with the same length/embeddability
  rules as documents. See `docs/architecture/document-layer-v1.md`.
- How accessibility events compose with `desktop.context/terminal.context`
  when both surfaces fire on the same window. The desktop-context automaton
  (synthesis layer) is responsible for de-duplication.
- Whether the AT-SPI subscription should run in the desktop capture process
  or as a sibling node. Default: same process, feature-gated; revisit when
  per-source-unit topology lands (#1054).

## Boundaries

- Do not capture editable text from AT-SPI. The whitelist is element-role
  scoped, not just app-class scoped.
- Do not run OCR continuously. On-demand only.
- Do not store unfiltered OCR output. Privacy engine is mandatory.
- Do not treat OCR text as authoritative document content. It is one source
  material among many for the document layer to consume.

**Related:** `docs/architecture/desktop-capture-substrate.md`,
`docs/architecture/document-layer-v1.md`,
`docs/architecture/embedding-runtime.md`,
issues #391, #396, #1042.
