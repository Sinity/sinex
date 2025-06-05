# TIM-WebArchivingTooling: Tools and Workflow for Web Archiving

*   **Relevant ADR:** (N/A directly, but implements core PKM/archiving functionality)
*   **Original UG Context:** Section 11.1

This TIM details the recommended tools and a hybrid workflow for robust web archiving within the Exocortex, including handling authenticated and JavaScript-heavy sites. The goal is to create high-fidelity, durable archives.

## 1. Rationale Summary

No single web archiving tool excels at all scenarios. A hybrid approach leveraging multiple tools is recommended: Trafilatura for fast text extraction, SingleFile for high-fidelity single HTMLs, Browsertrix Crawler for deep dynamic WARC/WACZ archives, and ArchiveBox as a potential orchestrator or for multi-format outputs. Heritrix is for very large-scale institutional archiving, less for personal Exocortex.

## 2. Tooling Overview

### 2.1. ArchiveBox [UG Sec 11.1.1, SR1, SA1, OR3]

*   **Type:** Self-hosted, all-in-one archiver (Python).
*   **Methods:** Uses `wget`, headless Chrome (via Playwright/Puppeteer or tools like SingleFile), `yt-dlp`, readability libraries. Outputs HTML, PDF, screenshot, WARC, extracted text, etc.
*   **Authentication:**
    *   `CHROME_USER_DATA_DIR=/path/to/chrome/profile` (uses existing browser session).
    *   `COOKIES_FILE=/path/to/cookies.txt` (Netscape format).
    *   `SAVE_ARCHIVE_DOT_ORG=False` may be needed to prioritize local capture with cookies [OR3].
*   **Exocortex Use:** Can be invoked by the `WebArchivingAgent` for URLs, especially if multiple output formats (PDF, screenshot) are desired alongside a DOM snapshot or WARC. Configured as a service or CLI tool.

### 2.2. Trafilatura [UG Sec 11.1.2, SR1, SA1]

*   **Type:** Python library for main text/metadata extraction (articles, blogs, docs).
*   **Method:** `lxml` parsing, heuristics. No JS execution.
*   **Performance:** Very fast, lightweight. High accuracy for main content extraction [SR1: 25-30% better than competitors].
*   **Exocortex Use:** First-pass extraction by `WebArchivingAgent`. If successful and text-only is sufficient, may avoid heavier browser-based capture. Output is Markdown.

### 2.3. Browsertrix Crawler [UG Sec 11.1.3, SR1, SA1, CR3, SA4]

*   **Type:** High-fidelity dynamic archiver (Webrecorder project). Uses headless browser (Chromium/Brave via Puppeteer).
*   **Method:** Deep, interactive crawls. Captures JS-rendered content, network requests.
*   **Output:** WARC (Web ARChive) files, often bundled into WACZ (Web Archive Collection Zipped) format.
*   **Authentication:**
    *   Browser Profile: `browsertrix-crawler crawl --profile /path/to/profile.tar.gz ...` (created with `browsertrix-crawler profile-create`).
    *   Cookie File: `cookies.json` (Puppeteer format), potentially via `BROWSERTRIX_COOKIE` env var or `--cookie-file` CLI option.
*   **Resource Usage [SR1, SA1]:** Resource-intensive (1GB+ RAM per worker).
*   **Performance [CR3]:** 4 workers ~45-80 pages/min. Aim for 50-100MB WARCs per ~1000 pages.
*   **Exocortex Use:** Primary tool for high-fidelity, dynamic, authenticated WARC/WACZ archival. Run via Docker container managed by NixOS (see UG Sec 11.1.3, `openai_sinex_6.md` Sec 3 for NixOS Docker setup example).
    ```nix
    // Example NixOS configuration for Browsertrix Crawler Docker container
    // (Ensure Docker is enabled: virtualisation.docker.enable = true;)
    // virtualisation.oci-containers.containers."browsertrix-crawler" = {
    //   image = "ghcr.io/webrecorder/browsertrix-crawler:latest"; // Pin to a specific version tag in production
    //   ports = [ "8080:8080" ]; // If using Browsertrix Cloud UI or API, map appropriate ports
    //   volumes = [
    //     "/srv/exocortex/browsertrix/collections:/data/collections" // For crawl outputs (WACZ/WARC)
    //     "/srv/exocortex/browsertrix/url_lists:/data/url_lists"     // For seed URL files
    //     "/srv/exocortex/browsertrix/configs:/data/configs"         // For crawl config YAML files
    //     "/srv/exocortex/browsertrix/profiles:/data/profiles"       // For browser profiles or cookie files
    //   ];
    //   # cmd = [ "crawl", "--config", "/data/configs/default_crawl.yaml" ]; // Example command
    //   # Or use 'entrypoint' and 'extraArgs' depending on NixOS OCI container module
    //   # Ensure proper user/permissions for mounted volumes if needed.
    //   # For GPU acceleration with Browsertrix (if it ever supports it for e.g. video capture within crawl),
    //   # add GPU passthrough options similar to Milvus example in TIM-VectorSearchGPUAcceleration.md.
    // };
    ```

### 2.4. SingleFile [UG Sec 11.1.4, SA1]

*   **Type:** Browser extension and CLI tool. Saves page as a single, self-contained HTML file.
*   **Method:** Inlines all resources (CSS, JS, images, fonts) as data URIs or embedded elements.
*   **Fidelity & Auth:** Very high visual fidelity. Extension uses live browser session (good for authenticated pages). CLI needs headless browser and can use cookies/profiles.
*   **Exocortex Use:** Excellent for capturing single, JavaScript-heavy authenticated pages (e.g., dashboards, social media) as a self-contained HTML blob. Invoked by `WebArchivingAgent` via its CLI, configured with cookies/profile if needed.

### 2.5. Heritrix [UG Sec 11.1.5, OR3]

*   **Type:** Large-scale, institutional web crawler (Internet Archive). Java-based.
*   **Authentication:** HTTP Basic/Digest (`credentialStore` bean), HTML Form Submission config, cookies from `cookies.txt` (`CookieStore` bean).
*   **Exocortex Use:** Generally overkill for personal Exocortex. More for very large, automated archival projects. Included for completeness of options.

## 3. Hybrid Workflow for Exocortex Web Archiving Agent [UG Sec 11.1.6, SR1, SA1]

The `WebArchivingAgent` implements this logic, triggered by `sinex.web.capture_request` events.

1.  **Input:** URL, desired fidelity (`text_only`, `dom_snapshot_html`, `full_warc`), auth context.
2.  **Artifact Creation (Initial):** New `core_artifacts` entry (`artifact_type='webpage_archive'`, `status='archiving_pending'`). Log `sinex.web.archival_started`.
3.  **Step 1: Lightweight Text Extraction (Trafilatura):**
    *   Attempt to fetch URL (with auth if provided) and extract main text + metadata using Trafilatura.
    *   If successful and `fidelity='text_only'` or content is clearly article-like:
        *   Convert to Markdown. Store as `core_blobs` (annexed).
        *   Create `core_artifact_contents` entry for this Markdown. Update `core_artifacts`.
        *   Proceed to Step 5 (Finalization).
4.  **Step 2: Full Fidelity Capture (if Trafilatura insufficient or higher fidelity requested):**
    *   **If `fidelity='dom_snapshot_html'` or for single, dynamic, authenticated pages:**
        *   Use **SingleFile CLI** with appropriate cookies/profile.
        *   Output is a single HTML file. Store as primary `core_blobs` (annexed, `blob_type="html_singlefile_archive"`).
        *   Extract Markdown from this HTML using Trafilatura/Jina Reader (for `core_artifact_contents`).
    *   **If `fidelity='full_warc'` (for robust, deep, or site-wide archival):**
        *   Use **Browsertrix Crawler** (via Docker container). Configure with URL, crawl depth, behaviors, profile/cookies.
        *   Output is a WACZ/WARC file. Store as primary `core_blobs` (annexed, `blob_type="application/wacz"` or `application/warc"`).
        *   Extract Markdown from a representative page in the WARC (e.g., seed URL) using a WARC reading library + Trafilatura/Jina Reader.
    *   **ArchiveBox (Alternative Orchestrator):** Can be used to invoke some of these methods and get multiple outputs (PDF, screenshot, etc.) which can also be stored as related `core_blobs`.
5.  **Content Processing & Storage (Common for Full Fidelity):**
    *   Primary archive (HTML, WACZ/WARC) -> BLAKE3 hash -> `core_blobs` (annex key, `blob_id`).
    *   Link `core_artifacts.properties` to this `blob_id`.
    *   Extracted Markdown -> BLAKE3 hash -> `core_artifact_contents` (`content_text` or link to another `core_blobs` if Markdown itself is large). Update `core_artifacts.current_content_id`.
6.  **Metadata & Linking:**
    *   Extract title, author, pub date, etc., store in `core_artifacts.properties` or `core_artifact_contents.metadata`.
    *   Parse outgoing links from Markdown -> `core_artifact_links`.
    *   Queue Markdown `content_id` for embedding.
7.  **Finalization:**
    *   Update `core_artifacts.status` to `'archived_success'` or `'archived_failed'`.
    *   Emit `sinex.web.page_archived` event (with `artifact_id`, Markdown `content_id`, URL, annex keys, status).

This workflow prioritizes efficiency (Trafilatura first) then progressively uses more resource-intensive tools for higher fidelity and complex scenarios.

