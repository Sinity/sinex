# TIM-WebArchivingCDP_DOMDiff: Authenticated Crawling with CDP & DOM Diffing

*   **Relevant ADR:** (N/A directly, advanced technique for web archiving agent)
*   **Original UG Context:** Section 11.2, 11.3

This TIM details advanced techniques for web archiving: using the Chrome DevTools Protocol (CDP) for fine-grained authenticated session capture, and DOM diffing for efficient change tracking on re-crawled pages.

## 1. Authenticated Session Capture with Chrome DevTools Protocol (CDP) [UG Sec 11.2, CR3]

For custom crawlers or tools needing deep control over a Chrome/Chromium browser instance, CDP provides programmatic access. Libraries like Puppeteer (Node.js), Playwright (multi-language), or `chrome-remote-interface` (Node.js) simplify CDP usage.

### 1.1. Cookie Management for Authentication [CR3]

*   **`Network.getCookies([urls])`:** CDP command to retrieve browser cookies (including HttpOnly) for specified URLs.
*   **`Network.setCookies(cookies)`:** CDP command to set cookies, restoring a logged-in session. Cookies typically provided in Puppeteer/Playwright JSON format.
*   **Process for Exocortex Custom CDP Crawler (if built):**
    1.  Launch Chrome/Chromium with remote debugging port enabled (e.g., `--remote-debugging-port=9222`).
    2.  Connect CDP client to this port.
    3.  Use `Page.navigate(url)` to go to the target site's login page.
    4.  Programmatically fill login form fields (e.g., using `DOM.querySelector`, `DOM.setAttributeValue`, `Input.insertText`) and simulate clicks (`Input.dispatchMouseEvent`).
    5.  Alternatively, if cookies for an active session are available (e.g., exported from a user's browser or managed by a profile):
        *   Use `Network.setCookies` to inject these cookies before navigating to authenticated pages.
    6.  After successful login or cookie injection, navigate to target authenticated pages and extract content (e.g., `Runtime.evaluate({expression: "document.documentElement.outerHTML"})`).
    7.  Optionally, use `Network.getCookies` after login to save the session cookies for future use.

### 1.2. Performance and Fidelity [CR3]

*   CDP operations: Low latency (2-5ms per op).
*   Session replay fidelity: Can achieve 95%+ by carefully managing cookies, User-Agent, and other headers via CDP.

## 2. WARC/WACZ Management [UG Sec 11.3]

*   **WARC (Web ARChive):** ISO standard for storing web crawls (requests, responses, metadata). Enables long-term preservation and replay.
*   **WACZ (Web Archive Collection Zipped):** Bundles WARCs, `datapackage.json` (collection metadata), full-text index (optional), page lists into a single ZIP. Output format of Browsertrix Crawler. Viewable with ReplayWeb.page.
*   **Exocortex Storage:** WARC/WACZ files are stored as `core_blobs` (git-annexed). Metadata in `core_artifacts`.

## 3. DOM Diffing for Change Tracking [UG Sec 11.3, CR3]

To efficiently detect significant changes on re-crawled pages without storing full DOM every time if only minor changes occurred.

### 3.1. Library and Algorithm [CR3]

*   **Library Example:** `diff-dom` (JavaScript). Concept can be implemented in other languages. Compares two DOM trees, produces a structured diff object.
*   **Process (by `WebArchivingAgent` or a dedicated diffing agent):**
    1.  **Initial Crawl:** Store full DOM content (e.g., `document.documentElement.outerHTML` from CDP/Puppeteer, or extracted from WARC) as `core_blobs` or directly in `core_artifact_contents` if small enough. Calculate and store its BLAKE3 hash.
    2.  **Subsequent Re-crawls:**
        a.  Fetch new DOM.
        b.  Compute diff between new DOM and previously stored DOM (latest version in `core_artifact_contents` for that URL/artifact) using `diff-dom` or similar.
        c.  Serialize the diff object to a canonical string format.
        d.  Calculate a cryptographic hash (e.g., SHA-256 or BLAKE3) of this *serialized diff string*. This is the "diff hash".
    3.  **Decision Logic:**
        *   **No Change:** If new DOM's full content hash matches previous full content hash, only update "last crawled" timestamp.
        *   **Significant Change:** If diff hash is new AND diff object indicates substantial changes (e.g., diff object size > threshold, or specific important elements changed based on heuristics/rules):
            *   Store new full DOM as a new version in `core_artifact_contents` (or new `core_blobs`).
            *   Store the diff object (or its hash) in `core_artifact_contents.metadata` or a related table for auditing changes.
            *   Trigger downstream processing (re-embedding, re-analysis).
        *   **Minor/Insignificant Change:** If diff hash is new but indicates only minor changes (e.g., ad rotation, dynamic timestamp update, known insignificant DOM sections):
            *   Update "last crawled" timestamp.
            *   Do *not* store new full DOM if primary content is substantially similar.
            *   Do *not* re-trigger expensive downstream processing.
            *   May store the diff hash to recognize this "minor change pattern" if it reoccurs.
        *   **Known Change Pattern:** If current diff hash matches a previously seen diff hash for this page (either significant or minor), handle according to that pattern's established outcome.

### 3.2. Performance and Cache Hit Rate [CR3]

*   `diff-dom` typical DOM diff: 2-5 ms.
*   Diff hash cache hit rate (for re-crawls finding only previously observed changes or no significant changes): Can be high (e.g., 94.7% in CR3 benchmark), leading to significant optimization.

## 4. Storage Strategy (Web Archives) [UG Sec 11.4, CR3]

*   **Primary Storage (`git-annex` via `core_blobs`):** WARC/WACZ, full HTML snapshots (SingleFile), extracted Markdown, screenshots.
*   **Metadata:** `core_artifacts` (type `webpage_archive`) and `core_artifact_contents` (for Markdown versions, linking to `core_blobs` for raw archive).
*   **Retrieval Performance:**
    *   Local NVMe SSD annex: 800-1200 MB/s.
    *   S3 annex remote: 90-200 MB/s (network dependent).

## 5. Failure Modes for Web Archiving and Mitigation [UG Sec 11.5, OR3]

Refer to UG Section 11.5 for a detailed list of failure modes (Login Expired, CAPTCHAs, SSL Errors, Headless Browser Crashes, Partial Archival) and their mitigation strategies. The `WebArchivingAgent` must implement robust error handling, retries, and detection for these.

