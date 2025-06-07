# TIM-BrowserExtensionAPIs: Browser Extension APIs for Data Capture

*   **Relevant ADR:** (N/A directly, core for browser ingestor)
*   **Original UG Context:** Section 10.1

This TIM details the key WebExtension APIs used by the Exocortex browser extension (Manifest V3) for capturing browsing activity and context.

## 1. Rationale Summary

Browser extensions provide privileged access to browsing events, tab states, history, and page content, crucial for understanding the user's web-based activities. The APIs detailed here form the sensory input for the browser ingestor.

## 2. `webNavigation` API [UG Sec 10.1.1, CR2]

Provides detailed information about the lifecycle of browser navigations.

*   **Key Events & Sequence:**
    1.  `onBeforeNavigate(details)`: Navigation about to occur. `details` (common for most events): `url`, `tabId`, `frameId`, `timeStamp`, `parentFrameId`.
    2.  `onCommitted(details)`: Navigation committed by browser (headers received). `details` adds `transitionType` (e.g., "link", "typed", "form_submit"), `transitionQualifiers` (e.g., "client_redirect", "server_redirect").
    3.  `onDOMContentLoaded(details)`: HTML document parsed, DOM ready.
    4.  `onCompleted(details)`: Page and all sub-resources (images, iframes) loaded.
    5.  `onErrorOccurred(details)`: Navigation error. `details` includes `error` string.
    6.  `onHistoryStateUpdated(details)`: Crucial for Single Page Applications (SPAs). Fires on `history.pushState()` or `history.replaceState()`. `details` include `url`, `transitionType`.
    7.  `onReferenceFragmentUpdated(details)`: URL fragment (`#hash`) changed.
*   **Event Data Structure (Typical `details` object from CR2):**
    ```javascript
    // {
    //   tabId: number,
    //   url: string,
    //   // processId: number, // Deprecated/restricted in MV3 for some events
    //   frameId: number, // 0 for main frame, positive for iframes
    //   parentFrameId: number, // -1 if main frame
    //   timeStamp: number, // DOMHighResTimeStamp (ms since epoch or navigation start)
    //   transitionType: string,
    //   transitionQualifiers: string[]
    //   // error?: string // For onErrorOccurred
    // }
    ```
*   **Exocortex Usage:** The extension listens to these events, formats their `details` into a payload (conforming to a schema like `browser.navigation.event_name`), and sends it to the native messaging host.

## 3. `storage` API [UG Sec 10.1.2, CR2]

Allows extensions to store data.

*   **`storage.local` (Preferred for Exocortex Extension Cache/Config):**
    *   Quota: Typically 5MB-10MB (Chrome 113+ 10MB). Can request `unlimitedStorage` permission.
    *   Use: Caching frequently accessed data, extension settings, temporary queues if native host is unavailable.
*   **`storage.session` (Chrome `chrome.storage.session`, Firefox `browser.storage.session`):**
    *   Quota: Typically 10MB.
    *   Persistence: For the duration of the browser session (persists across service worker restarts within that session).
    *   Use: Non-critical session-specific caches.
*   **`storage.sync` (Limited Use for Exocortex):**
    *   Quota: ~100KB total, 8KB/item, 512 items. Rate limited.
    *   Use: Only for very small user preferences that need to be synced across browser instances via browser's built-in sync (e.g., Chrome Sync, Firefox Sync). Exocortex primarily relies on its own multi-device sync.

## 4. `tabs` API (`chrome.tabs` / `browser.tabs`) [UG Sec 10.1.3]

Manages and interacts with browser tabs.

*   **Key Methods & Events:**
    *   `tabs.query(queryInfo)`: Find tabs (e.g., `tabs.query({active: true, currentWindow: true})` for current active tab).
    *   `tabs.get(tabId)`: Get details of a tab (URL, title, favIconUrl, windowId, status).
    *   `tabs.onUpdated.addListener((tabId, changeInfo, tab) => { ... })`: Fires when tab updates. `changeInfo` has `status` ("loading", "complete"), `url`, `title`, `favIconUrl`. Filter for `changeInfo.status === "complete"` and title changes for meaningful updates.
    *   `tabs.onActivated.addListener((activeInfo) => { ... })`: Active tab in a window changes. `activeInfo` has `tabId`, `windowId`.
    *   `tabs.onRemoved.addListener((tabId, removeInfo) => { ... })`: Tab closed.
*   **Exocortex Usage:** Track tab lifecycle, get current tab URL/title for context when other events fire (e.g., clipboard copy), capture tab state for session restoration features. Event: `browser.tab.created/updated/activated/removed`.

## 5. `history` API (`chrome.history` / `browser.history`) [UG Sec 10.1.3]

Accesses browser's browsing history.

*   **Key Methods & Events:**
    *   `history.search(query)`: Search history (text, time range).
    *   `history.getVisits({url: "..."})`: Get visit times for a URL.
    *   `history.onVisited.addListener((historyItem) => { ... })`: URL added to history. `historyItem` has `url`, `title`, `visitCount`, `typedCount`.
    *   `history.onTitleChanged.addListener(({url, title}) => { ... })`: Title of a page in history changes.
*   **Exocortex Usage:** Can supplement `webNavigation` for capturing visited URLs, especially for backfilling history or if `webNavigation` events are missed. Event: `browser.history.visited`.

## 6. `bookmarks` API (`chrome.bookmarks` / `browser.bookmarks`) [UG Sec 10.1.3]

Manages bookmarks.

*   **Key Methods & Events:**
    *   `bookmarks.getTree()`: Get entire bookmark tree.
    *   `bookmarks.onCreated.addListener((id, bookmark) => { ... })`
    *   `bookmarks.onRemoved.addListener((id, removeInfo) => { ... })`
    *   `bookmarks.onChanged.addListener((id, changeInfo) => { ... })`
*   **Exocortex Usage:** Capture bookmarks as potential PKM artifacts or links. Event: `browser.bookmark.created/changed/removed`. These are sent to the native host, which then may trigger web archiving for the bookmarked URL.

## 7. `scripting` API (Manifest V3) (`chrome.scripting` / `browser.scripting`) [UG Sec 10.1.3]

For injecting JavaScript/CSS into web pages from the extension's service worker.

*   **Methods:**
    *   `scripting.executeScript({target: {tabId: ...}, files: ["content_script.js"] / func: myFunc })`
    *   `scripting.insertCSS(...)`, `scripting.removeCSS(...)`
    *   `scripting.registerContentScripts(...)` (dynamic content script registration)
*   **Exocortex Usage:** Programmatically inject content scripts to extract page content (DOM, text, metadata), listen for in-page user interactions (clicks, form focus/blur), or perform actions on the page if needed by an agent.

## 8. Content Scripts (Declared in `manifest.json` or dynamically injected)

*   **Mechanism:** JavaScript files running in the context of web pages (sandboxed from page's JS, but can access/manipulate DOM).
*   **Communication:** Use `runtime.sendMessage(message)` to send data to the extension service worker, and `runtime.onMessage.addListener(callback)` in the service worker to receive.
*   **Exocortex Usage:**
    *   **Primary mechanism for page content extraction:** Get full text (`document.body.innerText`), main article content (using readability-like heuristics or libraries like `mozilla/readability.js` bundled with content script), specific metadata from DOM (`<meta>` tags, JSON-LD).
    *   **Interaction Capture:** Listen for DOM events like `click`, `submit` (on forms), `focus`/`blur` on input fields. Send details of these interactions (e.g., clicked element selector/text, form data) to the service worker.
    *   **Data sent to service worker is then relayed to native host.**

## 9. Cross-Browser API Compatibility (`chrome.*` vs `browser.*`) [UG Sec 10.1.4]

*   **Chrome/Chromium:** `chrome.*` namespace.
*   **Firefox:** `browser.*` namespace (also provides `chrome.*` aliases for many APIs).
*   **Exocortex Extension Strategy:**
    *   Use a WebExtension API polyfill (e.g., Mozilla's `webextension-polyfill`) which provides a promise-based `browser.*` API.
    *   Or, minimal check: `const browserApi = typeof browser !== "undefined" ? browser : chrome;`
    *   Target APIs common to both or use conditional logic for browser-specific features if absolutely necessary.

## 10. Manifest V3 Considerations [UG Sec 10.3, SR1, CR2]

*   **Service Workers:** Replace persistent background pages. Event-driven, terminate after inactivity (~30s). State must be managed via `storage.local/session` or offloaded to native host. Long-running tasks need `chrome.alarms` API or offloading.
*   **`webRequest` API Restrictions:** Blocking capabilities severely limited. Use non-blocking `webRequest` for observing requests. For modification/blocking, use Declarative Net Request (DNR) API (rule-based). DNR is less relevant for Exocortex capture-focused extension.
*   **No Arbitrary Remote Code Execution:** All JS must be bundled with extension.
*   **Impact on Exocortex Extension:**
    *   Content extraction via content scripts and `scripting.executeScript` remains effective.
    *   `webNavigation` and other observation APIs are available.
    *   Native Messaging becomes critical for offloading processing, persistent state, and any operations requiring system access or long-lived connections to the Exocortex backend.

