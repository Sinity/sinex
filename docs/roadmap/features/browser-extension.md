# Browser Extension for Web Activity Capture

## Overview
A Manifest V3 WebExtension that captures browsing activity, page content, and user interactions to provide comprehensive web context for the Exocortex system.

## MVP Specification
- Manifest V3 compatible extension (Chrome/Firefox)
- Native messaging host integration
- Basic navigation event capture (webNavigation API)
- Tab lifecycle tracking (tabs API)
- Content script for page text extraction
- Local storage for temporary event queuing

## Enhanced Features
- Advanced content extraction with readability algorithms
- Form interaction and click tracking
- Bookmark and history synchronization
- Dynamic content script injection
- Session restoration capabilities
- Cross-browser compatibility layer
- Offline event queuing and batch sync
- Privacy-focused selective capture modes

## Technical Architecture

### Core APIs Used
- **webNavigation**: Page lifecycle events (navigate, load, error)
- **tabs**: Tab state management and activation tracking
- **storage.local**: Event queue and configuration cache
- **scripting**: Dynamic content script injection
- **runtime**: Native messaging communication
- **history/bookmarks**: Browser data synchronization

### Event Flow
1. Browser events captured by service worker
2. Events formatted according to schema
3. Events sent to native messaging host
4. Native host forwards to Sinex collector
5. Fallback to local storage queue if host unavailable

### Content Extraction
- Full page text via `document.body.innerText`
- Structured data from meta tags and JSON-LD
- Main article content using readability heuristics
- Form field values and interaction context
- Click targets and navigation patterns

## Implementation Roadmap

### Phase 1: Foundation
- [ ] Manifest V3 extension structure
- [ ] Native messaging protocol
- [ ] Basic navigation tracking
- [ ] Simple content extraction
- [ ] Chrome compatibility

### Phase 2: Enhanced Capture
- [ ] Advanced content parsing
- [ ] User interaction tracking
- [ ] Firefox compatibility
- [ ] Offline queue management
- [ ] Configuration UI

### Phase 3: Intelligence
- [ ] Smart content filtering
- [ ] Privacy mode controls
- [ ] Session reconstruction
- [ ] Performance optimization
- [ ] Cross-device sync support

## Technical Challenges

### Manifest V3 Constraints
- Service workers terminate after ~30s
- No persistent background pages
- Limited webRequest capabilities
- All code must be bundled

### Solutions
- Use chrome.alarms for periodic tasks
- Store state in storage.local/session
- Offload processing to native host
- Pre-bundle all JavaScript dependencies

## Privacy Considerations
- User-controlled capture modes
- Exclude sensitive domains
- Local processing preference
- Encrypted event transmission
- Clear data retention policies

## Performance Targets
- <10ms event capture overhead
- <100ms content extraction time
- <1MB memory footprint
- Minimal impact on page load
- Efficient batch synchronization

## Related Components
- TIM-BrowserNativeMessaging: Native host communication
- TIM-WebArchivingTooling: Page content archival
- Core event ingestion pipeline