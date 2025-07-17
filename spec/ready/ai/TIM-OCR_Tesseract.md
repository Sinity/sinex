# TIM-OCR_Tesseract: Content Analysis - OCR with Tesseract

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 0% (Design complete, implementation not started)
**Dependencies**: Tesseract OCR engine, image processing libraries, screenshot capture pipeline
**Blocks**: Text extraction from images, UI accessibility fallback, document processing

## MVP Specification
- Tesseract OCR engine integration
- Basic text extraction from screenshot images
- Image preprocessing for OCR accuracy
- Text confidence scoring and filtering
- Integration with image capture pipeline

## Enhanced Features
- Advanced image preprocessing and enhancement
- Multi-language OCR support
- OCR confidence optimization
- Integration with accessibility tools
- Batch processing for document archives
- OCR result validation and correction

## Implementation Checklist
- [ ] Tesseract installation and configuration
- [ ] Image preprocessing pipeline
- [ ] OCR text extraction interface
- [ ] Confidence scoring and filtering
- [ ] Integration with screenshot capture
- [ ] Multi-language model support
- [ ] Performance optimization
- [ ] Error handling and fallbacks
- [ ] Quality assessment and validation

*   **Relevant ADR:** (N/A directly, but supports fallback for AT-SPI2 and visual capture analysis)
*   **Original UG Context:** Section 18.1

This TIM details the use of Tesseract OCR for extracting text from images (screenshots) within the Exocortex.

## 1. Rationale Summary

Tesseract OCR is a mature, open-source engine for converting images of text into machine-readable text. It's used in Exocortex as a fallback for inaccessible application UIs, or for extracting text from images captured via screenshots or other visual means.

## 2. Tesseract OCR Tooling [UG Sec 18.1, OR2]

*   **Engine:** Tesseract OCR (v4+ with LSTM models recommended for better accuracy).
*   **NixOS Packages:**
    *   `pkgs.tesseract`: The engine.
    *   Language data: `pkgs.tessdata` (standard models), `pkgs.tessdata_fast` (faster, less accurate), `pkgs.tessdata_best` (slower, more accurate LSTM models). Install desired language packs (e.g., `eng` for English).
*   **Command Line Usage:**
    ```bash
    tesseract <input_image_path_or_stdin> <output_base_name_or_stdout> \
        -l <language_code_eg_eng> \
        --psm <page_segmentation_mode_0_to_13> \
        [configfile...]
    ```
    *   **Input:** Image file (PNG, JPEG, TIFF) or `stdin` (for piped image data).
    *   **Output:** `stdout` for direct text output, or specified base name for files (e.g., `out.txt`).
        *   Other output formats: `hocr` (HTML with coordinates), `alto` (XML with coordinates), `tsv` (tab-separated values with coordinates/confidences), `pdf` (searchable PDF).
    *   **`-l lang`:** Language (e.g., `eng`, `deu`, `eng+fra`). `osd` for script detection.
    *   **`--psm <mode>` (Page Segmentation Mode):**
        *   `3`: Fully automatic page segmentation (often default).
        *   `6`: Assume a single uniform block of text (good for clean screenshots of paragraphs).
        *   `7`: Treat image as a single text line.
        *   `11`: Sparse text, find as much as possible.
        *   `13`: Raw line, single line, bypass Tesseract-specific hacks.
    *   **Config Files:** Can pass Tesseract variable settings via config files (e.g., `tessedit_char_whitelist`, `preserve_interword_spaces`).

## 3. Language Data [UG Sec 18.1, OR2]

*   Requires `.traineddata` files for each language/script.
*   Ensure `TESSDATA_PREFIX` environment variable points to the directory containing `tessdata` folder, or that Tesseract can find them in standard locations. NixOS packaging usually handles this.

## 4. Performance and Accuracy [UG Sec 18.1, OR2]

*   **CPU Intensive:** LSTM models (Tesseract 4+) can take several hundred ms to a few seconds per image on CPU, depending on image size, text density, model complexity.
*   **Accuracy Factors:**
    *   **Image Quality:** High resolution (300+ DPI ideal for scanned docs, screen DPI for screenshots), good contrast, no noise/blur.
    *   **Font:** Standard fonts easier than stylized/small fonts.
    *   **Preprocessing:** Can significantly improve accuracy.
        *   Binarization (to black & white, e.g., Otsu's method).
        *   Upscaling (for small text).
        *   De-skewing (correcting rotation).
        *   Noise removal.
        *   Libraries: OpenCV, Leptonica (used by Tesseract).
        *   Example preprocessing with ImageMagick (CLI):
            ```bash
            # convert input.png -colorspace Gray -normalize -threshold 60% -deskew 40% +repage processed_for_ocr.png
            # tesseract processed_for_ocr.png stdout ...
            ```

## 5. Exocortex OCR Agent Implementation (`agent_ocr_processor`) [UG Sec 18.3]

*   **Trigger:**
    *   User hotkey (selects region/window, screenshot taken, path sent to agent).
    *   Agentic: Hyprland ingestor detects damage in OCR-monitored window.
    *   Other agents: Image linked in PKM note, etc.
    *   Consumes an event like `sinex.visual.ocr_request` (payload: `image_annex_key` or `image_path`, `region_coordinates_json`, `source_context_json`).
*   **Processing Steps:**
    1.  Retrieve image blob from `git-annex` or path.
    2.  (Optional but Recommended) Perform image preprocessing (e.g., using OpenCV Python/Rust bindings or ImageMagick CLI).
    3.  Invoke Tesseract CLI:
        *   Pipe preprocessed image data to `tesseract stdin stdout ...`
        *   Request multiple output formats if needed (e.g., `txt` for plain text, `hocr` or `tsv` for bounding boxes and confidences).
            ```bash
            # Example command within agent (pseudo-code)
            # image_data_bytes = preprocess(image_blob)
            # text_output = execute_command("tesseract stdin stdout -l eng --psm 6", input=image_data_bytes)
            # hocr_output = execute_command("tesseract stdin stdout -l eng --psm 6 hocr", input=image_data_bytes)
            ```
    4.  Parse Tesseract output:
        *   Plain text.
        *   If HOCR/TSV: Parse XML/TSV to extract text per word/line, bounding boxes (`bbox x0 y0 x1 y1`), and confidence scores (`x_wconf`).
*   **Eventification & Storage:**
    1.  Store original screenshot (if not already) and preprocessed image as `core_blobs`.
    2.  Emit `ocr.text_recognized.completed` event to `core.events`.
        *   `source`: `"agent.ocr_processor"`
        *   `event_type`: `"text_recognition_completed"`
        *   `payload`:
            ```json
            // {
            //   "source_image_annex_key": "key_of_original_screenshot",
            //   "preprocessed_image_annex_key": "key_of_image_sent_to_tesseract", // Optional
            //   "extracted_text_full": "all recognized text...",
            //   "words_with_details": [ // If HOCR/TSV parsed
            //     { "text": "Hello", "confidence": 95.5, "bbox_x1": 10, "bbox_y1": 20, "bbox_x2": 50, "bbox_y2": 40, "line_num": 1, "word_num": 1 },
            //     ...
            //   ],
            //   "page_segmentation_mode_used": 6,
            //   "languages_used": ["eng"],
            //   "average_confidence": 88.2, // Optional: calculated from word confidences
            //   "original_capture_context": { // From the ocr_request event
            //     "window_title": "Some Application",
            //     "application_class": "some_app",
            //     "region_on_screen": {"x":100, "y":100, "w":300, "h":50}
            //   }
            // }
            ```
    3.  Store `extracted_text_full` (and potentially structured word details) in `core_artifact_contents` linked to a new `core_artifacts` entry (type `ocr_result`). This artifact is linked to the source image `core_blobs` entry. This makes OCRed text embeddable and searchable.

## 6. Wayland Screenshot for OCR Source [UG Sec 18.1, OR2]

*   The agent triggering OCR (or user via script) uses Wayland screenshot tools:
    *   `grim` and `slurp` for region/window selection.
    *   Example: `grim -g "$(slurp -b 00000000 -c FF0000FF -s 1 -w 2)" -t png - | /opt/sinex/bin/agent_ocr_processor --stdin --context-json "{...}"`
        *   `slurp` selects region (custom border/selection colors).
        *   `grim` captures that region as PNG to `stdout`.
        *   `agent_ocr_processor` (hypothetical CLI for the agent) reads PNG from `stdin`, gets context via JSON arg, then performs OCR and Exocortex event logging.

