# OCR Text Extraction with Tesseract

**Status**: Designed, not implemented
**Implementation**: 0% (Design complete, implementation not started)
**Priority**: Medium
**Dependencies**: Tesseract OCR engine, image processing libraries, screenshot capture pipeline
**Blocks**: Text extraction from images, UI accessibility fallback, document processing

## Overview

Tesseract OCR provides mature, open-source text extraction from images. In Sinex, it serves as a fallback for inaccessible application UIs and enables text extraction from screenshots, documents, and other visual content.

## Technical Specification

### Tesseract OCR Integration

**Engine**: Tesseract OCR v4+ with LSTM models for better accuracy

**NixOS Packages**:
- `pkgs.tesseract`: Core OCR engine
- `pkgs.tessdata_best`: High-accuracy LSTM models
- `pkgs.tessdata_fast`: Faster, less accurate models
- Language packs: `eng` for English, etc.

**Command Line Interface**:
```bash
tesseract <input_image> <output_base> \
    -l <language_code> \
    --psm <page_segmentation_mode> \
    [config_options...]
```

### Page Segmentation Modes

Key PSM values for different use cases:
- `3`: Fully automatic page segmentation (default)
- `6`: Single uniform block of text (good for clean screenshots)
- `7`: Single text line
- `11`: Sparse text, find as much as possible
- `13`: Raw line, bypass Tesseract-specific hacks

### Image Preprocessing Pipeline

Critical for accuracy improvement:

**Preprocessing Steps**:
1. **Grayscale Conversion**: Reduce color complexity
2. **Binarization**: Convert to black & white (Otsu's method)
3. **Upscaling**: For small text (2-4x for screen text)
4. **De-skewing**: Correct rotation angles
5. **Noise Removal**: Clean up artifacts

**Example with ImageMagick**:
```bash
convert input.png \
    -colorspace Gray \
    -normalize \
    -threshold 60% \
    -deskew 40% \
    +repage \
    processed_for_ocr.png
```

## OCR Agent Architecture

### Processing Pipeline

1. **Image Acquisition**:
   - Screenshot capture via grim/slurp
   - Retrieve from git-annex storage
   - Direct image path input

2. **Preprocessing**:
   - Apply image enhancements
   - Optimize for OCR accuracy
   - Handle different image types

3. **OCR Execution**:
   - Run Tesseract with appropriate settings
   - Request multiple output formats:
     - Plain text for content
     - HOCR/TSV for coordinates and confidence

4. **Result Processing**:
   - Parse OCR output
   - Extract word-level details
   - Calculate confidence scores

### Event Schema

```json
{
  "source": "agent.ocr_processor",
  "event_type": "text_recognition_completed",
  "payload": {
    "source_image_annex_key": "key_of_original_screenshot",
    "preprocessed_image_annex_key": "key_of_processed_image",
    "extracted_text_full": "all recognized text...",
    "words_with_details": [
      {
        "text": "Hello",
        "confidence": 95.5,
        "bbox_x1": 10,
        "bbox_y1": 20,
        "bbox_x2": 50,
        "bbox_y2": 40,
        "line_num": 1,
        "word_num": 1
      }
    ],
    "page_segmentation_mode_used": 6,
    "languages_used": ["eng"],
    "average_confidence": 88.2,
    "original_capture_context": {
      "window_title": "Some Application",
      "application_class": "some_app",
      "region_on_screen": {"x": 100, "y": 100, "w": 300, "h": 50}
    }
  }
}
```

## Wayland Screenshot Integration

### Tools and Workflow

**Screenshot Capture**:
```bash
# Select region with visual feedback
grim -g "$(slurp -b 00000000 -c FF0000FF -s 1 -w 2)" -t png - | \
  agent_ocr_processor --stdin --context-json "{...}"
```

**Components**:
- `slurp`: Interactive region selection
- `grim`: Screenshot capture to stdout
- Pipeline to OCR agent for immediate processing

## Implementation Plan

### Phase 1: Core OCR Infrastructure
- [ ] Tesseract installation in NixOS module
- [ ] Basic OCR agent implementation
- [ ] Plain text extraction
- [ ] Integration with event system

### Phase 2: Image Preprocessing
- [ ] OpenCV or ImageMagick integration
- [ ] Preprocessing pipeline implementation
- [ ] Quality assessment metrics
- [ ] Adaptive preprocessing based on image type

### Phase 3: Advanced Features
- [ ] HOCR/TSV parsing for coordinates
- [ ] Confidence scoring and filtering
- [ ] Multi-language support
- [ ] Batch processing for documents

### Phase 4: Integration
- [ ] Screenshot capture pipeline
- [ ] Window monitoring for auto-OCR
- [ ] Accessibility fallback system
- [ ] Search integration for OCR results

### Phase 5: Optimization
- [ ] Performance benchmarking
- [ ] GPU acceleration (if available)
- [ ] Caching of repeated content
- [ ] Error handling and recovery

## Performance Considerations

**Processing Time**:
- LSTM models: 100ms-3s per image depending on size
- Fast models: 50-60% faster, 5-10% less accurate

**Accuracy Factors**:
- Image resolution (300+ DPI ideal)
- Font standardization
- Contrast and noise levels
- Text orientation

**Resource Usage**:
- CPU intensive during processing
- Memory: ~200-500MB for typical operations
- Disk: Model files ~10-50MB per language

## Use Cases

### Primary Applications
1. **Accessibility Fallback**: Extract text from inaccessible UIs
2. **Document Processing**: Convert scanned documents to searchable text
3. **Screenshot Analysis**: Extract text from captured screens
4. **Visual Content Indexing**: Make image text searchable

### Advanced Scenarios
- Real-time UI monitoring with OCR
- Automated form filling from visual data
- Cross-application text extraction
- Historical document digitization

## Future Enhancements

- **Alternative OCR Engines**: EasyOCR, PaddleOCR for comparison
- **Handwriting Recognition**: Specialized models for handwritten text
- **Layout Analysis**: Preserve document structure
- **Table Extraction**: Structured data from visual tables
- **Multi-column Support**: Complex document layouts
- **Real-time OCR**: Live text extraction from screen regions