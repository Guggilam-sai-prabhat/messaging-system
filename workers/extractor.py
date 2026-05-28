import io
import logging
import re

import pypdf

from workers.config import MAX_PAGES
from workers.models import ExtractionResult

logger = logging.getLogger("document.worker")


class PDFExtractor:
    """
    Extracts and cleans text from a PDF byte blob using pypdf.

    Raises pypdf.errors.PdfReadError on a corrupted/invalid PDF.
    Raises ValueError if the PDF contains no extractable text.
    """

    @staticmethod
    def extract(pdf_bytes: bytes) -> ExtractionResult:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))

        total_pages = len(reader.pages)
        truncated = total_pages > MAX_PAGES
        pages_to_read = min(total_pages, MAX_PAGES)

        raw_chunks: list[str] = []

        for page_num in range(pages_to_read):
            try:
                page_text = reader.pages[page_num].extract_text() or ""
                raw_chunks.append(page_text)
            except Exception as e:
                logger.warning(
                    f"Page {page_num + 1}/{total_pages} extraction error "
                    f"(skipping): {e}"
                )

        raw_text = "\n".join(raw_chunks)
        cleaned = PDFExtractor._clean(raw_text)

        if not cleaned.strip():
            raise ValueError(
                "No extractable text found. "
                "The PDF may be image-only (scanned) or encrypted."
            )

        return ExtractionResult(
            text=cleaned,
            page_count=total_pages,
            truncated=truncated,
        )

    @staticmethod
    def _clean(raw: str) -> str:
        _table = str.maketrans({
            "ﬁ": "fi",
            "ﬂ": "fl",
            "’": "'",
            "‘": "'",
            "“": '"',
            "”": '"',
            "–": "-",
            " ": " ",
        })
        raw = raw.translate(_table)
        for src, dst in [("ﬃ", "ffi"), ("ﬄ", "ffl"), ("ﬀ", "ff"), ("—", "--")]:
            raw = raw.replace(src, dst)

        raw = re.sub(r"-\n(\S)", r"\1", raw)
        raw = raw.replace("\x00", "").replace("\x0c", "\n")
        raw = re.sub(r"[ \t]{2,}", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)

        return raw.strip()
