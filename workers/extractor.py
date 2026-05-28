import hashlib
import io
import logging
import re
import unicodedata

import pypdf

from workers.config import MAX_PAGES
from workers.models import ExtractionResult

logger = logging.getLogger("document.worker")


# ---------------------------------------------------------------------------
# Ligature / typographic character normalization table.
#
# Why this exists: PDF fonts frequently encode ligatures (ﬁ, ﬂ, ﬃ, ﬄ, ﬀ)
# as single Unicode codepoints in the "Alphabetic Presentation Forms" block
# (U+FB00–U+FB06). Tokenizers and embedding models trained on web text have
# almost never seen these, so "eﬃcient" and "efficient" produce completely
# different token sequences and embeddings.
#
# Curly quotes and typographic dashes have the same problem: they may or may
# not appear in a query, creating asymmetric matches. We normalize everything
# to plain ASCII equivalents before any further processing.
# ---------------------------------------------------------------------------
_LIGATURE_TABLE = str.maketrans({
    # Ligatures
    "ﬁ": "fi",
    "ﬂ": "fl",
    "ﬃ": "ffi",
    "ﬄ": "ffl",
    "ﬀ": "ff",
    "ﬅ": "st",
    "ﬆ": "st",
    # Curly / directional quotes  →  plain ASCII
    "\u2018": "'",   # '  LEFT SINGLE QUOTATION MARK
    "\u2019": "'",   # '  RIGHT SINGLE QUOTATION MARK
    "\u201c": '"',   # "  LEFT DOUBLE QUOTATION MARK
    "\u201d": '"',   # "  RIGHT DOUBLE QUOTATION MARK
    "\u201a": "'",   # ‚  SINGLE LOW-9 QUOTATION MARK
    "\u201e": '"',   # „  DOUBLE LOW-9 QUOTATION MARK
    # Dashes / hyphens
    "\u2013": "-",   # –  EN DASH
    "\u2014": "--",  # —  EM DASH
    "\u2212": "-",   # −  MINUS SIGN
    # Spaces that look like spaces but aren't
    "\u00a0": " ",   # NO-BREAK SPACE
    "\u200b": "",    # ZERO WIDTH SPACE  (discard)
    "\u200c": "",    # ZERO WIDTH NON-JOINER  (discard)
    "\u200d": "",    # ZERO WIDTH JOINER  (discard)
    "\ufeff": "",    # BOM  (discard)
    # Soft hyphen — marks a potential hyphenation point; not visible text
    "\u00ad": "",    # SOFT HYPHEN  (discard)
})

# Regex compiled once at module load — cheap at import, fast at call time.

# Hard hyphen at end of line followed by optional whitespace and a word char.
# Handles both "dis-\ntributed" and "dis-\n  tributed" (leading spaces after
# the newline are common in two-column PDFs).
_HYPHEN_NEWLINE = re.compile(r"-\n\s*(\S)")

# A newline that is NOT a paragraph boundary.
# Heuristic: if the previous line does NOT end with sentence-final punctuation
# (.  !  ?  :) and the next line does NOT start with a capital or bullet, it
# is a soft wrap we should collapse.
#
# Why heuristic and not rule: PDFs have no semantic markup. We cannot know
# whether a newline is intentional. This heuristic has false positives on
# numbered lists and code blocks — acceptable for prose-heavy documents.
_SOFT_NEWLINE = re.compile(
    r"(?<![.!?:])\n(?![A-Z\u2022\u2023\u25e6\u2043\u2219•\-\d])"
)

# Two or more blank lines → exactly one blank line (paragraph separator).
_EXCESS_BLANK_LINES = re.compile(r"\n{3,}")

# Runs of spaces/tabs within a line (but not newlines).
_INLINE_WHITESPACE = re.compile(r"[ \t]{2,}")

# Replacement character produced by encoding failures or bad OCR confidence.
_REPLACEMENT_CHAR = re.compile(r"\ufffd+")

# Lone digits or single characters on their own line — usually page numbers,
# header/footer artifacts, or table-of-contents leaders.
_PAGE_NUMBER_LINE = re.compile(r"^\s*\d{1,4}\s*$", re.MULTILINE)

# Lines that are purely non-alphanumeric (e.g. ".......", "--------", "===")
# These are visual separators in the source PDF that become noise in text.
_SEPARATOR_LINE = re.compile(r"^[\W_]+$", re.MULTILINE)


class PDFExtractor:
    """
    Extracts, normalises, and cleans text from a PDF byte blob.

    Pipeline
    --------
    1. pypdf page extraction with per-page fault isolation
    2. Ligature / typographic character normalisation
    3. NFC Unicode normalisation  (see _clean for rationale)
    4. Control character stripping
    5. Hyphen-newline rejoining
    6. Soft-wrap newline collapsing
    7. Page-number / separator-line removal
    8. Whitespace normalisation
    9. Paragraph boundary preservation

    Raises
    ------
    pypdf.errors.PdfReadError
        The byte blob is not a valid or not a readable PDF.
    ValueError
        No text could be extracted (image-only / encrypted PDF).
        Callers that want to attempt OCR should catch this and branch.
    """

    @staticmethod
    def extract(pdf_bytes: bytes) -> ExtractionResult:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))

        total_pages = len(reader.pages)
        truncated = total_pages > MAX_PAGES
        pages_to_read = min(total_pages, MAX_PAGES)

        raw_chunks: list[str] = []
        empty_pages: list[int] = []

        for page_num in range(pages_to_read):
            try:
                page_text = reader.pages[page_num].extract_text() or ""
            except Exception as exc:
                # Per-page isolation: a corrupt page should not abort the job.
                # Log it; downstream chunking will deal with the gap.
                logger.warning(
                    "Page %d/%d extraction failed (skipping): %s",
                    page_num + 1, total_pages, exc,
                )
                empty_pages.append(page_num + 1)
                continue

            if not page_text.strip():
                # pypdf returned an empty string.  Two causes:
                #   a) The page contains only images (scanned page).
                #   b) The page is genuinely blank.
                # We record both as "empty" for observability.  A caller
                # that sees many empty pages should route to an OCR worker.
                logger.info(
                    "Page %d/%d yielded no text (image-only or blank).",
                    page_num + 1, total_pages,
                )
                empty_pages.append(page_num + 1)
                continue

            raw_chunks.append(page_text)

        raw_text = "\n\n".join(raw_chunks)  # page break = paragraph boundary

        # ----------------------------------------------------------------
        # Edge case: entirely image-based PDF (scanned without OCR layer).
        # We raise a typed ValueError so callers can branch to an OCR path
        # rather than treating it as a generic failure.
        # ----------------------------------------------------------------
        if not raw_text.strip():
            raise ValueError(
                f"No extractable text found in {total_pages} page(s). "
                "The PDF appears to be image-only (scanned without an OCR "
                "layer) or is encrypted. Route to an OCR worker."
            )

        cleaned = PDFExtractor._clean(raw_text)
        content_hash = hashlib.sha256(cleaned.encode()).hexdigest()

        if empty_pages:
            logger.info(
                "Empty/unreadable pages: %s (out of %d)",
                empty_pages, total_pages,
            )

        return ExtractionResult(
            text=cleaned,
            page_count=total_pages,
            truncated=truncated,
            empty_pages=empty_pages,
            content_hash=content_hash,
        )

    # ------------------------------------------------------------------
    # Cleaning pipeline — each step is a pure str → str transformation.
    # Order matters: normalise first, then structural fixes, then whitespace.
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(raw: str) -> str:
        text = raw

        # ── Step 1: Ligature and typographic character normalisation ──────
        # Must happen BEFORE any regex work so subsequent patterns operate
        # on consistent ASCII characters.
        text = text.translate(_LIGATURE_TABLE)

        # ── Step 2: Unicode NFC normalisation ─────────────────────────────
        # NFC folds combining character sequences into precomposed forms.
        # Example: "e\u0301" (e + combining acute) → "é" (U+00E9).
        # Without this, OCR-generated text and query text may represent the
        # same word differently, producing embedding mismatches.
        text = unicodedata.normalize("NFC", text)

        # ── Step 3: Control character stripping ───────────────────────────
        # \x00 (null byte) and \x0c (form feed / page break) are emitted by
        # pypdf on some PDFs.  Null bytes corrupt downstream string handling
        # in many databases and APIs.  Form feeds are replaced with newlines
        # (we already insert \n\n between pages, but belt-and-suspenders).
        text = text.replace("\x00", "").replace("\x0c", "\n")

        # Strip other non-printable control characters (U+0000–U+001F) except
        # tab (\x09) and newline (\x0a), which carry structural meaning.
        text = re.sub(r"[\x01-\x08\x0b\x0e-\x1f]", "", text)

        # ── Step 4: Encoding failure markers ──────────────────────────────
        # U+FFFD (REPLACEMENT CHARACTER) indicates a byte sequence that could
        # not be decoded.  Isolated replacements are usually a single corrupt
        # character; runs indicate a larger encoding failure.
        # Replace with a space so we don't run adjacent words together.
        text = _REPLACEMENT_CHAR.sub(" ", text)

        # ── Step 5: Hyphen-newline rejoining ──────────────────────────────
        # "hyphen at end of line" is the PDF renderer telling us it broke a
        # word across lines.  We rejoin the pieces.
        # The pattern handles leading whitespace on the continuation line,
        # which pypdf emits in indented or two-column layouts.
        text = _HYPHEN_NEWLINE.sub(r"\1", text)

        # ── Step 6: Soft-wrap newline collapsing ──────────────────────────
        # PDFs wrap lines at a fixed column width unrelated to paragraph
        # boundaries.  A newline inside a sentence fragments the sentence into
        # separate "documents" if not collapsed.
        # Replace soft-wrap newlines with a single space.
        text = _SOFT_NEWLINE.sub(" ", text)

        # ── Step 7: Artefact line removal ─────────────────────────────────
        # Page numbers and visual separator lines are valid in the rendered
        # PDF but are pure noise in extracted text.
        text = _PAGE_NUMBER_LINE.sub("", text)
        text = _SEPARATOR_LINE.sub("", text)

        # ── Step 8: Inline whitespace normalisation ───────────────────────
        # Collapse runs of spaces/tabs within a line to a single space.
        # This handles the coordinate-based spacing pypdf reconstructs from
        # glyph positions (often multiple spaces between words).
        text = _INLINE_WHITESPACE.sub(" ", text)

        # ── Step 9: Paragraph boundary normalisation ──────────────────────
        # After the above steps there may be runs of 3+ newlines.
        # Collapse to exactly two (the conventional paragraph separator).
        # We intentionally do NOT collapse \n\n to a single \n — the double
        # newline is our only remaining structural signal for chunking.
        text = _EXCESS_BLANK_LINES.sub("\n\n", text)

        return text.strip()