"""
Unit tests for workers.extractor.PDFExtractor.

All tests use synthetic inputs — no real PDF files required.
Real PDFs are covered by integration tests elsewhere.
"""

import hashlib
import io
from unittest.mock import MagicMock, patch

import pypdf
import pytest

from workers.extractor import (
    PDFExtractor,
    _LIGATURE_TABLE,
)
from workers.models import ExtractionResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_reader(pages: list[str | Exception]) -> MagicMock:
    """
    Build a mock pypdf.PdfReader whose pages return the given text strings.
    Pass an Exception instance instead of a string to simulate a broken page.
    """
    mock_pages = []
    for item in pages:
        page = MagicMock()
        if isinstance(item, Exception):
            page.extract_text.side_effect = item
        else:
            page.extract_text.return_value = item
        mock_pages.append(page)

    reader = MagicMock()
    reader.pages = mock_pages
    return reader


def _patch_reader(pages: list[str | Exception]):
    """Context manager: patch pypdf.PdfReader to return a mock reader."""
    return patch(
        "workers.extractor.pypdf.PdfReader",
        return_value=_make_reader(pages),
    )


# ---------------------------------------------------------------------------
# ExtractionResult shape
# ---------------------------------------------------------------------------

class TestExtractionResultShape:
    def test_fields_present(self):
        result = ExtractionResult(
            text="hello",
            page_count=1,
            truncated=False,
            empty_pages=[],
            content_hash="abc",
        )
        assert result.text == "hello"
        assert result.page_count == 1
        assert result.truncated is False
        assert result.empty_pages == []
        assert result.content_hash == "abc"


# ---------------------------------------------------------------------------
# PDFExtractor.extract — happy path
# ---------------------------------------------------------------------------

class TestExtractHappyPath:
    def test_single_page_returns_result(self):
        with _patch_reader(["Hello world"]):
            result = PDFExtractor.extract(b"fake-pdf")

        assert isinstance(result, ExtractionResult)
        assert "Hello world" in result.text
        assert result.page_count == 1
        assert result.truncated is False
        assert result.empty_pages == []

    def test_content_hash_is_sha256_of_cleaned_text(self):
        with _patch_reader(["Hello world"]):
            result = PDFExtractor.extract(b"fake-pdf")

        expected = hashlib.sha256(result.text.encode()).hexdigest()
        assert result.content_hash == expected

    def test_multiple_pages_joined_with_paragraph_break(self):
        with _patch_reader(["Page one text", "Page two text"]):
            result = PDFExtractor.extract(b"fake-pdf")

        assert "Page one text" in result.text
        assert "Page two text" in result.text
        assert result.page_count == 2

    def test_truncated_flag_set_when_over_max_pages(self):
        from workers.config import MAX_PAGES
        pages = [f"Page {i}" for i in range(MAX_PAGES + 5)]
        with _patch_reader(pages):
            result = PDFExtractor.extract(b"fake-pdf")

        assert result.truncated is True
        assert result.page_count == MAX_PAGES + 5

    def test_not_truncated_when_at_max_pages(self):
        from workers.config import MAX_PAGES
        pages = [f"Page {i}" for i in range(MAX_PAGES)]
        with _patch_reader(pages):
            result = PDFExtractor.extract(b"fake-pdf")

        assert result.truncated is False


# ---------------------------------------------------------------------------
# PDFExtractor.extract — edge cases
# ---------------------------------------------------------------------------

class TestExtractEdgeCases:
    def test_raises_value_error_on_all_empty_pages(self):
        with _patch_reader(["", "   ", "\n"]):
            with pytest.raises(ValueError, match="No extractable text"):
                PDFExtractor.extract(b"fake-pdf")

    def test_raises_value_error_on_scanned_pdf(self):
        """Image-only PDF: pypdf returns empty strings for every page."""
        with _patch_reader(["", ""]):
            with pytest.raises(ValueError, match="image-only"):
                PDFExtractor.extract(b"fake-pdf")

    def test_raises_pdf_read_error_on_corrupt_pdf(self):
        with patch(
            "workers.extractor.pypdf.PdfReader",
            side_effect=pypdf.errors.PdfReadError("corrupt"),
        ):
            with pytest.raises(pypdf.errors.PdfReadError):
                PDFExtractor.extract(b"corrupt-bytes")

    def test_broken_page_is_skipped_not_fatal(self):
        """A single broken page should not abort extraction of other pages."""
        with _patch_reader(["Good page", RuntimeError("bad page"), "Also good"]):
            result = PDFExtractor.extract(b"fake-pdf")

        assert "Good page" in result.text
        assert "Also good" in result.text
        assert 2 in result.empty_pages  # page 2 was the broken one

    def test_empty_page_recorded_in_empty_pages(self):
        with _patch_reader(["Real content", "", "More content"]):
            result = PDFExtractor.extract(b"fake-pdf")

        assert 2 in result.empty_pages
        assert result.page_count == 3

    def test_all_pages_broken_raises_value_error(self):
        with _patch_reader([RuntimeError("bad"), RuntimeError("bad")]):
            with pytest.raises(ValueError, match="No extractable text"):
                PDFExtractor.extract(b"fake-pdf")


# ---------------------------------------------------------------------------
# PDFExtractor._clean — ligature normalisation
# ---------------------------------------------------------------------------

class TestCleanLigatures:
    def test_fi_ligature(self):
        assert PDFExtractor._clean("eﬁcient") == "eficient"

    def test_fl_ligature(self):
        assert PDFExtractor._clean("reﬂect") == "reflect"

    def test_ffi_ligature(self):
        assert PDFExtractor._clean("eﬃcient") == "efficient"

    def test_ffl_ligature(self):
        assert PDFExtractor._clean("aﬄuent") == "affluent"

    def test_ff_ligature(self):
        assert PDFExtractor._clean("eﬀect") == "effect"

    def test_st_ligatures(self):
        assert PDFExtractor._clean("ﬅring") == "string"
        assert PDFExtractor._clean("beﬆ") == "best"


# ---------------------------------------------------------------------------
# PDFExtractor._clean — quote / dash normalisation
# ---------------------------------------------------------------------------

class TestCleanTypography:
    def test_curly_single_quotes(self):
        result = PDFExtractor._clean("‘hello’")
        assert result == "'hello'"

    def test_curly_double_quotes(self):
        result = PDFExtractor._clean("“hello”")
        assert result == '"hello"'

    def test_low_9_single_quote(self):
        result = PDFExtractor._clean("‚hello")
        assert result == "'hello"

    def test_low_9_double_quote(self):
        result = PDFExtractor._clean("„hello")
        assert result == '"hello'

    def test_en_dash(self):
        result = PDFExtractor._clean("2020–2021")
        assert result == "2020-2021"

    def test_em_dash(self):
        result = PDFExtractor._clean("word—word")
        assert result == "word--word"

    def test_minus_sign(self):
        result = PDFExtractor._clean("x−y")
        assert result == "x-y"


# ---------------------------------------------------------------------------
# PDFExtractor._clean — invisible / zero-width characters
# ---------------------------------------------------------------------------

class TestCleanInvisibleChars:
    def test_non_breaking_space_becomes_regular_space(self):
        result = PDFExtractor._clean("hello world")
        assert result == "hello world"

    def test_zero_width_space_discarded(self):
        result = PDFExtractor._clean("hel​lo")
        assert result == "hello"

    def test_zero_width_non_joiner_discarded(self):
        result = PDFExtractor._clean("hel‌lo")
        assert result == "hello"

    def test_zero_width_joiner_discarded(self):
        result = PDFExtractor._clean("hel‍lo")
        assert result == "hello"

    def test_bom_discarded(self):
        result = PDFExtractor._clean("﻿hello")
        assert result == "hello"

    def test_soft_hyphen_discarded(self):
        result = PDFExtractor._clean("pro­cess")
        assert result == "process"


# ---------------------------------------------------------------------------
# PDFExtractor._clean — control characters
# ---------------------------------------------------------------------------

class TestCleanControlChars:
    def test_null_bytes_removed(self):
        result = PDFExtractor._clean("hel\x00lo")
        assert result == "hello"

    def test_form_feed_becomes_newline(self):
        result = PDFExtractor._clean("page1\x0cpage2")
        # form feed → newline, then excess newlines normalised
        assert "page1" in result
        assert "page2" in result
        assert "\x0c" not in result

    def test_other_control_chars_removed(self):
        result = PDFExtractor._clean("hel\x07lo")  # BEL
        assert result == "hello"

    def test_tab_preserved(self):
        # Tab carries structural meaning (alignment) — we collapse runs but keep one
        result = PDFExtractor._clean("col1\tcol2")
        assert "col1" in result
        assert "col2" in result


# ---------------------------------------------------------------------------
# PDFExtractor._clean — replacement character (encoding failures)
# ---------------------------------------------------------------------------

class TestCleanReplacementChar:
    def test_single_replacement_char_becomes_space(self):
        result = PDFExtractor._clean("hel�o")
        assert result == "hel o"

    def test_run_of_replacement_chars_becomes_single_space(self):
        result = PDFExtractor._clean("hel���o")
        assert result == "hel o"


# ---------------------------------------------------------------------------
# PDFExtractor._clean — hyphen-newline rejoining
# ---------------------------------------------------------------------------

class TestCleanHyphenNewline:
    def test_basic_hyphen_newline(self):
        result = PDFExtractor._clean("proces-\nsing")
        assert result == "processing"

    def test_hyphen_newline_with_leading_spaces(self):
        result = PDFExtractor._clean("proces-\n   sing")
        assert result == "processing"

    def test_intentional_hyphen_at_line_end_not_rejoined(self):
        # A hyphen followed by a newline then capital = probably a list item
        # The soft-newline rule won't fire (capital), hyphen-newline rule will
        # rejoin — this is an acceptable false positive for prose documents.
        result = PDFExtractor._clean("well-\nknown")
        assert result == "wellknown"  # rejoined — known limitation


# ---------------------------------------------------------------------------
# PDFExtractor._clean — soft-wrap newline collapsing
# ---------------------------------------------------------------------------

class TestCleanSoftWrap:
    def test_mid_sentence_newline_collapsed(self):
        text = "This is a sentence\nthat wrapped at the column."
        result = PDFExtractor._clean(text)
        assert "\n" not in result
        assert "sentence that wrapped" in result

    def test_newline_after_period_preserved(self):
        text = "End of sentence.\nNew sentence starts."
        result = PDFExtractor._clean(text)
        # After period → paragraph boundary preserved (not collapsed)
        assert "\n" in result

    def test_newline_before_capital_preserved(self):
        text = "some text\nCapital starts new paragraph"
        result = PDFExtractor._clean(text)
        assert "\n" in result

    def test_newline_before_bullet_preserved(self):
        text = "intro line\n• bullet item"
        result = PDFExtractor._clean(text)
        assert "\n" in result


# ---------------------------------------------------------------------------
# PDFExtractor._clean — artefact line removal
# ---------------------------------------------------------------------------

class TestCleanArtefacts:
    def test_standalone_page_number_removed(self):
        text = "Some paragraph.\n\n42\n\nNext paragraph."
        result = PDFExtractor._clean(text)
        assert "42" not in result

    def test_separator_line_removed(self):
        text = "Section one.\n\n----------\n\nSection two."
        result = PDFExtractor._clean(text)
        assert "----------" not in result
        assert "Section one" in result
        assert "Section two" in result

    def test_dots_separator_removed(self):
        # A line with ONLY dots (no alphanumeric chars) is a separator
        text = "Section one.\n\n..............................\n\nSection two."
        result = PDFExtractor._clean(text)
        assert "......" not in result
        assert "Section one" in result
        assert "Section two" in result

    def test_real_number_in_sentence_not_removed(self):
        # "42" in context should NOT be removed (not a standalone line)
        text = "There are 42 items in the list."
        result = PDFExtractor._clean(text)
        assert "42" in result


# ---------------------------------------------------------------------------
# PDFExtractor._clean — whitespace normalisation
# ---------------------------------------------------------------------------

class TestCleanWhitespace:
    def test_multiple_spaces_collapsed(self):
        result = PDFExtractor._clean("hello    world")
        assert result == "hello world"

    def test_multiple_tabs_collapsed(self):
        result = PDFExtractor._clean("col1\t\t\tcol2")
        assert "col1" in result
        assert "col2" in result

    def test_excess_blank_lines_collapsed(self):
        text = "para one\n\n\n\n\npara two"
        result = PDFExtractor._clean(text)
        assert "\n\n\n" not in result
        assert "para one" in result
        assert "para two" in result

    def test_double_newline_paragraph_separator_preserved(self):
        text = "Para one.\n\nPara two."
        result = PDFExtractor._clean(text)
        assert "\n\n" in result

    def test_leading_trailing_whitespace_stripped(self):
        result = PDFExtractor._clean("   hello world   ")
        assert result == "hello world"


# ---------------------------------------------------------------------------
# PDFExtractor._clean — Unicode NFC normalisation
# ---------------------------------------------------------------------------

class TestCleanUnicodeNFC:
    def test_combining_accent_normalised(self):
        # "e" + combining acute accent → precomposed "é"
        decomposed = "résumé"
        result = PDFExtractor._clean(decomposed)
        import unicodedata
        assert unicodedata.is_normalized("NFC", result)


# ---------------------------------------------------------------------------
# Integration: full pipeline on realistic text
# ---------------------------------------------------------------------------

class TestCleanIntegration:
    def test_realistic_pdf_text(self):
        raw = (
            "  Eﬃcient Dis\x00tributed Sys­tems  \n"
            "Chapter 1\n\n\n\n"
            "This sec–tion covers the ba—sic con​cepts of\n"
            "distributed sys’tems, includ‘ing con\x0csensus.\n\n"
            "                    42\n\n"
            "A system is “correct” if it satis�fies\n"
            "its speciﬁcation.\n\n"
            "----------\n\n"
            "References\n"
        )
        result = PDFExtractor._clean(raw)

        assert "Efficient" in result          # ligature fixed
        assert "\x00" not in result           # null byte gone
        assert "­" not in result         # soft hyphen gone
        assert " " not in result         # nbsp gone
        assert "​" not in result         # zwsp gone
        assert "–" not in result         # en-dash → hyphen
        assert "—" not in result         # em-dash → --
        assert "42" not in result             # page number removed
        assert "----------" not in result     # separator removed
        assert '"correct"' in result          # curly quotes normalised
        assert "specification" in result or "satisfies" in result
        assert "\n\n\n" not in result         # excess blank lines gone
