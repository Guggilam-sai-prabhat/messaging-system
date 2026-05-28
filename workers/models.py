from dataclasses import dataclass


@dataclass
class DocumentEvent:
    """Strongly typed representation of a Kafka document-processing event."""

    document_id: str
    channel_id: str
    storage_path: str       # e.g. "channelId/documentId.pdf"
    file_name: str
    uploaded_by: str


@dataclass
class ExtractionResult:
    text: str
    page_count: int
    truncated: bool         # True if MAX_PAGES was hit
    empty_pages: list[int]  # 1-based page numbers that yielded no text
    content_hash: str       # SHA-256 of cleaned text
