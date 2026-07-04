"""add embedding_failed status to documents

Revision ID: f4c8e51a9d23
Revises: e2a7f01b3d08
Create Date: 2026-07-03 00:00:00.000000

Why this status is needed
--------------------------
The worker marks a document 'ready' as soon as text extraction succeeds,
before chunking/embedding runs, so the document is readable even if
embedding fails later (e.g. the model fails to load, or a batch encode
errors out). Previously, a failure in that later stage tried to set
status='failed', but the UPDATE's WHERE status = 'processing' guard was
already false (status was 'ready'), so the update silently no-op'd —
the document was left at 'ready' with zero chunks and no error recorded.

'embedding_failed' makes that outcome explicit and queryable: text was
extracted successfully, but the document has no searchable chunks.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "f4c8e51a9d23"
down_revision: Union[str, None] = "e2a7f01b3d08"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("ck_documents_status", "documents", type_="check")
    op.create_check_constraint(
        "ck_documents_status",
        "documents",
        "status IN ('processing', 'ready', 'embedding_failed', 'failed')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_documents_status", "documents", type_="check")
    op.create_check_constraint(
        "ck_documents_status",
        "documents",
        "status IN ('processing', 'ready', 'failed')",
    )
