"""
Prompt construction for retrieval-augmented chat answers.

Design decisions
-----------------
1. Grounding instruction lives in the SYSTEM prompt, not buried in the user
   turn. Models weight system-role instructions more heavily against
   conflicting signals in the user turn (e.g. a user saying "ignore the
   context and just answer from general knowledge"), which is exactly the
   injection surface a RAG endpoint needs to resist.

2. Explicit "insufficient information" escape hatch. The single biggest
   hallucination risk in RAG is the model treating retrieval as optional
   flavor text and falling back to parametric knowledge when the retrieved
   chunks don't actually answer the question. We instruct it to say so
   explicitly instead — a wrong "I don't know" is far cheaper than a
   confident wrong answer in a workplace chat context.

3. Each chunk is labeled with a source tag ([Source 1], [Source 2], ...)
   and the model is told to cite sources inline. This makes hallucinated
   claims visually distinguishable (an unlabelled claim) from grounded ones,
   and gives a human reviewer a way to spot-check without re-reading the
   whole document.

4. No chat history is threaded in yet — this endpoint answers one question
   against one retrieval pass. Multi-turn conversational memory would need
   its own design (what goes in context vs. gets re-embedded) and is out of
   scope for wiring retrieval into a single /ask turn.

Context size / token limits
----------------------------
- RETRIEVAL_CONTEXT_CHAR_BUDGET (default 16000 chars ≈ 4000 tokens) caps the
  total size of concatenated chunk content, not just chunk *count*. Capping
  by count alone (e.g. "top 5") is insufficient because chunk length varies
  (workers/chunker.py targets ~600 words but table/large-paragraph chunks can
  run longer) — a handful of oversized chunks could still blow the budget.
- Chunks are added in ranked order (highest score first) until the budget
  would be exceeded; the remainder are dropped rather than truncated
  mid-chunk, since a half-sentence of context is worse than one fewer chunk.
- meta/llama-3.1-8b-instruct has a 128k-token context window, so 4000 tokens
  of retrieved context leaves enormous headroom — the char budget here is a
  deliberately conservative ceiling chosen for cost/latency (every token in
  the prompt is a token NVIDIA bills and the model must attend over), not
  because we're close to running out of window. Tune upward if answers are
  frequently cut short by missing context; NVIDIA_MAX_TOKENS (default 1024)
  independently caps the *output* length and needs separate tuning if answers
  are being truncated.
"""

from dataclasses import dataclass

from ai_service.config import RETRIEVAL_CONTEXT_CHAR_BUDGET

SYSTEM_PROMPT = """You are an AI assistant embedded in a team chat application. You answer \
questions using ONLY the context excerpts provided below, which were retrieved from \
documents shared in this channel.

Rules you must follow:
1. Base your answer strictly on the provided context. Do not use outside knowledge, \
even if you know the answer from elsewhere.
2. If the context does not contain enough information to answer the question, \
explicitly say so — for example: "I don't have enough information in this channel's \
documents to answer that." Do not guess or fill gaps with assumptions.
3. When you use a fact from the context, cite it inline using its source tag, e.g. [Source 2].
4. If the context is empty or entirely irrelevant to the question, say so directly instead \
of attempting an answer.
5. Be concise and direct. Do not pad the answer with generic disclaimers beyond what these \
rules require."""


@dataclass(frozen=True)
class RetrievedChunk:
    content: str
    score: float
    document_id: str
    chunk_index: int


@dataclass(frozen=True)
class PromptResult:
    system_prompt: str
    user_prompt: str
    sources_used: list[RetrievedChunk]
    context_truncated: bool  # True if some retrieved chunks were dropped for budget


def build_prompt(query: str, chunks: list[RetrievedChunk]) -> PromptResult:
    """
    Assemble the system + user prompt for one RAG turn.

    `chunks` must already be filtered (min_score) and ranked (best first) by
    ChunkRepository.semantic_search — this function does not re-rank, it only
    labels, budgets, and formats.

    If `chunks` is empty, the user prompt says so explicitly rather than
    presenting an empty context block, so the model isn't left to infer
    "no context" from a blank section (which some models silently ignore).
    """
    included: list[RetrievedChunk] = []
    used_chars = 0
    truncated = False

    for chunk in chunks:
        chunk_chars = len(chunk.content)
        if used_chars + chunk_chars > RETRIEVAL_CONTEXT_CHAR_BUDGET and included:
            truncated = True
            continue
        included.append(chunk)
        used_chars += chunk_chars

    if not included:
        context_block = "(No relevant context was found in this channel's documents.)"
    else:
        context_block = "\n\n".join(
            f"[Source {i}] (relevance score: {chunk.score:.2f})\n{chunk.content}"
            for i, chunk in enumerate(included, start=1)
        )

    user_prompt = f"""Context excerpts from this channel's documents:

{context_block}

Question: {query}

Answer the question using only the context above, following the rules in your instructions."""

    return PromptResult(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        sources_used=included,
        context_truncated=truncated,
    )
