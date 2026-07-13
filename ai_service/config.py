"""
Centralized configuration for the AI service, mirroring the pattern in
app/config.py and workers/config.py: env-overridable constants in one place.
"""

import os

# Sender identity the AI service uses when it *produces* a reply message.
# Reserved so incoming-message loop guards can recognize "this message came
# from the AI" without needing a separate message "type" field on the wire.
AI_SENDER_ID = os.getenv("AI_SENDER_ID", "ai-assistant")

# Sender identities that never trigger the AI, beyond AI_SENDER_ID itself.
# Populated with system/automation accounts as they're introduced.
SYSTEM_SENDER_IDS = frozenset(
    s for s in os.getenv("SYSTEM_SENDER_IDS", "").split(",") if s
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("AI_KAFKA_GROUP_ID", "ai-service-group")
KAFKA_POLL_TIMEOUT_S = 1.0

# Same topic app/core/kafka_producer.py publishes chat messages to.
MESSAGES_TOPIC = "channel-messages"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:new_password@localhost:5432/messaging",
)

# ── RAG / retrieval-augmented generation ────────────────────────────────────

# NVIDIA NIM (build.nvidia.com) — OpenAI-compatible chat completions endpoint.
# No local GPU required; requires an API key from https://build.nvidia.com.
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_API_BASE_URL = os.getenv(
    "NVIDIA_API_BASE_URL", "https://integrate.api.nvidia.com/v1"
)
# Instruction-tuned Llama 3.1 8B: fast, cheap, strong enough for grounded
# short-context QA. Swap via env for a larger model if answer quality demands it.
NVIDIA_CHAT_MODEL = os.getenv("NVIDIA_CHAT_MODEL", "meta/llama-3.1-8b-instruct")

# Measured directly against this endpoint: meta/llama-3.1-8b-instruct took
# ~10.4s end-to-end for a 5-token completion under real load, and a heavier
# model (llama-3.3-70b) consistently exceeded 12-15s and was dropped as the
# default (see NVIDIA_CHAT_MODEL) for exactly this reason. 20s gives headroom
# above the measured 8B latency without waiting unreasonably long on a
# genuine failure. Retries stay moderate — this is normal inference latency
# variance, not a dead connection, so there's no benefit to retrying rapidly.
NVIDIA_REQUEST_TIMEOUT_S = float(os.getenv("NVIDIA_REQUEST_TIMEOUT_S", "20.0"))
NVIDIA_MAX_RETRIES = int(os.getenv("NVIDIA_MAX_RETRIES", "3"))

# Generation params. Low temperature favors grounded, less creative answers —
# appropriate for RAG QA where hallucination is the primary failure mode.
NVIDIA_TEMPERATURE = float(os.getenv("NVIDIA_TEMPERATURE", "0.2"))
NVIDIA_MAX_TOKENS = int(os.getenv("NVIDIA_MAX_TOKENS", "1024"))

# Retrieval knobs — passed through to ChunkRepository.semantic_search.
RETRIEVAL_TOP_K = int(os.getenv("RETRIEVAL_TOP_K", "5"))
RETRIEVAL_MIN_SCORE = float(os.getenv("RETRIEVAL_MIN_SCORE", "0.6"))
RETRIEVAL_MAX_PER_DOCUMENT = int(os.getenv("RETRIEVAL_MAX_PER_DOCUMENT", "3"))

# Context budget in characters, not tokens: a fast, dependency-free guard
# against a pathological set of chunks blowing past the model's context
# window. ~4 chars/token is a conservative English-text approximation, so
# 16000 chars ≈ 4000 tokens of context — well inside an 8k+ context model
# alongside the system prompt, conversation framing, and the answer itself.
RETRIEVAL_CONTEXT_CHAR_BUDGET = int(
    os.getenv("RETRIEVAL_CONTEXT_CHAR_BUDGET", "16000")
)
