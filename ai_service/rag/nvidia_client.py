"""
Client for NVIDIA NIM's chat completions endpoint
(https://integrate.api.nvidia.com/v1/chat/completions), which is
OpenAI-Chat-Completions-API-compatible.

Why this shells out to curl instead of using httpx
----------------------------------------------------
Originally written to work around what looked like a TLS-fingerprinting
issue (curl succeeded, hand-built Python `ssl` sockets hung). Root-caused
later: the real cause was request latency on the larger model
(meta/llama-3.3-70b-instruct took 12-15s+ under load; the default model was
changed to meta/llama-3.1-8b-instruct, which measured ~10.4s) combined with
a too-short client timeout — not a transport-layer or fingerprinting issue
at all. httpx would have worked fine with a longer timeout.

Kept on curl anyway since it's already verified working end-to-end against
this endpoint and there's no reason to churn back to httpx for its own sake.
If this ever needs to change, swapping back to httpx is a one-file change:
only chat_completion()'s internals change, the public API
(chat_completion(system_prompt, user_prompt, ...) -> str) doesn't.

Retry strategy: moderate exponential backoff for transient failures (5xx,
429, and any curl-level failure), no retry on 4xx client errors (bad
request, bad API key) since retrying won't change the outcome. See
NVIDIA_REQUEST_TIMEOUT_S in ai_service/config.py for the measured latency
this timeout is tuned against.
"""

import asyncio
import json
import logging
import os
import tempfile

from ai_service.config import (
    NVIDIA_API_BASE_URL,
    NVIDIA_API_KEY,
    NVIDIA_CHAT_MODEL,
    NVIDIA_MAX_RETRIES,
    NVIDIA_MAX_TOKENS,
    NVIDIA_REQUEST_TIMEOUT_S,
    NVIDIA_TEMPERATURE,
)

logger = logging.getLogger("ai_service.nvidia")

RETRY_BASE_DELAY_S = 1.0
RETRY_MAX_DELAY_S = 4.0


class NvidiaClientError(Exception):
    """Raised when the NVIDIA NIM API call fails after retries, or is misconfigured."""


class NvidiaChatClient:
    """
    Client for NVIDIA NIM's OpenAI-compatible chat completions API.

    Stateless aside from config — close() is a no-op kept for interface
    compatibility with callers (RagGenerator) that manage client lifecycle,
    in case a future transport swap (back to httpx, or a connection-pooling
    curl multi handle) needs teardown.
    """

    def __init__(self) -> None:
        if not NVIDIA_API_KEY:
            logger.warning(
                "NVIDIA_API_KEY is not set — chat completion calls will fail. "
                "Get a key from https://build.nvidia.com."
            )

    async def close(self) -> None:
        pass

    async def chat_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        model: str = NVIDIA_CHAT_MODEL,
        temperature: float = NVIDIA_TEMPERATURE,
        max_tokens: int = NVIDIA_MAX_TOKENS,
    ) -> str:
        """
        Return the assistant's reply text for a single-turn system+user prompt.

        Raises NvidiaClientError on missing API key, exhausted retries, or an
        unexpected response shape. Callers (the RAG generator) should catch
        this and degrade to an explicit "AI is unavailable" message rather
        than letting the exception surface as a crashed consumer.
        """
        if not NVIDIA_API_KEY:
            raise NvidiaClientError("NVIDIA_API_KEY is not configured")

        payload = json.dumps(
            {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
        )

        delay = RETRY_BASE_DELAY_S
        last_description = "unknown error"

        for attempt in range(1, NVIDIA_MAX_RETRIES + 1):
            try:
                status_code, body = await self._curl_post(payload)
            except NvidiaClientError as e:
                last_description = str(e)
                if attempt == NVIDIA_MAX_RETRIES:
                    logger.error(f"NVIDIA chat completion failed: {last_description}")
                    raise
                logger.warning(
                    f"NVIDIA chat completion attempt {attempt}/{NVIDIA_MAX_RETRIES} "
                    f"failed: {last_description} — retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_DELAY_S)
                continue

            if status_code == 429 or status_code >= 500:
                last_description = f"Retryable API error: HTTP {status_code} {body[:200]}"
                if attempt == NVIDIA_MAX_RETRIES:
                    logger.error(f"NVIDIA chat completion failed: {last_description}")
                    raise NvidiaClientError(last_description)
                logger.warning(
                    f"NVIDIA chat completion attempt {attempt}/{NVIDIA_MAX_RETRIES} "
                    f"failed: {last_description} — retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_DELAY_S)
                continue

            if status_code >= 400:
                # Client error (bad key, bad request) — retrying won't help.
                raise NvidiaClientError(
                    f"NVIDIA API request failed: HTTP {status_code} {body[:200]}"
                )

            try:
                data = json.loads(body)
                return data["choices"][0]["message"]["content"]
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                raise NvidiaClientError(f"Unexpected NVIDIA API response shape: {e}") from e

        raise NvidiaClientError(last_description)

    async def _curl_post(self, payload: str) -> tuple[int, str]:
        """
        POST `payload` to the chat completions endpoint via a curl subprocess.

        The JSON body is written to a temp file and passed via `--data-binary
        @<path>` rather than inlined as a `-d` argv value or through curl's
        `-K` config-file syntax — both require shell/config-quote escaping
        that's easy to get subtly wrong (verified: naive backslash/quote
        escaping through `-K` corrupts any payload containing a literal
        quote or backslash, which real chat prompts will contain). A file
        is passed byte-for-byte with no escaping needed.

        The API key is passed via a header file (`-H @<path>` is not
        supported by curl, so it's read into a `header = "..."` line in a
        `-K` config that carries ONLY the auth header, never the payload)
        rather than as an argv value, so it doesn't appear in `ps`/process
        listings. Returns (status_code, response_body); raises
        NvidiaClientError on subprocess failure, timeout, or malformed output.
        """
        url = f"{NVIDIA_API_BASE_URL}/chat/completions"

        with tempfile.TemporaryDirectory() as tmpdir:
            body_path = os.path.join(tmpdir, "body.json")
            with open(body_path, "w", encoding="utf-8") as f:
                f.write(payload)

            # Auth header still goes through a -K config (not argv), but this
            # config carries no user-controlled content — just the fixed
            # header line — so there's no escaping hazard here.
            config_path = os.path.join(tmpdir, "curl.cfg")
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(f'header = "Authorization: Bearer {NVIDIA_API_KEY}"\n')

            args = [
                "curl",
                "-K", config_path,
                "-sS",
                "-X", "POST",
                url,
                "-H", "Content-Type: application/json",
                "--data-binary", f"@{body_path}",
                "-w", "HTTPSTATUS:%{http_code}",
            ]

            try:
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=NVIDIA_REQUEST_TIMEOUT_S
                )
            except FileNotFoundError as e:
                raise NvidiaClientError(f"curl is not installed or not on PATH: {e}") from e
            except asyncio.TimeoutError as e:
                proc.kill()
                await proc.wait()
                raise NvidiaClientError("curl request timed out") from e

            if proc.returncode != 0:
                raise NvidiaClientError(
                    f"curl exited {proc.returncode}: {stderr.decode(errors='replace')[:200]}"
                )

        output = stdout.decode(errors="replace")
        marker = "HTTPSTATUS:"
        if marker not in output:
            raise NvidiaClientError(f"curl output missing status marker: {output[:200]}")

        body, _, status_str = output.rpartition(marker)
        try:
            status_code = int(status_str.strip())
        except ValueError as e:
            raise NvidiaClientError(f"curl returned non-numeric status: {status_str!r}") from e

        return status_code, body
