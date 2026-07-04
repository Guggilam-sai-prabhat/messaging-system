import asyncio
import json
import logging
import signal
import time

import pypdf
import redis.asyncio as aioredis
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from workers.config import (
    DATABASE_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_POLL_TIMEOUT_S,
    KAFKA_TOPIC,
    MAX_PAGES,
    REDIS_URL,
)

HEARTBEAT_KEY = "worker:document_worker:heartbeat"
HEARTBEAT_TTL_S = 90  # key expires if worker stops; reconciliation checks this
from workers.chunk_repository import ChunkRepository
from workers.chunker import split_text
from workers.embedder import Embedder
from workers.extractor import PDFExtractor
from workers.models import DocumentEvent, ExtractionResult
from workers.repository import DocumentRepository
from workers.storage import StorageClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("document.worker")


class DocumentWorker:
    """
    Main worker loop: poll Kafka → process → commit offset.

    Offset commit discipline
    ------------------------
    We use enable.auto.commit = False. The offset is committed in the
    finally block AFTER processing completes (success or failure).
    This means:
      - success                        → status=ready,            offset committed
      - error before extraction ready  → status=failed,           offset committed
      - error during chunking/embedding
        (extraction already succeeded) → status=embedding_failed, offset committed
      - unhandled crash → finally still runs, status set per the above, offset committed

    The DB updates are idempotent (guarded by the expected prior status),
    so redelivery on restart is safe.
    """

    def __init__(self) -> None:
        self._consumer: Consumer
        self._storage = StorageClient()
        self._repo: DocumentRepository
        self._chunk_repo: ChunkRepository
        self._embedder = Embedder()
        self._running = False
        self._db_engine = None
        self._session_factory: async_sessionmaker
        self._redis: aioredis.Redis | None = None

    async def start(self) -> None:
        await self._init_db()
        self._init_kafka()
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        logger.info("DocumentWorker started")

    async def _init_db(self) -> None:
        db_url = DATABASE_URL
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

        self._db_engine = create_async_engine(
            db_url,
            pool_size=5,
            max_overflow=2,
            pool_recycle=3600,
            echo=False,
        )
        self._session_factory = async_sessionmaker(
            bind=self._db_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        self._repo = DocumentRepository(self._session_factory)
        self._chunk_repo = ChunkRepository(self._session_factory)

        async with self._db_engine.begin() as conn:
            result = await conn.execute(text("SELECT version()"))
            version = result.scalar()
            logger.info(f"PostgreSQL connected: {version[:60]}...")

    def _init_kafka(self) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10_000,
                "heartbeat.interval.ms": 3_000,
                "max.poll.interval.ms": 300_000,
            }
        )
        self._consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to topic={KAFKA_TOPIC} group={KAFKA_GROUP_ID}")

    async def shutdown(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer closed")
        if self._redis:
            await self._redis.delete(HEARTBEAT_KEY)
            await self._redis.aclose()
            logger.info("Redis connection closed")
        if self._db_engine:
            await self._db_engine.dispose()
            logger.info("DB engine disposed")

    async def run(self) -> None:
        self._running = True
        loop = asyncio.get_running_loop()

        while self._running:
            msg = await loop.run_in_executor(
                None,
                self._consumer.poll,
                KAFKA_POLL_TIMEOUT_S,
            )

            if self._redis:
                await self._redis.set(HEARTBEAT_KEY, "alive", ex=HEARTBEAT_TTL_S)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        f"Reached end of partition "
                        f"{msg.topic()}[{msg.partition()}]"
                    )
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                continue

            await self._handle_message(msg)

    async def _handle_message(self, msg) -> None:
        t_start = time.monotonic()
        document_id = "unknown"

        reached_ready = False

        try:
            event = self._parse_event(msg)
            document_id = event.document_id

            logger.info(
                f"Processing document_id={document_id} "
                f"file={event.file_name} "
                f"path={event.storage_path}"
            )

            result = await self._process_document(event)

            await self._repo.mark_ready(
                document_id=document_id,
                extracted_text=result.text,
                page_count=result.page_count,
                truncated=result.truncated,
            )
            reached_ready = True

            if result.truncated:
                logger.warning(
                    f"document_id={document_id} was truncated at "
                    f"{MAX_PAGES} pages (total={result.page_count})"
                )

            await self._embed_and_store_chunks(event, result.text)

            elapsed = (time.monotonic() - t_start) * 1000
            logger.info(
                f"document_id={document_id} processed successfully "
                f"in {elapsed:.0f}ms | pages={result.page_count}"
            )

        except (ValueError, pypdf.errors.PdfReadError, Exception) as e:
            elapsed = (time.monotonic() - t_start) * 1000
            if isinstance(e, ValueError):
                reason = str(e)
                logger.warning(f"document_id={document_id} failed (user error) in {elapsed:.0f}ms: {e}")
            elif isinstance(e, pypdf.errors.PdfReadError):
                reason = f"Corrupted or invalid PDF: {e}"
                logger.warning(f"document_id={document_id} corrupted PDF in {elapsed:.0f}ms: {e}")
            else:
                reason = f"Internal processing error: {type(e).__name__}: {e}"
                logger.exception(f"document_id={document_id} unexpected error in {elapsed:.0f}ms: {e}")
            if document_id != "unknown":
                if reached_ready:
                    # Text extraction already succeeded and status is 'ready';
                    # this failure is in chunking/embedding, not extraction.
                    await self._repo.mark_embedding_failed(document_id, reason)
                else:
                    await self._repo.mark_failed(document_id, reason)

        finally:
            self._consumer.commit(message=msg, asynchronous=False)

    async def _embed_and_store_chunks(
        self, event: DocumentEvent, text: str
    ) -> None:
        """
        Split `text` into overlapping chunks, embed them in one API call,
        then bulk-insert into document_chunks.

        This runs after mark_ready so a document is always accessible even
        if embedding fails (e.g. the model fails to load). If it raises,
        the caller marks the document status='embedding_failed' — it stays
        readable but won't appear in semantic search until reprocessed.
        """
        chunks = split_text(text)
        if not chunks:
            logger.warning(f"document_id={event.document_id} produced no chunks")
            return

        total_words = sum(c.word_count for c in chunks)
        avg_words = total_words // len(chunks)
        logger.info(
            f"document_id={event.document_id} split into {len(chunks)} chunks "
            f"avg_words={avg_words} total_words={total_words}"
        )

        embeddings = await self._embedder.embed_texts(
            [c.content for c in chunks]
        )

        chunk_dicts = [
            {
                "chunk_index": c.chunk_index,
                "content": c.content,
                "embedding": emb,
            }
            for c, emb in zip(chunks, embeddings)
        ]

        await self._chunk_repo.insert_chunks(
            document_id=event.document_id,
            channel_id=event.channel_id,
            chunks=chunk_dicts,
        )

    def _parse_event(self, msg) -> DocumentEvent:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Unparseable Kafka message: {e}") from e

        required = {"documentId", "channelId", "storagePath", "fileName", "uploadedBy"}
        missing = required - payload.keys()
        if missing:
            raise ValueError(f"Kafka payload missing fields: {missing}")

        return DocumentEvent(
            document_id=payload["documentId"],
            channel_id=payload["channelId"],
            storage_path=payload["storagePath"],
            file_name=payload["fileName"],
            uploaded_by=payload["uploadedBy"],
        )

    async def _process_document(self, event: DocumentEvent) -> ExtractionResult:
        loop = asyncio.get_running_loop()

        logger.debug(f"Fetching {event.storage_path} from MinIO")
        pdf_bytes: bytes = await loop.run_in_executor(
            None,
            self._storage.get_object_bytes,
            event.storage_path,
        )

        logger.debug(f"Fetched {len(pdf_bytes):,} bytes for document_id={event.document_id}")

        result: ExtractionResult = await loop.run_in_executor(
            None,
            PDFExtractor.extract,
            pdf_bytes,
        )

        return result


async def main() -> None:
    worker = DocumentWorker()
    await worker.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(sig):
        logger.info(f"Received {signal.Signals(sig).name} — shutting down")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    worker_task = asyncio.create_task(worker.run())
    await stop_event.wait()

    logger.info("Stopping worker...")
    await worker.shutdown()
    worker_task.cancel()
    try:
        await asyncio.wait_for(worker_task, timeout=10.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    logger.info("Worker stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
