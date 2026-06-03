"""
sarvam_engine.py — Persistent Sarvam TTS WebSocket engine + global pool.

SarvamVoiceEngine : one self-healing WebSocket connection with keep-alive ping.
SarvamVoicePool   : queue-based pool of N engines (global, shared across calls).
"""

from __future__ import annotations

import asyncio
import base64
import time
from typing import Optional, AsyncGenerator

from sarvamai import AsyncSarvamAI, AudioOutput, EventResponse


# ── Constants ─────────────────────────────────────────────────────────────────

PING_INTERVAL     = 20   # seconds — well under the 60s Sarvam inactivity timeout
MAX_RETRIES       = 3    # reconnect attempts per speak() call
DEFAULT_POOL_SIZE = 2    # engines in the global pool


# ══════════════════════════════════════════════════════════════════════════════
# SarvamVoiceEngine — one self-healing WebSocket connection
# ══════════════════════════════════

class SarvamVoiceEngine:
    def __init__(
        self,
        api_key:       str,
        language_code: str,
        speaker:       str,
        sample_rate:   int = 8000,
        engine_id:     int = 0,
    ):
        self._api_key       = api_key
        self._language_code = language_code
        self._speaker       = speaker
        self._sample_rate   = sample_rate
        self._engine_id     = engine_id

        self._client:   AsyncSarvamAI          = AsyncSarvamAI(api_subscription_key=api_key)
        self._ws                               = None
        self._ws_ctx                           = None
        self._ping_task: Optional[asyncio.Task] = None
        self._lock      = asyncio.Lock()

        print(
            f"[Engine-{self._engine_id}] Initialized | "
            f"lang={language_code} | speaker={speaker} | rate={sample_rate}Hz"
        )

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        print(f"[Engine-{self._engine_id}] Connecting...")

        self._ws_ctx = self._client.text_to_speech_streaming.connect(
            model="bulbul:v3",
            send_completion_event=True,
        )
        self._ws = await self._ws_ctx.__aenter__()

        await self._ws.configure(
            target_language_code=self._language_code,
            speaker=self._speaker,
            output_audio_codec="linear16",
            speech_sample_rate=self._sample_rate,
            min_buffer_size=30,       # process sooner
            max_chunk_length=150,     
        )
        print(f"[Engine-{self._engine_id}] Connected and configured ✓")

        self._ping_task = asyncio.create_task(self._keep_alive())

    async def disconnect(self) -> None:
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None

        if self._ws_ctx and self._ws:
            try:
                await self._ws_ctx.__aexit__(None, None, None)
            except Exception:
                pass

        self._ws     = None
        self._ws_ctx = None
        print(f"[Engine-{self._engine_id}] Disconnected.")

    async def ensure_connected(self) -> bool:
        if self.is_connected:
            return True
        print(f"[Engine-{self._engine_id}] ensure_connected: reconnecting...")
        try:
            await self._reconnect()
            return True
        except Exception as e:
            print(f"[Engine-{self._engine_id}] ensure_connected failed: {e}")
            return False

    async def _reconnect(self) -> None:
        print(f"[Engine-{self._engine_id}] Reconnecting...")
        await self.disconnect()
        await self.connect()

    # ── Keep-Alive ────────────────────────────────────────────────────────────

    async def _keep_alive(self) -> None:

        while True:
            await asyncio.sleep(PING_INTERVAL)
            if self._ws is None:
                break
            try:
                await self._ws._websocket.send('{"type": "ping"}')
                print(f"[Engine-{self._engine_id}] Ping ✓")
            except Exception as e:
                print(f"[Engine-{self._engine_id}] Ping failed — marking dead: {e}")
                self._ws     = None
                self._ws_ctx = None
                break

    # ── TTS ───────────────────────────────────────────────────────────────────

    async def speak(self, text: str) -> AsyncGenerator[bytes, None]:

        text = text.strip()
        if not text:
            return

        async with self._lock:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # _keep_alive nulls out self._ws when ping fails,
                    # so this check is sufficient to catch dead connections.
                    if self._ws is None:
                        await self.connect()

                    t0 = time.perf_counter()
                    await self._ws.convert(text)
                    await self._ws.flush()

                    chunk_count = 0
                    async for message in self._ws:
                        if isinstance(message, AudioOutput):
                            chunk_count += 1
                            if chunk_count == 1:
                                print(
                                    f"[Engine-{self._engine_id}] First chunk "
                                    f"{time.perf_counter() - t0:.3f}s | attempt {attempt}"
                                )
                            yield base64.b64decode(message.data.audio)

                        elif isinstance(message, EventResponse):
                            if message.data.event_type == "final":
                                break  # utterance done — connection stays open

                    return  # success

                except Exception as e:
                    print(f"[Engine-{self._engine_id}] speak() error (attempt {attempt}): {e}")
                    try:
                        await self.disconnect()
                    except Exception:
                        pass
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(1.0 * attempt)
                    else:
                        print(f"[Engine-{self._engine_id}] All retries exhausted.")
                        return

    # ── Health ────────────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        try:
            return not self._ws._websocket.closed
        except Exception:
            return False

    def __repr__(self) -> str:
        return (
            f"<SarvamVoiceEngine id={self._engine_id} "
            f"lang={self._language_code} connected={self.is_connected}>"
        )


# ══════════════════════════════════════════════════════════════════════════════
# SarvamVoicePool — global pool of N engines, shared across all calls
# ══════════════════════════════════════════════════════════════════════════════

class SarvamVoicePool:
    """
    Queue-based pool of persistent SarvamVoiceEngine connections.

    - Each engine has its own WebSocket + independent ping loop.
    - borrow() blocks until an engine is available.
    - release() returns the engine to the bottom of the queue (round-robin).
    - Used by the global engine pool in instance_cache / tts.py.
    """

    def __init__(
        self,
        api_key:       str,
        language_code: str,
        speaker:       str,
        sample_rate:   int = 8000,
        size:          int = DEFAULT_POOL_SIZE,
    ):
        self._api_key       = api_key
        self._language_code = language_code
        self._speaker       = speaker
        self._sample_rate   = sample_rate
        self._size          = size

        self._queue:       asyncio.Queue[SarvamVoiceEngine] = asyncio.Queue()
        self._all_engines: list[SarvamVoiceEngine]          = []

    async def prewarm(self) -> None:
        """Create all engines, connect them, and fire a warm-up utterance."""
        print(f"[SarvamVoicePool] Prewarming {self._size} engines...")

        engines = [
            SarvamVoiceEngine(
                api_key=self._api_key,
                language_code=self._language_code,
                speaker=self._speaker,
                sample_rate=self._sample_rate,
                engine_id=i + 1,
            )
            for i in range(self._size)
        ]

        results = await asyncio.gather(
            *[e.connect() for e in engines],
            return_exceptions=True,
        )

        warmup_tasks = []
        for engine, result in zip(engines, results):
            if isinstance(result, Exception):
                print(
                    f"[SarvamVoicePool] Engine-{engine._engine_id} "
                    f"failed to connect: {result}"
                )
            else:
                self._all_engines.append(engine)
                warmup_tasks.append(self._warmup_engine(engine))

        await asyncio.gather(*warmup_tasks, return_exceptions=True)

        # Only enqueue engines that survived warmup
        for engine in self._all_engines:
            await self._queue.put(engine)

        ready = self._queue.qsize()
        print(f"[SarvamVoicePool] Pool ready — {ready}/{self._size} engines ✓")

        if ready == 0:
            raise RuntimeError("[SarvamVoicePool] No engines connected — check API key.")

    async def _warmup_engine(self, engine: SarvamVoiceEngine) -> None:
        """
        Send a short silent utterance to prime the Sarvam TTS pipeline.
        Audio is discarded — we only care about exercising the cold path.
        """
        WARMUP_TEXT = "hello"
        try:
            print(f"[Engine-{engine._engine_id}] Warming up...")
            async for _ in engine.speak(WARMUP_TEXT):
                pass  # drain and discard
            print(f"[Engine-{engine._engine_id}] Warm-up done ✓")
        except Exception as e:
            print(f"[Engine-{engine._engine_id}] Warm-up failed (non-fatal): {e}")

    async def shutdown(self) -> None:
        """Disconnect all engines cleanly."""
        print("[SarvamVoicePool] Shutting down...")
        while not self._queue.empty():
            engine = self._queue.get_nowait()
            await engine.disconnect()
        self._all_engines.clear()
        print("[SarvamVoicePool] All engines shut down.")

    async def borrow(self) -> SarvamVoiceEngine:
        """Get a warm engine. Blocks until one is available."""
        return await self._queue.get()

    async def release(self, engine: SarvamVoiceEngine) -> None:
        """Return engine to the bottom of the queue (round-robin)."""
        await self._queue.put(engine)

    @property
    def available(self) -> int:
        return self._queue.qsize()

    @property
    def total(self) -> int:
        return len(self._all_engines)
    

