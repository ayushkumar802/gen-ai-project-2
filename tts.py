from __future__ import annotations

import asyncio
import base64
import json
import re
import time
import random
import struct
from reason_prompts import fall_back, agent_fallback
from network_moniter import ChunkArrivalMonitor
from langchain_core.messages import HumanMessage, AIMessage


# ─────────────────────────────────────────────
# Audio constants — single source of truth
# ─────────────────────────────────────────────
SAMPLE_RATE       = 8000
BYTES_PER_SAMPLE  = 2
BYTES_PER_SEC     = SAMPLE_RATE * BYTES_PER_SAMPLE   # 16_000
FRAME_DURATION_MS = 20
FRAME_SIZE        = BYTES_PER_SEC * FRAME_DURATION_MS // 1000  # 320 bytes

# Buffer thresholds for consumer state machine (bytes)
BUFFER_HEALTHY_BYTES = 8000    # ~500ms
BUFFER_MEDIUM_BYTES  = 3200    # ~200ms

# Micro-wait before declaring starvation (spec: 50-100ms)
STARVATION_WAIT_MS   = 80

# FIX-3: Reduced pre-roll — 2 chunks (~275ms) is enough to smooth burst delivery
INITIAL_PREROLL_BYTES = 800  # was 8_000

# Default mini-buffer before ChunkArrivalMonitor has enough samples (< 3 gaps)
MINI_BUFFER_DEFAULT   = 3_200   # ~400ms

MAX_CONCURRENT_FETCHES = 2
MIN_AUDIO_CHUNK        = 100
FIRST_CHUNK_TIMEOUT    = 5.0
MAX_FETCH_WAIT         = 4.0
STREAM_THRESHOLD_FAST  = 2200  # nominal chunk size from Sarvam

PCM_QUEUE_MAXSIZE = 48

# Cap frame_ready_queue depth to ~1s of audio to prevent pre-buffering
# all sentences at once and making interruption laggy.
FRAME_QUEUE_MAXSIZE = 50

R="\033[91m"; G="\033[92m"; Y="\033[93m"; B="\033[94m"
M="\033[95m"; C="\033[96m"; DIM="\033[2m"; RST="\033[0m"

T0: float = 0.0
def ts() -> str:
    return f"{time.perf_counter()-T0:7.3f}s"


# ─────────────────────────────────────────────
# Sentence splitting
# ─────────────────────────────────────────────
SENTENCE_END = re.compile(r'(?<![0-9])(?<=[—।.!?])\s+')


def split_into_sentences(text: str) -> list[str]:
    parts = SENTENCE_END.split(text.strip())
    return [p.strip() for p in parts if p.strip()]


def extract_complete_sentences(buffer: str) -> tuple[list[str], str]:
    match = None
    for m in re.finditer(r'[।.!?]', buffer):
        match = m
    if match is None:
        return [], buffer
    complete_part = buffer[:match.end()].strip()
    leftover      = buffer[match.end():].strip()
    return split_into_sentences(complete_part), leftover


# ─────────────────────────────────────────────
# Comfort Noise Configuration (Global)
# ─────────────────────────────────────────────
# Amplitude: 150-250 is a subtle background floor (-45dB)
CN_AMPLITUDE = 250

# Buffer: 20 seconds. Even with 10 concurrent calls, this loops every 2s.
# This prevents audible repetitive patterns.
CN_BUFFER_SEC = 20

# Generate the raw PCM buffer once at startup
# 8000 samples/sec * 20 sec * 2 bytes/sample = 320,000 bytes (~320KB RAM)
print("[Init] Generating Comfort Noise buffer...")
_cn_buffer = bytearray()
for _ in range(SAMPLE_RATE * CN_BUFFER_SEC):
    sample = random.randint(-CN_AMPLITUDE, CN_AMPLITUDE)
    _cn_buffer.extend(struct.pack('<h', sample))

_CN_NOISE = bytes(_cn_buffer)
_cn_index = 0

def get_comfort_noise(num_bytes: int) -> bytes:
    """
    Returns `num_bytes` of comfort noise.
    Safe for concurrent use.
    """
    global _cn_index

    # CRITICAL FIX: Ensure we always request an even number of bytes
    # to maintain 16-bit PCM alignment. If odd, request 1 byte less.
    if num_bytes % 2 != 0:
        num_bytes -= 1

    if num_bytes <= 0:
        return b''

    buffer_len = len(_CN_NOISE)

    current_idx = _cn_index

    start_idx = current_idx % buffer_len
    end_idx = (start_idx + num_bytes) % buffer_len

    if end_idx > start_idx:
        chunk = _CN_NOISE[start_idx:end_idx]
    else:
        chunk = _CN_NOISE[start_idx:] + _CN_NOISE[:end_idx]

    _cn_index = (start_idx + num_bytes) % buffer_len

    return chunk


# ─────────────────────────────────────────────
# Garbage filter
# ─────────────────────────────────────────────
GARBAGE_PATTERNS = [
    "(waiting", "(loading", "(thinking", "(processing",
    "[waiting", "[loading", "[thinking", "...", "___",
]


def is_garbage_response(text: str) -> bool:
    return any(p in text.strip().lower() for p in GARBAGE_PATTERNS)


def get_pool(pool_source, speaker: str):
    """
    pool_source: either a dict {speaker: SarvamVoicePool} or a single SarvamVoicePool
    speaker:     the desired speaker name
    Returns the right SarvamVoicePool.
    """
    if isinstance(pool_source, dict):
        if speaker in pool_source:
            return pool_source[speaker]
        # fallback to first available
        return next(iter(pool_source.values()))
    # legacy: raw pool passed directly
    return pool_source


# ─────────────────────────────────────────────
# Real-time streaming pipeline
# ─────────────────────────────────────────────

class _PcmSentinel:
    pass


_PCM_DONE = _PcmSentinel()


class _TtsEnd:
    """Carries total sentence count to the fetcher."""
    def __init__(self, total: int):
        self.total = total

class ExotelFrameQueue:
    def __init__(self):
        self.pcm_queue: asyncio.Queue = asyncio.Queue(maxsize=PCM_QUEUE_MAXSIZE)
        self.frame_ready_queue: asyncio.Queue       = asyncio.Queue(maxsize=FRAME_QUEUE_MAXSIZE)
        self.producer_task:     asyncio.Task | None = None
        self.sender_task:       asyncio.Task | None = None
        self.producer_finished: asyncio.Event       = asyncio.Event()


# ─────────────────────────────────────────────
# TASK 1: PCM PRODUCER
# ─────────────────────────────────────────────
async def _exotel_producer_worker(
        fq:         ExotelFrameQueue,
        frame_ready_queue: asyncio.Queue,
        stop_event: asyncio.Event,
) -> None:
    carry = bytearray()
    done  = False

    async def _drain_queue():
        nonlocal done
        while True:
            try:
                item = fq.pcm_queue.get_nowait(); fq.pcm_queue.task_done()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, _PcmSentinel): done = True; break
            carry.extend(item)

    while True:
        if stop_event.is_set():
            # FIX-4: clear carry immediately so frame loop doesn't process stale data
            carry.clear()
            discarded = fq.pcm_queue.qsize()
            while not fq.pcm_queue.empty():
                try: fq.pcm_queue.get_nowait(); fq.pcm_queue.task_done()
                except asyncio.QueueEmpty: break
            if discarded: print(f"{R}[Producer] stop_event — discarded {discarded}{RST}")
            break

        await _drain_queue()

        if len(carry) < FRAME_SIZE and not done:
            try:
                item = await asyncio.wait_for(
                    fq.pcm_queue.get(), timeout=STARVATION_WAIT_MS / 1000
                )
                fq.pcm_queue.task_done()
                if isinstance(item, _PcmSentinel): done = True
                else: carry.extend(item)
                await _drain_queue()
            except asyncio.TimeoutError:
                pass

        while len(carry) >= FRAME_SIZE:
            if stop_event.is_set():
                carry.clear()
                break
            frame = bytes(carry[:FRAME_SIZE]); del carry[:FRAME_SIZE]
            try:
                frame_ready_queue.put_nowait(frame)
            except asyncio.QueueFull:
                await asyncio.sleep(0)
                if stop_event.is_set():
                    carry.clear()
                    break
                await frame_ready_queue.put(frame)

        if done:
            if carry:
                padding_size = FRAME_SIZE - len(carry)
                frame = bytes(carry) + get_comfort_noise(padding_size)
                await frame_ready_queue.put(frame)
                carry.clear()

            fq.producer_finished.set()

            await frame_ready_queue.put(_PCM_DONE)
            print(f"{C}[Producer]  done — flushed all frames  t={ts()}{RST}")
            break


# ─────────────────────────────────────────────
# TASK 2: EXOTEL SENDER
# ─────────────────────────────────────────────
async def _exotel_sender_worker(
        frame_ready_queue: asyncio.Queue,
        websocket,
        stream_sid:        str,
        stop_event:        asyncio.Event,
        chunk_monitor:     ChunkArrivalMonitor,
        frame_queue:       ExotelFrameQueue
) -> None:
    BURST_FRAMES   = 5
    BURST_DURATION = FRAME_DURATION_MS * BURST_FRAMES / 1000   # 0.1s

    was_starved   = False
    starvation_ms = 0.0
    frame_num     = 0
    loop          = asyncio.get_event_loop()
    next_tick     = loop.time()

    while True:
        if stop_event.is_set():
            print(f"{R}[Sender]  stop_event — exiting  frames={frame_num}{RST}")
            break

        burst          = bytearray()
        done           = False
        missing_frames = 0

        for _ in range(BURST_FRAMES):
            try:
                item = frame_ready_queue.get_nowait()
                frame_ready_queue.task_done()
                if isinstance(item, _PcmSentinel):
                    done = True
                    break
                burst.extend(item)
                frame_num += 1
            except asyncio.QueueEmpty:
                if frame_queue.producer_finished.is_set():
                    filler = bytes(FRAME_SIZE)
                else:
                    filler = get_comfort_noise(FRAME_SIZE)
                missing_frames += 1
                burst.extend(filler)
                frame_num += 1

        starved_this_burst = missing_frames > 0

        if not frame_queue.producer_finished.is_set():
            if chunk_monitor is not None and chunk_monitor.is_stalled():
                print(f"{R}[Sender]  ⚠ TTS SOURCE STALLED (no chunks for 600ms)  t={ts()}{RST}")

            if starved_this_burst:
                starvation_ms += missing_frames * FRAME_DURATION_MS
                if not was_starved:
                    print(f"{R}[Sender]  ⚠ STARVED  frame={frame_num}  t={ts()}{RST}")
                    was_starved = True
            elif was_starved:
                print(f"{G}[Sender]  ✓ REFILLED  silence={starvation_ms:.0f}ms  t={ts()}{RST}")
                was_starved   = False
                starvation_ms = 0.0

        if burst:
            payload = base64.b64encode(bytes(burst)).decode()
            await websocket.send_text(json.dumps({
                "event":      "media",
                "stream_sid": stream_sid,
                "media":      {"payload": payload},
            }))

        if done:
            print(f"{C}[Sender]  producer done — exiting  frames={frame_num}  t={ts()}{RST}")
            break

        next_tick += BURST_DURATION
        now  = loop.time()
        wait = next_tick - now
        if wait > 0.0005:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait)
                print(f"{R}[Sender] stop_event during sleep — exiting{RST}")
                break
            except asyncio.TimeoutError:
                pass  # normal path, continue
        elif wait < -0.010:
            print(f"{R}[Sender]  ⚠ behind by {-wait*1000:.1f}ms, resyncing  t={ts()}{RST}")
            next_tick = loop.time() + BURST_DURATION


# ─────────────────────────────────────────────
# Sarvam TTS → pcm_queue producer
# ─────────────────────────────────────────────
async def _stream_chunks_to_queue(
        audio_gen, pcm_out_queue, stop_event, s_num, s_start,
        ready_event, label="", chunk_monitor=None,
        sentence=None, pool=None,
) -> int:
    raw_total = 0; chunk_count = 0
    MIN_EXPECTED_BYTES = 6400   # ~400ms minimum for any real sentence

    try:
        async for chunk in audio_gen:
            if stop_event.is_set(): break
            if len(chunk) < MIN_AUDIO_CHUNK: continue
            chunk_count += 1; raw_total += len(chunk)
            if chunk_monitor is not None:
                chunk_monitor.record_chunk()
            if chunk_count == 1:
                print(f"{B}[TTS #{s_num}]  first chunk  {len(chunk)}B  t={ts()}{label}{RST}")
                ready_event.set()
            await pcm_out_queue.put(bytes(chunk))
    except asyncio.CancelledError: raise
    except Exception as e: print(f"{R}[TTS #{s_num}]  error: {e}{RST}")

    # FIX-5: guard stop_event before borrow (borrow can block if pool is empty)
    if raw_total > 0 and raw_total < MIN_EXPECTED_BYTES and sentence and pool \
            and not stop_event.is_set():
        print(f"{R}[TTS #{s_num}]  ✗ truncated {raw_total}B — retrying{RST}")
        try:
            engine = await pool.borrow()
            # double-check after potentially blocking borrow
            if stop_event.is_set():
                await pool.release(engine)
                return raw_total
            ok = await engine.ensure_connected()
            if ok:
                try:
                    async for chunk in engine.speak(sentence):
                        if stop_event.is_set(): break
                        if len(chunk) < MIN_AUDIO_CHUNK: continue
                        raw_total += len(chunk)
                        if chunk_monitor is not None:
                            chunk_monitor.record_chunk()
                        await pcm_out_queue.put(bytes(chunk))
                    print(f"{B}[TTS #{s_num}]  retry done  total={raw_total}B{RST}")
                finally:
                    await pool.release(engine)
            else:
                await pool.release(engine)
        except Exception as e:
            print(f"{R}[TTS #{s_num}]  retry error: {e}{RST}")

    if raw_total:
        print(f"{B}[TTS #{s_num}]  fetch done  total={raw_total}B  chunks={chunk_count}  t={ts()}{RST}")
    return raw_total


# ─────────────────────────────────────────────
# Core TTS engine
# ─────────────────────────────────────────────
async def _run_tts_engine(
        tts_queue:      asyncio.Queue,
        stream_sid:     str,
        websocket,
        stop_event:     asyncio.Event,
        sarvam_client,
        language_code:  str = "hi-IN",
        speaker:        str = "shubh",
        end_call_event: asyncio.Event = None,
        pool=None,
        initial_preroll_bytes: int = INITIAL_PREROLL_BYTES,
        conversation_history: list[HumanMessage | AIMessage] | None = None,
) -> tuple[int, list[str]]:

    from instance_cache import get_cached_sync

    if pool is None:
        pool_source = get_cached_sync("engine_pool")
        if pool_source is None:
            print("[TTS ENGINE] ✗ No engine source available — cannot run TTS")
            return 0, []
        pool = get_pool(pool_source, speaker)
        print(f"[TTS ENGINE] Using global engine pool for speaker='{speaker}'")
    elif isinstance(pool, dict):
        pool = get_pool(pool, speaker)
        print(f"[TTS ENGINE] Using per-call local prewarm pool for speaker='{speaker}' ✓")
    else:
        print("[TTS ENGINE] Using per-call local prewarm pool ✓")

    total_pcm_sent   = 0
    spoken_sentences = []
    semaphore        = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    play_queue       = asyncio.Queue()

    chunk_monitor = ChunkArrivalMonitor(bytes_per_sec=BYTES_PER_SEC)
    frame_queue   = ExotelFrameQueue()

    # FIX-1: always define watch_task so finally block never hits NameError
    if end_call_event is not None:
        async def _watch_end_call():
            await end_call_event.wait()
            stop_event.set()
        watch_task = asyncio.create_task(_watch_end_call())
    else:
        watch_task = asyncio.create_task(asyncio.sleep(0))  # no-op sentinel

    sender_started_event = asyncio.Event()

    frame_queue.producer_task = asyncio.create_task(
        _exotel_producer_worker(frame_queue, frame_queue.frame_ready_queue, stop_event)
    )

    async def _deferred_sender():
        print(f"{Y}[Sender]  waiting for first TTS chunk before starting clock...{RST}")
        await sender_started_event.wait()
        if stop_event.is_set():
            print(f"{R}[Sender]  stop_event before start — aborting{RST}")
            return
        while frame_queue.frame_ready_queue.qsize() < 4 and not stop_event.is_set():
            await asyncio.sleep(0.005)
        print(f"{G}[Sender]  frames ready — starting 20ms clock  t={ts()}{RST}")
        await _exotel_sender_worker(
            frame_queue.frame_ready_queue, websocket, stream_sid, stop_event, chunk_monitor, frame_queue
        )

    frame_queue.sender_task = asyncio.create_task(_deferred_sender())

    # ── fetcher ───────────────────────────────────────────────────────────────
    async def fetcher():
        s_num = 0
        total_sentences = None
        while True:
            sentence = await tts_queue.get(); tts_queue.task_done()
            if isinstance(sentence, _TtsEnd):
                total_sentences = sentence.total
                continue

            if sentence is None:
                await play_queue.put((None, None, None, None, total_sentences))
                break

            if stop_event.is_set():
                while True:
                    s = await tts_queue.get(); tts_queue.task_done()
                    if s is None: break
                await play_queue.put((None, None, None, None, None)); break

            s_num      += 1
            s_start     = time.perf_counter()
            pcm_chunk_q = asyncio.Queue()
            ready_event = asyncio.Event()

            async def _fetch_and_release(
                sent=sentence, num=s_num, start=s_start,
                q=pcm_chunk_q, ready=ready_event,
            ):
                await semaphore.acquire()
                try:
                    engine = await pool.borrow()
                    ok = await engine.ensure_connected()
                    if not ok:
                        await pool.release(engine)
                        print(f"{R}[TTS #{num}]  engine reconnect failed — skipping{RST}")
                        return
                    try:
                        audio_gen = engine.speak(sent)
                        await _stream_chunks_to_queue(
                            audio_gen, q, stop_event, num, start, ready,
                            label=" [pool]", chunk_monitor=chunk_monitor,
                            sentence=sent, pool=pool,
                        )
                    finally:
                        if stop_event.is_set():
                            print(f"{Y}[TTS #{num}]  interrupted — reconnecting engine before pool release{RST}")
                            try:
                                await engine._reconnect()
                                print(f"{G}[TTS #{num}]  engine reconnected cleanly ✓{RST}")
                            except Exception as e:
                                print(f"{R}[TTS #{num}]  engine reconnect failed: {e} — releasing as-is{RST}")
                        await pool.release(engine)

                except asyncio.CancelledError: raise
                except Exception as e: print(f"{R}[TTS #{num}]  fetch error: {e}{RST}")
                finally:
                    await q.put(None); semaphore.release()

            fetch_task = asyncio.create_task(_fetch_and_release())
            await play_queue.put((sentence, pcm_chunk_q, fetch_task, ready_event, None))

    fetcher_task = asyncio.create_task(fetcher())

    # ── helpers ───────────────────────────────────────────────────────────────
    async def drain_play_queue():
        while True:
            try:
                _, q, ft, _, _ = play_queue.get_nowait()
                if ft: ft.cancel()
                try: await ft
                except: pass
            except asyncio.QueueEmpty: break

    # FIX-2: lightweight _put_or_stop — only creates futures when queue is actually full
    async def _put_or_stop(queue: asyncio.Queue, item, stop_event: asyncio.Event):
        """Put into queue but abort immediately if stop_event fires."""
        if stop_event.is_set():
            return False
        try:
            queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            pass
        # Only reach here when queue is genuinely full — create tasks then
        put_task  = asyncio.ensure_future(queue.put(item))
        stop_task = asyncio.ensure_future(stop_event.wait())
        done, pending = await asyncio.wait(
            [put_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        if stop_task in done:
            put_task.cancel()
            return False
        return True

    async def _shutdown_sender(graceful=True):
        async def _flush_frame_queue(fq: ExotelFrameQueue):
            """Drain all pending frames so sender exits immediately."""
            cleared = 0
            while True:
                try:
                    fq.frame_ready_queue.get_nowait()
                    fq.frame_ready_queue.task_done()
                    cleared += 1
                except asyncio.QueueEmpty:
                    break
            if cleared:
                print(f"{R}[Flush] Discarded {cleared} frames ({cleared * FRAME_DURATION_MS}ms){RST}")
            try:
                fq.frame_ready_queue.put_nowait(_PCM_DONE)
            except asyncio.QueueFull:
                pass

        if graceful:
            try: frame_queue.pcm_queue.put_nowait(_PCM_DONE)
            except: pass
        else:
            await _flush_frame_queue(frame_queue)

        if frame_queue.producer_task and not frame_queue.producer_task.done():
            pcm_queued    = frame_queue.pcm_queue.qsize() * STREAM_THRESHOLD_FAST
            frames_queued = frame_queue.frame_ready_queue.qsize() * FRAME_SIZE
            total_bytes   = pcm_queued + frames_queued
            drain_timeout = max(total_bytes / BYTES_PER_SEC + 5.0, 10.0)
            print(f"{Y}[Sender]  draining  timeout={drain_timeout:.1f}s  "
                f"(pcm={pcm_queued}B  frames={frames_queued}B){RST}")
            try:
                await asyncio.wait_for(frame_queue.producer_task, timeout=drain_timeout)
            except asyncio.TimeoutError:
                print(f"{R}[Producer]  drain timeout — force cancel{RST}")
                frame_queue.producer_finished.set() 
                frame_queue.producer_task.cancel() 
                try: await frame_queue.producer_task
                except: pass

        if frame_queue.sender_task and not frame_queue.sender_task.done():
            frames_remaining = frame_queue.frame_ready_queue.qsize()
            sender_timeout   = max(frames_remaining * FRAME_DURATION_MS / 1000 + 5.0, 2.0)
            print(f"{Y}[Sender]  waiting for sender  timeout={sender_timeout:.1f}s{RST}")
            try:
                await asyncio.wait_for(frame_queue.sender_task, timeout=sender_timeout)
            except asyncio.TimeoutError:
                print(f"{R}[Sender]  drain timeout — force cancel{RST}")
                frame_queue.sender_task.cancel()
                try: await frame_queue.sender_task
                except: pass


    # ── player ────────────────────────────────────────────────────────────────
    play_num       = 0
    first_sentence = True
    next_item      = None

    try:
        while True:
            if next_item is not None:
                sentence, pcm_chunk_q, fetch_task, ready_event, total_sentences = next_item; next_item = None
            else:
                sentence, pcm_chunk_q, fetch_task, ready_event, total_sentences = await play_queue.get()

            if sentence is None: break

            if stop_event.is_set():
                fetch_task.cancel()
                try: await fetch_task
                except: pass
                await drain_play_queue()
                break

            play_num += 1
            print(f"\n{Y}[Player #{play_num}]  \"{sentence[:60]}\"  t={ts()}{RST}")

            try:
                await asyncio.wait_for(ready_event.wait(), timeout=FIRST_CHUNK_TIMEOUT)
            except asyncio.TimeoutError:
                print(f"{R}[Player #{play_num}]  ✗ ready_event timeout — skipping{RST}")
                fetch_task.cancel()
                try: await fetch_task
                except: pass
                first_sentence = False; continue

            if first_sentence and not sender_started_event.is_set():
                sender_started_event.set()

            sentence_pcm_bytes = 0
            preroll_buf  = bytearray() if first_sentence else None
            preroll_done = not first_sentence
            mini_buf     = bytearray()
            chunk_monitor.reset_timer()

            while True:
                if stop_event.is_set(): break
                try:
                    pcm_chunk = await asyncio.wait_for(
                        pcm_chunk_q.get(), timeout=MAX_FETCH_WAIT + 2.0
                    )
                except asyncio.TimeoutError:
                    print(f"{R}[Player #{play_num}]  chunk stall — finishing{RST}")
                    if mini_buf:
                        if not await _put_or_stop(frame_queue.pcm_queue, bytes(mini_buf), stop_event):
                            break
                        mini_buf.clear()
                        await asyncio.sleep(0)
                    break

                if pcm_chunk is None:
                    if not preroll_done and preroll_buf:
                        pcm_chunk    = bytes(preroll_buf); preroll_buf.clear()
                        preroll_done = True; first_sentence = False
                        mini_buf     = bytearray()
                    else:
                        if mini_buf:
                            if not await _put_or_stop(frame_queue.pcm_queue, bytes(mini_buf), stop_event):
                                break
                            mini_buf.clear()

                        # detect last sentence
                        is_last = (
                            total_sentences is not None
                            and play_num >= total_sentences
                        )
                        if is_last and not stop_event.is_set():
                            try:
                                frame_queue.pcm_queue.put_nowait(_PCM_DONE)
                                print(f"{C}[Player #{play_num}]  last sentence → _PCM_DONE  t={ts()}{RST}")
                            except asyncio.QueueFull:
                                pass
                        break

                if not preroll_done:
                    preroll_buf.extend(pcm_chunk)
                    print(f"{M}[Player #{play_num}]  pre-roll {len(preroll_buf)}B / {initial_preroll_bytes}B{RST}")
                    if len(preroll_buf) < initial_preroll_bytes:
                        continue
                    pre_bytes    = bytes(preroll_buf); preroll_buf.clear()
                    preroll_done = True; first_sentence = False
                    mini_buf     = bytearray()
                    print(f"{M}[Player #{play_num}]  ✓ pre-roll HIT — flushing {len(pre_bytes)}B  t={ts()}{RST}")
                    sentence_pcm_bytes += len(pre_bytes)
                    if not await _put_or_stop(frame_queue.pcm_queue, pre_bytes, stop_event):
                        break
                    continue

                mini_buf.extend(pcm_chunk)
                sentence_pcm_bytes += len(pcm_chunk)

                _in_tail = fetch_task.done() and sentence_pcm_bytes > initial_preroll_bytes
                threshold = STREAM_THRESHOLD_FAST if _in_tail else chunk_monitor.mini_buffer_bytes
                if len(mini_buf) >= threshold:
                    print(
                        f"{C}[Player #{play_num}]  pushed {len(mini_buf)}B "
                        f"(thr={threshold}B | {chunk_monitor.summary()})  "
                        f"total={sentence_pcm_bytes}B  t={ts()}{RST}"
                    )
                    if not await _put_or_stop(frame_queue.pcm_queue, bytes(mini_buf), stop_event):
                        break
                    mini_buf.clear()

            if not fetch_task.done():
                fetch_task.cancel()
                try: await fetch_task
                except: pass

            if sentence_pcm_bytes == 0:
                print(f"{R}[Player #{play_num}]  ✗ zero audio — skipping{RST}")
                first_sentence = False; continue

            spoken_sentences.append(sentence)
            
            if conversation_history is not None:
                conversation_history.append({"role": "assistant", "content": sentence})

            total_pcm_sent += sentence_pcm_bytes
            print(
                f"{G}[Player #{play_num}]  ✓ done  "
                f"audio={sentence_pcm_bytes / BYTES_PER_SEC * 1000:.0f}ms  "
                f"monitor=({chunk_monitor.summary()})  t={ts()}{RST}"
            )
            first_sentence = False

            # FIX-3: pre-fetch next sentence's ready_event as a background task
            # so by the time we consume it, it may already be set (no gap)
            if not stop_event.is_set():
                try:
                    next_item = play_queue.get_nowait()
                    if next_item[0] is not None:
                        _, _, _, nr, _ = next_item
                        if not nr.is_set():
                #             asyncio.ensure_future(nr.wait())
                # except asyncio.QueueEmpty: pass
                            await asyncio.wait_for(nr.wait(), timeout=FIRST_CHUNK_TIMEOUT)
                except (asyncio.QueueEmpty, asyncio.TimeoutError): pass

    except asyncio.CancelledError:
        fetcher_task.cancel()
        try: await fetcher_task
        except: pass
        await drain_play_queue(); raise

    finally:
        # if sender was never started (e.g. TTS failed entirely),
        # unblock the deferred sender so it can exit cleanly.
        sender_started_event.set()

        if not stop_event.is_set():
            try: frame_queue.pcm_queue.put_nowait(_PCM_DONE)
            except: pass

        chunk_monitor.reset_timer()

        if stop_event.is_set():
            await _shutdown_sender(graceful=False)
        else:
            await _shutdown_sender(graceful=True)

        if not fetcher_task.done():
            fetcher_task.cancel()
            try: await fetcher_task
            except: pass

        if not watch_task.done():
            watch_task.cancel()
            try: await watch_task
            except: pass

    return total_pcm_sent, spoken_sentences


# ─────────────────────────────────────────────
# Public API — unchanged signatures
# ─────────────────────────────────────────────

async def tts_worker(
        tts_queue:      asyncio.Queue,
        stream_sid:     str,
        websocket,
        sarvam_client,
        stop_event:     asyncio.Event,
        language_code:  str = "hi-IN",
        speaker:        str = "shubh",
        end_call_event: asyncio.Event = None,
        pool=None,
        initial_preroll_bytes: int = INITIAL_PREROLL_BYTES,
        conversation_history: list[HumanMessage | AIMessage] | None = None
) -> tuple[int, list[str]]:
    print(f"\n{'='*55}")
    print(f"[TTS WORKER] Started | lang={language_code} | speaker={speaker}")
    print(f"{'='*55}")
    return await _run_tts_engine(
        tts_queue, stream_sid, websocket, stop_event,
        sarvam_client, language_code, speaker, end_call_event,
        pool=pool, initial_preroll_bytes=initial_preroll_bytes, conversation_history= conversation_history,
    )


tts_worker_pipelined        = tts_worker
tts_worker_with_existing_ws = tts_worker


async def play_system_message(
        text:           str,
        stream_sid:     str,
        websocket,
        client,
        language_code:  str = "hi-IN",
        speaker:        str = "shubh",
        stop_event:     asyncio.Event = None,
        end_call_event: asyncio.Event = None,
) -> None:
    print(f"[System] Playing: \"{text[:80]}\"")
    try:
        queue    = asyncio.Queue()
        stop_evt = stop_event if stop_event is not None else asyncio.Event()
        await queue.put(text)
        await queue.put(_TtsEnd(1))  
        await queue.put(None)
        await tts_worker(
            queue, stream_sid, websocket, client,
            stop_evt, language_code, speaker, end_call_event,
            pool=None,conversation_history=None,
        )
    except Exception as e:
        print(f"[System] play_system_message error: {e}")


from instance_cache import get_cached_sync


async def send_greeting_audio(
        greeting_text:  str,
        stream_sid:     str,
        websocket,
        sarvam_client,
        prewarm_tts_ws=None,
        language_code:  str = "hi-IN",
        speaker:        str = "shubh",
        end_call_event: asyncio.Event = None,
        pool=None,
        sarvam_api_key: str = None,
) -> float:
    sentences = split_into_sentences(greeting_text)
    if not sentences:
        sentences = [greeting_text]

    if pool is not None:
        pool._connect_task = asyncio.create_task(pool.prewarm())
        print("[Greeting] Local prewarm pool connecting in background...")

    greeting_pool_source = get_cached_sync("engine_pool")
    greeting_pool = get_pool(greeting_pool_source, speaker) if greeting_pool_source else None

    tts_queue = asyncio.Queue()
    stop_evt  = asyncio.Event()
    for s in sentences:
        await tts_queue.put(s)
    await tts_queue.put(_TtsEnd(len(sentences)))
    await tts_queue.put(None)

    t_start = asyncio.get_event_loop().time()
    total_pcm, _ = await tts_worker(
        tts_queue, stream_sid, websocket, sarvam_client,
        stop_evt, language_code, speaker, end_call_event,
        pool=greeting_pool, initial_preroll_bytes=2200, conversation_history=None,
    )
    elapsed    = asyncio.get_event_loop().time() - t_start
    audio_secs = total_pcm / BYTES_PER_SEC
    print(f"[Greeting] PCM audio={audio_secs:.2f}s | wall-clock elapsed={elapsed:.2f}s")
    return elapsed




async def text_to_speech_and_send(
        transcript:           str,
        stream_sid:           str,
        websocket,
        client,
        conversation_history: list,
        meta_data:            dict,
        agent=None,
        language_code:        str = "hi-IN",
        speaker:              str = "shubh",
        stop_event:           asyncio.Event = None,
        end_call_event:       asyncio.Event = None,
        pool=None,
):
    pipeline_start = time.perf_counter()
    print(f"\n[PIPELINE] START | \"{transcript}\"")

    if agent is None:
        try:
            from instance_cache import get_cached_sync
            agent = get_cached_sync("agent")
        except ImportError:
            pass
    if agent is None:
        raise ValueError("agent must be provided or pre-cached")

    if client is None:
        try:
            from instance_cache import get_cached_sync
            client = get_cached_sync("sarvam_client")
        except ImportError:
            pass
    if client is None:
        raise ValueError("client must be provided or pre-cached")

    if stop_event is None:
        stop_event = asyncio.Event()

    def _to_lc_messages(history: list) -> list:
        result = []
        for msg in history:
            role    = msg.get("role", "")
            content = msg.get("content", "")
            if role == "user":
                result.append(HumanMessage(content=content))
            elif role == "assistant":
                result.append(AIMessage(content=content))
        return result

    agent_input = {
        "messages": _to_lc_messages(conversation_history)
    }

    tts_queue   = asyncio.Queue()
    worker_task = asyncio.create_task(
        tts_worker(
            tts_queue, stream_sid, websocket, client,
            stop_event, language_code, speaker, end_call_event,
            pool=pool, conversation_history=conversation_history
        )
    )

    token_buffer   = ""
    full_response  = ""
    sentence_total = 0
    llm_start      = time.perf_counter()
    print("[Agent] Streaming started...")

    try:
        async for event in agent.astream_events(agent_input, version="v2"):
            if stop_event.is_set():
                print("[Agent] stop_event — stopping stream")
                break

            kind = event["event"]

            if kind == "on_chat_model_stream":
                chunk = event["data"]["chunk"]
                token = chunk.content if hasattr(chunk, "content") else ""
                if not token or not token.strip():
                    continue
                token_buffer  += token
                full_response += token
                sentences, token_buffer = extract_complete_sentences(token_buffer)
                for sentence in sentences:
                    sentence_total += 1
                    print(
                        f"[Agent] → #{sentence_total} "
                        f"(t={time.perf_counter() - llm_start:.3f}s): "
                        f"\"{sentence[:60]}{'...' if len(sentence) > 60 else ''}\""
                    )
                    await tts_queue.put(sentence)

            elif kind == "on_tool_start":
                tool_name = event.get("name", "")
                print(f"[Agent] Tool: {tool_name} | {event['data'].get('input', {})}")
                if tool_name == "end_call":
                    print("[Agent] end_call — breaking stream")
                    if end_call_event is not None:
                        end_call_event.set()
                    break
                print("[Agent] tool_running=True — cancellation suppressed")

            elif kind == "on_tool_end":
                print(
                    f"[Agent] Tool result: {event.get('name', '')} → "
                    f"{event['data'].get('output', '')!r}"
                )

        if not stop_event.is_set() and not (end_call_event and end_call_event.is_set()):
            leftover = token_buffer.strip()
            if leftover:
                sentence_total += 1
                print(f"[Agent] → leftover #{sentence_total}: \"{leftover[:60]}\"")
                await tts_queue.put(leftover)

        print(
            f"[Agent] Done | {sentence_total} sentence(s) | "
            f"{len(full_response)} chars | "
            f"{time.perf_counter() - llm_start:.3f}s"
        )

    except asyncio.CancelledError:
        print("[PIPELINE] CancelledError — stopping TTS")
        stop_event.set()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        raise

    except Exception as e:
        print(f"[Agent ERROR] {e}")
        if not stop_event.is_set():
            await tts_queue.put(
                agent_fallback.get(meta_data['language_preference'], 'Hindi')
            )

    finally:
        if not worker_task.done():
            await tts_queue.put(_TtsEnd(sentence_total))
            await tts_queue.put(None)
            try:
                total_pcm, spoken_sentences = await worker_task
            except asyncio.CancelledError:
                total_pcm, spoken_sentences = 0, []
        else:
            try:
                total_pcm, spoken_sentences = worker_task.result()
            except Exception:
                total_pcm, spoken_sentences = 0, []

        if is_garbage_response(full_response):
            print(f"[History] Garbage filtered: {full_response[:60]!r}")
            spoken_sentences = []

        spoken_response = " ".join(spoken_sentences).strip()
        was_interrupted = stop_event.is_set()

        if (
            not spoken_response
            and not was_interrupted                                 
            and not (end_call_event and end_call_event.is_set())
        ):
            fallback = fall_back.get(meta_data['language_preference'], 'Hindi')
            print("[History] Empty response — playing fallback")
            try:
                fallback_queue = asyncio.Queue()
                fallback_stop  = asyncio.Event()
                await fallback_queue.put(fallback)
                await fallback_queue.put(_TtsEnd(1)) 
                await fallback_queue.put(None)
                _, fallback_spoken = await tts_worker(
                    fallback_queue, stream_sid, websocket, client,
                    fallback_stop, language_code, speaker, end_call_event,
                    pool=pool, conversation_history=conversation_history,
                )
                spoken_response = " ".join(fallback_spoken).strip()
            except Exception as e:
                print(f"[Fallback TTS] Error: {e}")

        