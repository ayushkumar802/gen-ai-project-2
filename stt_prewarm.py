import asyncio
import base64
from sarvamai import AsyncSarvamAI


class LiveStreamingSTT:
    def __init__(self, api_key: str, language_code: str = "hi-IN"):
        self._api_key      = api_key
        self._language_code = language_code
        self._sarvam       = AsyncSarvamAI(api_subscription_key=api_key)

        self.ws         = None
        self._context   = None

        # transcript output queue — written by _back_runner, read by get_transcript
        self.queue = asyncio.Queue()

        # audio input queue — written by send_audio, drained by _audio_sender
        self._audio_queue   = asyncio.Queue()

        self.task         = None   # _back_runner task
        self._sender_task = None   # _audio_sender task

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        self._context = self._sarvam.speech_to_text_streaming.connect(
            model="saaras:v3",
            mode="transcribe",
            language_code=self._language_code,
            high_vad_sensitivity=True,
            input_audio_codec="pcm_s16le",
            sample_rate=8000,
        )
        self.ws = await self._context.__aenter__()

        # Launch reader and writer as independent tasks so they never block each other
        self.task         = asyncio.create_task(self._back_runner())
        self._sender_task = asyncio.create_task(self._audio_sender())

        print("[STT] Session started")

    # ------------------------------------------------------------------
    # Dedicated write task — owns ALL writes to self.ws
    # ------------------------------------------------------------------

    async def _audio_sender(self):
        """
        Drains _audio_queue and forwards each chunk to Sarvam.
        Running as its own task means the main loop's send_audio() call
        returns immediately (just a queue.put), so _back_runner always
        gets event-loop turns to read incoming transcripts.
        """
        print("[STT:sender] started")
        try:
            while True:
                pcm_chunk = await self._audio_queue.get()

                if pcm_chunk is None:
                    # Poison pill — clean shutdown requested
                    print("[STT:sender] received stop signal")
                    break

                if self.ws is None:
                    continue

                try:
                    await self.ws.transcribe(
                        audio=base64.b64encode(pcm_chunk).decode("utf-8"),
                        sample_rate=8000,
                        encoding="audio/wav",
                    )
                except Exception as e:
                    print(f"[STT:sender] transcribe error: {e}")

        except asyncio.CancelledError:
            print("[STT:sender] cancelled")
        finally:
            print("[STT:sender] exiting")

    # ------------------------------------------------------------------
    # Dedicated read task — owns ALL reads from self.ws
    # ------------------------------------------------------------------

    async def _back_runner(self):
        print("[STT:back_runner] started")
        try:
            async for message in self.ws:
                output = message.model_dump().get("data", {}).get("transcript") or None
                print(f"[STT:back_runner] received message | transcript='{output}'")
                if output:
                    await self.queue.put(output)
                    print(f"[STT:back_runner] queued | queue_size={self.queue.qsize()}")
        except asyncio.CancelledError:
            print("[STT:back_runner] cancelled")
        except Exception as e:
            print(f"[STT:back_runner] error: {e}")
        finally:
            print("[STT:back_runner] exiting")

    # ------------------------------------------------------------------
    # Stop
    # ------------------------------------------------------------------

    async def stop(self):
        # Signal sender to exit cleanly before closing the socket
        await self._audio_queue.put(None)

        if self._sender_task and not self._sender_task.done():
            try:
                await asyncio.wait_for(self._sender_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._sender_task.cancel()

        if self._context:
            try:
                await self._context.__aexit__(None, None, None)
            except Exception as e:
                print(f"[STT:stop] context exit error: {e}")

        self.ws       = None
        self._context = None

        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        print("[STT] Session stopped")

    # ------------------------------------------------------------------
    # Drain transcript queue
    # ------------------------------------------------------------------

    async def drain_queue(self):
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break

    # ------------------------------------------------------------------
    # Reconnect between turns
    # ------------------------------------------------------------------

    async def reconnect(self):
        """
        Cleanly close the current session and open a fresh one.
        All buffered audio in Sarvam is discarded — no stale transcript bleeds in.
        """
        await self.stop()

        # Reset audio input queue — discard any buffered frames from the old turn
        self._audio_queue = asyncio.Queue()

        await self.start()
        await self.drain_queue()

        print("[STT] 🔄 Reconnected")

    # ------------------------------------------------------------------
    # Audio input — non-blocking, just enqueues
    # ------------------------------------------------------------------

    async def send_audio(self, pcm_chunk: bytes):
        """
        Returns immediately — the actual ws.transcribe() happens in
        _audio_sender, leaving the event loop free for _back_runner to
        receive Sarvam's transcript responses without starvation.
        """
        if self.ws is None:
            print("[STT:send_audio] ws is None — skipping")
            return
        await self._audio_queue.put(pcm_chunk)

    # ------------------------------------------------------------------
    # Get transcript on demand
    # ------------------------------------------------------------------

    async def get_transcript(self, initial_timeout: float = 0.7) -> str | None:
        buffer = []

        queue_size_before = self.queue.qsize()
        print(f"[STT:get_transcript] called | queue_size={queue_size_before} | timeout={initial_timeout}")

        # Drain anything already queued (arrived before this call)
        while not self.queue.empty():
            try:
                msg = self.queue.get_nowait()
                print(f"[STT:get_transcript] drained from queue: '{msg}'")
                buffer.append(msg)
            except asyncio.QueueEmpty:
                break

        # Wait for at least one fresh chunk from Sarvam
        try:
            msg = await asyncio.wait_for(self.queue.get(), timeout=initial_timeout)
            print(f"[STT:get_transcript] waited and got: '{msg}'")
            buffer.append(msg)
        except asyncio.TimeoutError:
            print(f"[STT:get_transcript] timeout hit | buffer so far: {buffer}")
        except Exception as e:
            print(f"[STT:get_transcript] exception: {e}")

        result = " ".join(filter(None, buffer)) or None
        print(f"[STT:get_transcript] returning: '{result}'")
        return result