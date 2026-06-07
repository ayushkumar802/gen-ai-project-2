from contextlib import asynccontextmanager
import warnings

from sarvam_engine import SarvamVoicePool
warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastapi import FastAPI, HTTPException, WebSocket
from starlette.websockets import WebSocketDisconnect
import json
import base64
import asyncio
from sarvamai import AsyncSarvamAI
from dotenv import load_dotenv
import os
from tts import (
    text_to_speech_and_send,
    send_greeting_audio,
    play_system_message,
)
from preprocess_wav import is_speech
import time
from react import get_agent
from trigger_call import call_router
from make_version import version_router
from posthook import run_posthook
from reason_prompts import system_messages
from start_engine import engine_router
from shared import db, cache

from instance_cache import (
    get_cached_sync
)

from stt_prewarm import LiveStreamingSTT

load_dotenv()

SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    pool_source = get_cached_sync("engine_pool")
    if pool_source is not None:
        if isinstance(pool_source, dict):
            for p in pool_source.values():
                await p.shutdown()
        else:
            await pool_source.shutdown()
        print("[Shutdown] Engine pool closed")

    await db.close()
    await cache.client.aclose()
    print("[DB][Redis] Pools closed")


app = FastAPI(lifespan=lifespan)
app.include_router(call_router)
app.include_router(version_router)
app.include_router(engine_router)

# at the top, outside media_stream
_call_end_flags: dict[str, list] = {}

@app.post("/end-call/{call_sid}")
async def end_call(call_sid: str):
    flag = _call_end_flags.get(call_sid)
    if flag is None:
        raise HTTPException(status_code=404, detail="No active call")
    flag[0] = True
    return {"status": "ok"}

IDLE          = "IDLE"
EARLY         = "EARLY"
LOCKED        = "LOCKED"
INTERRUPTIBLE = "INTERRUPTIBLE"
GREETING      = "GREETING"
CANCELLING    = "CANCELLING"


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    await websocket.accept()

    call_sid     = None
    meta_data    = None
    version_data = None

    client = get_cached_sync("sarvam_client")
    if client is None:
        print("[WS] sarvam_client not cached — creating fresh (cold start?)")
        client = AsyncSarvamAI(api_subscription_key=SARVAM_API_KEY)

    stream_sid             = None
    conversation_history   = []

    end_call_event = asyncio.Event()
    call_ended     = [False]

    state                 = [IDLE]
    pipeline_task         = [None]
    silence_task          = [None]
    state_task            = [None]
    locked_task           = [None]
    pipeline_running      = [False]
    was_pipeline_running  = [False]
    stop_event            = [None]
    fast_interrupt_frames = [0]
    interrupt_count       = [0]
    early_interrupt_count = [0]
    loop_count            = [1]
    lng_code              = ['hi-IN']

    transcript_buffer = [[]]

    agent_cache      = [None]
    agent_cache_task = [None]

    local_prewarm = [None]   # SarvamVoicePool instance
    local_stt     = [None]   # LiveStreamingSTT instance

    LOOP2_THRESHOLD           = 1
    EARLY_INTERRUPT_THRESHOLD = 1
    LOOP2_LOCK_SECS           = 15.0

    watchdog_tts_stop = [None]

    speech_window = []
    WINDOW_SIZE   = 10
    SPEECH_RATIO  = 0.50
    MIN_CONSEC    = 7

    SILENCE_THRESHOLD = [0.7]

    EARLY_WINDOW   = 2.0
    LOCKED_WINDOW  = 5.0

    last_speech_time        = [time.time()]
    watchdog_task           = [None]
    watchdog_prompted       = [False]
    WATCHDOG_FIRST_SILENCE  = 15.0
    WATCHDOG_SECOND_SILENCE = 10.0

    # ─────────────────────────────────────────────────────────────────
    # Clean shutdown
    # ─────────────────────────────────────────────────────────────────
    async def cleanup_all_tasks():
        if call_ended[0]:
            print("[Cleanup] Already ended — skipping")
            return

        call_ended[0] = True

        for task in [
            silence_task[0],
            pipeline_task[0],
            state_task[0],
            locked_task[0],
            watchdog_task[0],
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # FIX-6: removed double-null bug — pool now always gets shut down
        prewarm = local_prewarm[0]
        local_prewarm[0] = None
        if prewarm is not None:
            try:
                await prewarm.shutdown()
            except Exception as e:
                print(f"[Cleanup] local_prewarm shutdown error: {e}")

        stt = local_stt[0]
        local_stt[0] = None
        if stt is not None:
            try:
                await stt.stop()
            except Exception as e:
                print(f"[Cleanup] local_stt stop error: {e}")

        if call_sid:
            await cache.delete(f"meta:{call_sid}")
            await cache.delete(f"version_data:{call_sid}")

        print("[Cleanup] All tasks cancelled")

        if call_sid and conversation_history:
            await run_posthook(call_sid, meta_data, version_data, conversation_history)
        else:
            print(f"[Posthook] Skipped | call_sid={call_sid} | messages={len(conversation_history)}")

    # ─────────────────────────────────────────────────────────────────
    # Speech detection
    # ─────────────────────────────────────────────────────────────────
    def is_sustained_speech() -> bool:
        if len(speech_window) < WINDOW_SIZE:
            return False
        if sum(speech_window) / len(speech_window) < SPEECH_RATIO:
            return False
        return all(speech_window[-MIN_CONSEC:])

    # ─────────────────────────────────────────────────────────────────
    # State helpers
    # ─────────────────────────────────────────────────────────────────
    def go_idle():
        state[0] = IDLE
        speech_window.clear()
        fast_interrupt_frames[0] = 0
        pipeline_task[0]        = None
        print("[State] → IDLE")

    def go_cancel():
        state[0] = CANCELLING
        speech_window.clear()
        fast_interrupt_frames[0] = 0
        print("[State] → CANCELLING")

    async def clear_exotel():
        try:
            await websocket.send_text(json.dumps({
                "event":      "clear",
                "stream_sid": stream_sid,
            }))
            print("[Exotel] Buffer cleared")
        except Exception:
            pass

    async def hang_up():
        print("[Watchdog] Hanging up — closing WebSocket")
        try:
            await websocket.close()
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────
    # Pipeline cancel helpers
    # ─────────────────────────────────────────────────────────────────
    def signal_cancel_pipeline():
        if stop_event[0] is not None:
            stop_event[0].set()
            print("[Cancel] stop_event set — TTS halts within 20ms")
        if pipeline_task[0] and not pipeline_task[0].done():
            pipeline_task[0].cancel()

    async def await_pipeline_done():
        if pipeline_task[0] and not pipeline_task[0].done():
            try:
                await pipeline_task[0]
            except asyncio.CancelledError:
                pass
        pipeline_task[0] = None
        stop_event[0]    = None

    async def cancel_state_timer():
        if state_task[0] and not state_task[0].done():
            state_task[0].cancel()
            try:
                await state_task[0]
            except asyncio.CancelledError:
                pass
        state_task[0] = None

    async def cancel_locked_timer():
        if locked_task[0] and not locked_task[0].done():
            locked_task[0].cancel()
            try:
                await locked_task[0]
            except asyncio.CancelledError:
                pass
        locked_task[0] = None

    # ─────────────────────────────────────────────────────────────────
    # State timers
    # ─────────────────────────────────────────────────────────────────
    async def early_to_locked(start_with: str = EARLY):
        try:
            await asyncio.sleep(EARLY_WINDOW)
            if state[0] in [EARLY, LOCKED]:
                state[0] = LOCKED
                print("[State] EARLY → LOCKED")
                locked_task[0] = asyncio.create_task(locked_to_interruptible(start_with))
        except asyncio.CancelledError:
            pass

    async def locked_to_interruptible(start_with: str = EARLY):
        checker = None
        try:
            if start_with != LOCKED:
                checker = asyncio.create_task(locked_rechecker())
            else:
                checker = asyncio.Future()
                checker.set_result(None)

            if loop_count[0] == 2:
                print(f"[Lock 2] LOCKED — {LOOP2_LOCK_SECS}s no interruption")
                await asyncio.sleep(LOOP2_LOCK_SECS)
                await checker
                if state[0] == LOCKED:
                    early_interrupt_count[0] = 0
                    state[0] = INTERRUPTIBLE
                    print("[Lock 2] Window expired → INTERRUPTIBLE")
            else:
                await asyncio.sleep(LOCKED_WINDOW)
                await checker
                if state[0] == LOCKED:
                    early_interrupt_count[0] = 0
                    state[0] = INTERRUPTIBLE
                    print("[State] LOCKED → INTERRUPTIBLE")

        except asyncio.CancelledError:
            if checker and not checker.done():
                checker.cancel()
                try:
                    await checker
                except:
                    pass

    # ─────────────────────────────────────────────────────────────────
    # Silence watchdog
    # ─────────────────────────────────────────────────────────────────
    async def silence_watchdog():
        try:
            speech_snapshot = last_speech_time[0]
            await asyncio.sleep(WATCHDOG_FIRST_SILENCE)

            if state[0] in [LOCKED, INTERRUPTIBLE, GREETING]:
                print(f"[Watchdog] Active state {state[0]} — dismissed")
                return

            if pipeline_task[0] and not pipeline_task[0].done():
                print("[Watchdog] Pipeline running — dismissed")
                return

            watchdog_prompted[0] = True

            name = ""
            if meta_data:
                full_name = meta_data.get("name", "")
                name = full_name.split()[0] if full_name else ""

            prompt_text = (
                system_messages["with_name"][meta_data["language_preference"]].replace(
                    "{{name}}", name
                )
                if name
                else system_messages["without_name"][meta_data["language_preference"]]
            )

            if last_speech_time[0] <= speech_snapshot:
                print(f"[Watchdog] {WATCHDOG_FIRST_SILENCE}s silence — asking: \"{prompt_text}\"")
                conversation_history.append({"role": "assistant", "content": prompt_text})

                watchdog_tts_stop[0] = asyncio.Event()
                await play_system_message(
                    text=prompt_text,
                    stream_sid=stream_sid,
                    websocket=websocket,
                    client=client,
                    stop_event=watchdog_tts_stop[0],
                    language_code=lng_code[0],
                    speaker=meta_data["speaker"] if meta_data else "shubh",
                    end_call_event=end_call_event,
                )
                watchdog_tts_stop[0] = None

            if last_speech_time[0] > speech_snapshot:
                print("[Watchdog] User spoke during prompt — call continues")
                watchdog_prompted[0] = False
                reset_watchdog()
                return

            speech_snapshot = last_speech_time[0]
            await asyncio.sleep(WATCHDOG_SECOND_SILENCE)

            if last_speech_time[0] > speech_snapshot:
                print("[Watchdog] Speech detected after prompt — call continues")
                watchdog_prompted[0] = False
                reset_watchdog()
                return

            print(f"[Watchdog] No response after {WATCHDOG_SECOND_SILENCE}s — hanging up")
            await hang_up()

        except asyncio.CancelledError:
            pass

    def reset_watchdog():
        if watchdog_prompted[0]:
            return
        if call_ended[0]:
            if not end_call_event.is_set():
                end_call_event.set()
            else:
                return
        # FIX-9: don't spawn new watchdog timers after call has ended
        if watchdog_task[0] and not watchdog_task[0].done():
            watchdog_task[0].cancel()
        watchdog_task[0] = asyncio.create_task(silence_watchdog())

    # ─────────────────────────────────────────────────────────────────
    # Greeting
    # ─────────────────────────────────────────────────────────────────
    async def send_greeting(text: str):
        state[0] = GREETING
        print(f"[State] → GREETING | \"{text[:60]}\"")

        speech_window.clear()
        if silence_task[0] and not silence_task[0].done():
            silence_task[0].cancel()
            silence_task[0] = None
        print("[Greeting] Buffer + silence timer cleared")
        
        conversation_history.append({"role": "assistant", "content": text})

        elapsed_secs = await send_greeting_audio(
            greeting_text=text,
            stream_sid=stream_sid,
            websocket=websocket,
            sarvam_client=client,
            language_code=lng_code[0],
            speaker=meta_data["speaker"] if meta_data else "shubh",
            end_call_event=end_call_event,
            pool=local_prewarm[0],
        )

        if elapsed_secs < 5.0:
            print(f"[Greeting] ⚠️ Suspiciously short ({elapsed_secs:.1f}s) — retrying")
            elapsed_secs = await send_greeting_audio(
                greeting_text=text,
                stream_sid=stream_sid,
                websocket=websocket,
                sarvam_client=client,
                language_code=lng_code[0],
                speaker=meta_data["speaker"] if meta_data else "shubh",
                end_call_event=end_call_event,
                pool=local_prewarm[0],
            )

        speech_window.clear()
        if silence_task[0] and not silence_task[0].done():
            silence_task[0].cancel()
            silence_task[0] = None

        # FIX-7: await agent_cache_task with a timeout so greeting can't hang
        # indefinitely if agent build is slow
        if agent_cache_task[0] is not None:
            try:
                agent_cache[0] = await asyncio.wait_for(
                    asyncio.shield(agent_cache_task[0]), timeout=10.0
                )
            except asyncio.TimeoutError:
                print("[Greeting] agent_cache_task timed out — will rebuild on first turn")
                agent_cache[0] = None

        go_idle()
        last_speech_time[0] = time.time()
        reset_watchdog()
        print("[Greeting] Done — now listening")

    # ─────────────────────────────────────────────────────────────────
    # Pipeline: STT → Agent → TTS
    # ─────────────────────────────────────────────────────────────────
    async def start_pipeline(transcript: str = None, start_with: str = EARLY):
        # FIX-10: set pipeline_running immediately to close the race window
        if pipeline_running[0]:
            print("[Pipeline] Already running — suppressing duplicate")
            return
        pipeline_running[0] = True
        was_pipeline_running[0] = False

        try:
            if pipeline_task[0] and not pipeline_task[0].done():
                pipeline_running[0] = False
                return

            await cancel_state_timer()
            await cancel_locked_timer()

            stop_event[0] = asyncio.Event()
            speech_window.clear()

            state[0] = start_with

            print("[Pipeline] Starting — flushing STT and waiting for transcript")

            async def run(transcript: str = None, start_with: str = EARLY):
                t1 = time.time()
                try:
                    if transcript is None:
                        go_idle()
                        return

                    print(f"[STT] {time.time()-t1:.2f}s | \"{transcript}\"")

                    last_speech_time[0] = time.time()
                    if watchdog_tts_stop[0] is not None:
                        watchdog_tts_stop[0].set()
                        print("[STT] Watchdog TTS cancelled — user spoke")

                    reset_watchdog()

                    await cancel_state_timer()
                    state_task[0] = asyncio.create_task(early_to_locked(start_with))
                    print("[State] → EARLY (TTS starting)")

                    t2 = time.time()

                    if agent_cache[0] is None:
                        print("[Agent] Building agent...")
                        agent_cache[0] = await get_agent(
                            version_data,
                            already_introduced=True,
                            end_call_event=end_call_event,
                        )
                        print("[Agent] Agent cached")
                    else:
                        print("[Agent] Reusing cached agent")

                    agent = agent_cache[0]

                    await text_to_speech_and_send(
                        transcript=transcript,
                        stream_sid=stream_sid,
                        websocket=websocket,
                        client=client,
                        conversation_history=conversation_history,
                        meta_data=meta_data,
                        agent=agent,
                        stop_event=stop_event[0],
                        end_call_event=end_call_event,
                        language_code=lng_code[0],
                        pool=local_prewarm[0]
                    )

                    print(f"[TTS] {time.time()-t2:.2f}s | [Total] {time.time()-t1:.2f}s")

                    if end_call_event.is_set():
                        print("[Pipeline] LLM called end_call — shutting down cleanly")
                        call_ended[0] = True

                        for t in [silence_task[0], state_task[0], locked_task[0], watchdog_task[0]]:
                            if t and not t.done():
                                t.cancel()

                        await asyncio.sleep(6.0)

                        if call_sid:
                            await cache.delete(f"meta:{call_sid}")

                        prewarm = local_prewarm[0]
                        local_prewarm[0] = None
                        if prewarm is not None:
                            try:
                                await prewarm.shutdown()
                            except Exception as e:
                                print(f"[Pipeline] local_prewarm close error: {e}")

                        await run_posthook(call_sid, meta_data, version_data, conversation_history)
                        await websocket.close()
                        return

                    print(f"[Loop] Agent completed | early_interrupt_count={early_interrupt_count[0]}")
                    interrupt_count[0] = 0
                    go_idle()

                except asyncio.CancelledError:
                    if state[0] == INTERRUPTIBLE:
                        go_idle()
                    else:
                        print("[Pipeline] Cancelled (clean interrupt)")
                        go_cancel()
                    raise

                except Exception as e:
                    print(f"[Pipeline] Error: {e}")

                finally:
                    pipeline_task[0] = None
                    await cancel_state_timer()
                    await cancel_locked_timer()

                    if not call_ended[0]:
                        watchdog_prompted[0] = False
                        last_speech_time[0]  = time.time()
                        reset_watchdog()
                    pipeline_running[0] = False

            pipeline_task[0] = asyncio.create_task(run(transcript=transcript, start_with=start_with))

        except Exception as e:
            print(f"[Pipeline] Failed to start: {e}")
            pipeline_running[0] = False

    async def locked_rechecker():
        try:
            stt = local_stt[0]
            if stt is None:
                return
            transcript = await stt.get_transcript()
            if transcript:
                print('[LOCKED] We find transcript in Queue - Cancelling Pipeline')
                await asyncio.shield(_do_recheck_and_restart(transcript))
            else:
                print('[LOCKED] We find no transcript in Queue left')
        except asyncio.CancelledError:
            pass

    async def _do_recheck_and_restart(transcript):
        if pipeline_running[0]:
            fast_interrupt_frames[0] = 0
            signal_cancel_pipeline()
            await clear_exotel()
            await await_pipeline_done()
            pipeline_running[0] = False
            await cancel_state_timer()
            await cancel_locked_timer()

        if state[0] == CANCELLING:
            state[0] = LOCKED

        if transcript:
            conversation_history.append({"role": "user", "content": transcript})
            print(f"[History] User turn committed: \"{transcript[:50]}\"")

        await start_pipeline(transcript=transcript, start_with=LOCKED)

    # ─────────────────────────────────────────────────────────────────
    # Silence timeout — owns silence detection, fires pipeline
    # ─────────────────────────────────────────────────────────────────
    async def silence_timeout():
        # FIX-8: capture stt reference immediately before any awaits
        # so a race with cleanup can't null it out beneath us
        stt = local_stt[0]
        if stt is None:
            return

        async def _clean_transcript(final_transcript):
            print(f"[Transcript Added] {final_transcript}")
            conversation_history.append({"role": "user", "content": final_transcript})
            print(f"[History] User turn committed: \"{final_transcript[:50]}\"")
            transcript_buffer[0].clear()

        async def _add_buffer():
            transcript = await stt.get_transcript(initial_timeout=0.25)
            if transcript and transcript not in transcript_buffer[0]:
                transcript_buffer[0].append(transcript)
            final_transcript = " ".join(transcript_buffer[0])
            return final_transcript

        async def _cancel_pipeline():
            pipeline_running[0], was_pipeline_running[0] = False, True
            fast_interrupt_frames[0] = 0
            early_interrupt_count[0] += 1
            print(f"[Silence Timeout] Interrupt triggered | early_interrupt_count={early_interrupt_count[0]}")
            signal_cancel_pipeline()
            await clear_exotel()
            await await_pipeline_done()
            await cancel_state_timer()
            await cancel_locked_timer()

        try:
            if pipeline_running[0]:
                await asyncio.shield(_cancel_pipeline())

            await asyncio.sleep(SILENCE_THRESHOLD[0])

            if state[0] == CANCELLING:
                state[0] = EARLY

            final_transcript = await asyncio.shield(_add_buffer())

            if final_transcript:
                await asyncio.shield(_clean_transcript(final_transcript))
                await start_pipeline(transcript=final_transcript)
            else:
                if was_pipeline_running[0]:
                    await start_pipeline(transcript="")

        except asyncio.CancelledError:
            pass

    async def reset_silence_timer():
        # FIX-9: don't spawn silence timers after call has ended
        if call_ended[0]:
            if not end_call_event.is_set():
                end_call_event.set()
        if silence_task[0] and not silence_task[0].done():
            silence_task[0].cancel()
        silence_task[0] = asyncio.create_task(silence_timeout())

    # ─────────────────────────────────────────────────────────────────
    # Main WebSocket loop
    # ─────────────────────────────────────────────────────────────────
    while True:
        try:
            try:
                message = await websocket.receive_text()
            except RuntimeError:
                print("[Server] WebSocket closed — exiting loop")
                break

            data  = json.loads(message)
            event = data.get("event")

            if event == "connected":
                print("[Server] Stream connected")

            elif event == "start":
                start_data = data.get("start", {})
                print(f"[Server] Stream start event received: {start_data}")

                stream_sid = (
                    data.get("stream_sid")
                    or start_data.get("stream_sid")
                    or start_data.get("streamSid")
                )
                call_sid = (
                    start_data.get("call_sid")
                    or start_data.get("callSid")
                )

                _call_end_flags[call_sid] = call_ended

                print(f"[Server] Stream started: {stream_sid} | call_sid: {call_sid}")

                meta_data, version_data = await asyncio.gather(
                    cache.get(f"meta:{call_sid}"),
                    cache.get(f"version_data:{call_sid}"),
                )
                caller_num = "+91" + start_data.get("from", "")[1:]

                if not meta_data:
                    print(f"[Inbound] No meta_data — fetching user info for {caller_num}")
                    user_info = None

                    if caller_num:
                        try:
                            user_info = await db.get_user_info(caller_num)
                            print(f"[DB] User info for {caller_num}: {user_info}")

                            if user_info:
                                meta_data = {
                                    "id":                  user_info.get("id", "N/A"),
                                    "name":                user_info.get("name", ""),
                                    "number":              user_info.get("phone", ""),
                                    "language_preference": "Hindi",
                                    "category":            "inbound_general",
                                    "speaker":             "shubh",
                                }
                                print(f"[Inbound] User found: {meta_data}")
                            else:
                                meta_data = {
                                    "id":                  "N/A",
                                    "name":                "",
                                    "number":              caller_num,
                                    "language_preference": "Hindi",
                                    "category":            "inbound_general",
                                    "speaker":             "shubh",
                                }
                                print("[Inbound] Unknown user — using defaults")

                        except Exception as e:
                            print(f"[Inbound] get_user_info error: {e}")
                            meta_data = {
                                "id":                  "N/A",
                                "name":                "",
                                "number":              caller_num or "unknown",
                                "language_preference": "Hindi",
                                "category":            "inbound_general",
                                "speaker":             "shubh",
                            }
                    else:
                        print("[Inbound] Could not determine caller number")
                        meta_data = {
                            "id":                  "N/A",
                            "name":                "",
                            "number":              "unknown",
                            "language_preference": "Hindi",
                            "category":            "inbound_general",
                            "speaker":             "shubh",
                        }

                    await cache.set(f"meta:{call_sid}", meta_data)

                print(f"[Debug] meta_data: {meta_data}")

                if not version_data:
                    version_db_row = await db.get_version_with_caller_num(caller_num)

                    if not version_db_row:
                        await cleanup_all_tasks()
                        break

                    version_data = {
                        "agent_name":             version_db_row["agent_name"],
                        "category":               version_db_row["category"],
                        "prompts":                version_db_row["prompts"][meta_data["language_preference"]].replace("{{name}}", meta_data["name"]),
                        "first_message":          version_db_row["first_message"][meta_data["language_preference"]].replace("{{name}}", meta_data["name"]),
                        "conditions":             version_db_row["conditions"],
                        "system_posthook_prompt": version_db_row["system_posthook_prompt"],
                        "post_hook_credential":   version_db_row["post_hook_credential"],
                        "llm_tool":               version_db_row["llm_tool"],
                    }

                    await cache.set(f"version_data:{call_sid}", version_data)

                agent_cache_task[0] = asyncio.create_task(get_agent(
                    version_data,
                    already_introduced=True,
                    end_call_event=end_call_event,
                ))

                greeting_data = version_data["first_message"] or None

                lng = meta_data["language_preference"]
                lng_code[0] = "hi-IN" if lng == "Hindi" else "en-IN"

                local_prewarm[0] = SarvamVoicePool(
                    api_key=SARVAM_API_KEY,
                    size=4,
                    language_code=lng_code[0],
                    speaker=meta_data["speaker"]
                )

                local_stt[0] = LiveStreamingSTT(
                    api_key=SARVAM_API_KEY,
                    language_code=lng_code[0],
                )
                asyncio.create_task(local_stt[0].start())

                if greeting_data:
                    asyncio.create_task(send_greeting(greeting_data))
                else:
                    local_prewarm[0]._connect_task = asyncio.create_task(
                        local_prewarm[0].prewarm()
                    )
                    last_speech_time[0] = time.time()
                    reset_watchdog()

            elif event == "media":
                if state[0] == GREETING:
                    continue

                pcm_bytes = base64.b64decode(data["media"]["payload"])

                stt = local_stt[0]
                if stt is not None:
                    await stt.send_audio(pcm_bytes)

                frame_is_speech = is_speech(pcm_bytes)
                speech_window.append(1 if frame_is_speech else 0)
                if len(speech_window) > WINDOW_SIZE:
                    speech_window.pop(0)

                current = state[0]

                if current == IDLE:
                    if frame_is_speech:
                        await reset_silence_timer()
                        last_speech_time[0] = time.time()

                elif current in [EARLY, CANCELLING]:
                    if frame_is_speech:
                        fast_interrupt_frames[0] += 1
                    else:
                        fast_interrupt_frames[0] = 0

                    if is_sustained_speech() and fast_interrupt_frames[0] >= 14:
                        if state[0] == "CANCELLING":
                            state[0] = EARLY

                        if early_interrupt_count[0] >= EARLY_INTERRUPT_THRESHOLD:
                            continue
                        else:
                            await reset_silence_timer()

                elif current == LOCKED:
                    fast_interrupt_frames[0] = 0

                elif current == INTERRUPTIBLE:
                    if frame_is_speech:
                        fast_interrupt_frames[0] += 1
                    else:
                        fast_interrupt_frames[0] = 0

                    if fast_interrupt_frames[0] >= 5:
                        if state[0] == "CANCELLING":
                            state[0] = INTERRUPTIBLE

                        if early_interrupt_count[0] >= LOOP2_THRESHOLD:
                            state[0] = LOCKED
                            continue
                        else:
                            interrupt_count[0] += 1
                            if interrupt_count[0] >= 2:
                                loop_count[0] = 2
                            else:
                                loop_count[0] = 1
                            await reset_silence_timer()

            elif event == "stop":
                print("[Server] Stream stopped")
                await cleanup_all_tasks()
                break

        except WebSocketDisconnect as e:
            print(f"[Server] Call ended — WebSocket closed by Exotel (code={e.code})")
            await cleanup_all_tasks()
            break

        except Exception as e:
            import traceback
            print(f"[Server] Error: {e}")
            traceback.print_exc()
            await cleanup_all_tasks()
            break
