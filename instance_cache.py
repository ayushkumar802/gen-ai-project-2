"""
instance_cache.py — Async singleton cache + SarvamVoicePool startup helper.

Global engine pool (SarvamVoicePool) is shared across all concurrent calls.
Per-call engine pairs (CallPrewarmPair) are managed in main.py / tts.py.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine, Dict

from sarvam_engine import SarvamVoicePool


# ─────────────────────────────────────────────
# Async singleton cache
# ─────────────────────────────────────────────

_instances: dict[str, Any]          = {}
_locks:     dict[str, asyncio.Lock] = {}


async def get_cached(key: str, factory: Callable[[], Coroutine]) -> Any:
    if key in _instances:
        return _instances[key]

    if key not in _locks:
        _locks[key] = asyncio.Lock()

    async with _locks[key]:
        if key in _instances:
            return _instances[key]

        t = time.perf_counter()
        result = await factory()
        _instances[key] = result
        print(f"[Cache] '{key}' created in {time.perf_counter() - t:.3f}s")
        return result


def get_cached_sync(key: str) -> Any | None:
    return _instances.get(key)


def set_cached(key: str, value: Any) -> None:
    _instances[key] = value


def invalidate_cache(key: str) -> None:
    _instances.pop(key, None)
    _locks.pop(key, None)
    print(f"[Cache] '{key}' invalidated")


# ─────────────────────────────────────────────
# All-in-one startup helper
# ─────────────────────────────────────────────

SARVAM_SPEAKERS = ["shubh", "priya"]  # ← add all speakers here

async def prewarm_all_at_startup(
    sarvam_api_key:  str,
    pool_size:       int = 5,
    list_speakers:  Dict[str, str] = None
) -> None:
    from sarvamai import AsyncSarvamAI

    set_cached("sarvam_api_key",  sarvam_api_key)

    print(f"\n{'='*55}")
    print(
        f"[Startup] Pre-warming all services in parallel...\n"
    )
    print(f"{'='*55}")
    t0 = time.perf_counter()

    tasks = []

    async def _warm_sarvam():
        client = AsyncSarvamAI(api_subscription_key=sarvam_api_key)
        set_cached("sarvam_client", client)
        print("[Startup] AsyncSarvamAI client ready ✓")

        pool_dict = {}
        for spk, lng in list_speakers.items():
            p = SarvamVoicePool(
                api_key=sarvam_api_key,
                language_code=lng,
                speaker=spk,
                size=pool_size,
            )
            await p.prewarm()
            pool_dict[spk] = p
            print(f"[Startup] SarvamVoicePool({pool_size}) ready for speaker='{spk}' ✓")
        set_cached("engine_pool", pool_dict)
        print(f"[Startup] Global pool dict ready: {list(pool_dict.keys())} ✓")

    tasks.append(_warm_sarvam())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            print(f"[Startup] WARNING — a prewarm task failed: {r}")

    print(f"[Startup] All services warm in {time.perf_counter() - t0:.3f}s")
    print(f"{'='*55}\n")