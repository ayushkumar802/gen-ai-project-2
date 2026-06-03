from fastapi import APIRouter, HTTPException, Header
from instance_cache import prewarm_all_at_startup, get_cached_sync, invalidate_cache
from react import get_agent
from typing import Dict
import asyncio
import os
from shared import db, cache
from pydantic import BaseModel
import requests

from dotenv import load_dotenv
load_dotenv()

SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")
ENGINE_API_KEY = os.getenv("ENGINE_API_KEY", "change-me")
EXOTEL_ACCOUNT_SID= os.getenv("EXOTEL_ACCOUNT_SID")
EXOTEL_API_TOKEN= os.getenv("EXOTEL_API_TOKEN")
EXOTEL_API_KEY= os.getenv("EXOTEL_API_KEY")
EXOTEL_SUBDOMAIN= os.getenv("EXOTEL_SUBDOMAIN")

engine_router = APIRouter()

class EngineParameters(BaseModel):
    speakers: Dict[str, str] = {"shubh": "hi-IN"}  # ← add all speakers here
    pool_size: int = 2
    

@engine_router.post("/engines/start")
async def start_engines(params: EngineParameters):

    if get_cached_sync("engine_pool") is not None:
        return {"status": "already_running"}

    await db.init()
    print("[Engine] DB pool initialized")


    await prewarm_all_at_startup(
        sarvam_api_key=SARVAM_API_KEY,
        pool_size=params.pool_size,
        list_speakers=params.speakers
    )

    return {"status": "started"}


@engine_router.post("/engines/stop")
async def stop_engines():
    pool_source = get_cached_sync("engine_pool")

    if pool_source is None:
        return {"status": "already_stopped"}

    if isinstance(pool_source, dict):
        for pool in pool_source.values():
            await pool.shutdown()
    else:
        await pool_source.shutdown()

    invalidate_cache("engine_pool")
    invalidate_cache("sarvam_client")

    await db.close()
    print("[Engine] DB pool closed")

    await cache.client.aclose()
    print("[Engine] Redis pool closed")

    return {"status": "stopped"}

@engine_router.get("/engines/call-status")
def get_exotel_call_status(exotel_call_sid: str) -> str:
    url = (
        f"https://{EXOTEL_API_KEY}:{EXOTEL_API_TOKEN}@"
        f"{EXOTEL_SUBDOMAIN}/v1/Accounts/{EXOTEL_ACCOUNT_SID}"
        f"/Calls/{exotel_call_sid}.json"
    )
    r = requests.get(url, timeout=10)
    if r.status_code == 200:
        return r.json().get("Call", {}).get("Status", "unknown")
    return "unknown"