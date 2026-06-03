import asyncio
import os
from langchain_core.tools import tool
from functools import partial
import requests
from typing import Optional
import os
from dotenv import load_dotenv
import httpx
from mydb import AsyncDatabaseManager
import logging
from langchain_mcp_adapters.client import MultiServerMCPClient

load_dotenv()


# ─────────────────────────────────────────────
# Tools are now created per-call with a scoped
# end_call_event — no more global state.
# ─────────────────────────────────────────────



def make_end_call_tool(end_call_event: asyncio.Event):
    @tool
    def end_call(reason: str = "") -> str:
        """
        End the phone call immediately.
        Call this when the conversation is fully complete — for example:
        - Customer confirmed delivery / payment / address
        - Customer cancelled the order
        - Customer said goodbye and there is nothing left to discuss
        The reason parameter is optional — use it to log why the call ended.
        """
        end_call_event.set()
        print(f"[Tool] end_call triggered | reason: {reason!r}")
        return "Call ending."
    return end_call


@tool
def escalate_to_human(reason: str = "") -> str:
    """
    Escalate the call to a human agent.
    Call this when:
    - Customer is frustrated and asking for a human
    - The issue is outside your scope (e.g. bank dispute, legal complaint)
    - You have been asked the same question 3+ times and cannot resolve it
    """
    print(f"[Tool] escalate_to_human | reason: {reason!r}")
    # TODO: integrate with your CRM / call transfer API
    return "Escalating to a human agent."


"""
Msgcavo WhatsApp - Send Message & Send Template Message
"""


DEFAULT_PHONE_NUMBER_ID = None  # set this if your account needs it


async def _post(endpoint: str, payload: dict, timeout: int = 15, keys: str = None) -> dict:

    MSGCAVO_API_BASE   = keys.get("MSGCAVO_API_BASE", None)
    MSGCAVO_VENDOR_UID = keys.get("MSGCAVO_VENDOR_UID", None)
    MSGCAVO_TOKEN      = keys.get("MSGCAVO_TOKEN", None)

    try:

        url = f"{MSGCAVO_API_BASE}/{MSGCAVO_VENDOR_UID}/{endpoint}?token={MSGCAVO_TOKEN}"
        headers = {"Content-Type": "application/json"}
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, json=payload, timeout=timeout)
        
        if not response.is_success:
            raise RuntimeError(f"Msgcavo API error {response.status_code}: {response.text}")
        
        return response.json()
    
    except Exception as e:
        logging.error(f"Failed to call Msgcavo API: {e}")
        return {"error": str(e)}




async def send_template_message(
    keys: str,
    phone_number: str,
    template_name: str,
    template_language: str = "en",
    header_image: Optional[str] = None,
    header_video: Optional[str] = None,
    header_document: Optional[str] = None,
    header_document_name: Optional[str] = None,
    header_field_1: Optional[str] = None,
    location_latitude: Optional[str] = None,
    location_longitude: Optional[str] = None,
    location_name: Optional[str] = None,
    location_address: Optional[str] = None,
    button_0: Optional[str] = None,
    button_1: Optional[str] = None,
    copy_code: Optional[str] = None,
    from_phone_number_id: Optional[str] = DEFAULT_PHONE_NUMBER_ID,
    **extra_fields,  # 👈 captures field_1, field_2, field_N dynamically
) -> dict:
    payload = {
        "phone_number": phone_number,
        "template_name": template_name,
        "template_language": template_language,
    }

    if from_phone_number_id:
        payload["from_phone_number_id"] = from_phone_number_id

    if header_image:            payload["header_image"]          = header_image
    if header_video:            payload["header_video"]          = header_video
    if header_document:         payload["header_document"]       = header_document
    if header_document_name:    payload["header_document_name"]  = header_document_name
    if header_field_1:          payload["header_field_1"]        = header_field_1

    if location_latitude:       payload["location_latitude"]     = location_latitude
    if location_longitude:      payload["location_longitude"]    = location_longitude
    if location_name:           payload["location_name"]         = location_name
    if location_address:        payload["location_address"]      = location_address

    if button_0 is not None:    payload["button_0"]  = button_0
    if button_1 is not None:    payload["button_1"]  = button_1
    if copy_code is not None:   payload["copy_code"] = copy_code

    # Merge all dynamic fields (field_1, field_2, field_N, header_field_2, etc.)
    for k, v in extra_fields.items():
        if v is not None:
            payload[k] = v

    return await _post("contact/send-template-message", payload, keys=keys)



# ─────────────────────────────────────────────
# Static tools (no per-call state needed)
# ─────────────────────────────────────────────

logger = logging.getLogger(__name__)

STATIC_TOOLS = [
    # escalate_to_human,
]

async def get_mcp_tools(mcp_tools_map: dict):
    if not mcp_tools_map:
        return []

    server_config = {
        tool_name: {
            "transport": "sse",
            "url": mcp_url,
        }
        for tool_name, mcp_url in mcp_tools_map.items()
    }

    try:
        client = MultiServerMCPClient(server_config)
        tools = await client.get_tools()
        return tools
    except* Exception as eg:          # ✅ `except*` unwraps ExceptionGroup in Python 3.11+
        for sub in eg.exceptions:
            logger.error(f"MCP real error: {type(sub).__name__}: {sub}")
    return []


async def get_tools(end_call_event: asyncio.Event, mcp_tools_map: dict = None):
    all_tools = STATIC_TOOLS.copy()

    if not isinstance(mcp_tools_map, dict):
        mcp_tools_map = {}
    # mcp_tools_map = mcp_tools_map | {'tool1': 'https://mcp-sasha-tools-production.up.railway.app/sse'}
    print(f"Fetching MCP tools with config: {mcp_tools_map}")
    mcp_tools = await get_mcp_tools(mcp_tools_map)
    all_tools.extend(mcp_tools)

    # if end_call_event:
    #     all_tools.append(make_end_call_tool(end_call_event))

    print(f"Final tools list: {all_tools}")
    return all_tools