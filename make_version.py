from typing import Optional, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from rediss import CacheManager
from mydb import AsyncDatabaseManager  # import the VersionDB instance

load_dotenv()

version_router = APIRouter()
cache = CacheManager()


class PostHookCredentials(BaseModel):
    MSGCAVO_API_BASE: str = Field(..., description="Base API endpoint for MSGCAVO")
    MSGCAVO_VENDOR_UID: str = Field(..., description="Vendor UID for MSGCAVO")
    MSGCAVO_TOKEN: str = Field(..., description="Authentication token for MSGCAVO")



class VersionRequest(BaseModel):
    agent_name: str = Field(..., description="Name of the company", examples=['Pawanputra Voice Agent'])
    category: str = Field(..., description="Category of the prompts", examples=['cod_verification'])
    prompts: Dict[str, str] = Field(..., description="The system prompts to AI for this version.", examples=[{'Language': 'System Prompt'}])
    dynamic_variable: Optional[Dict[str, str]] = Field(None, description="Dynamic variables to be used in the prompts (e.g., {'customer_name': 'Ayush Kumar'})", examples=[{'customer_name': 'Ayush Kumar'}])
    first_message: Dict[str, str] = Field(..., description="The first greeting prompt to be sent to the customer", examples=[{'Language': 'First message prompt'}])
    conditions: Dict[str, Dict[str, Dict[str, str]]] = Field(..., description="The output categories for that call reason (e.g. 'order_confirmed', 'human_agent', etc).", examples=[{'order_confirmed': {'field_1': {'meta_data': 'name'}, 'field_2': {'constant': '+628182712783'}}}])
    system_posthook_prompt: str = Field(..., description="The updated system prompt for post-call analysis")
    post_hook_credential: Optional[PostHookCredentials] = Field(None, description="Credentials for the post-hook tool (e.g., MSGCAVO API)")
    llm_tool: Optional[Dict[str, str]] = Field(None, description="Details of the LLM tool to use for analysis (e.g., ChatGroq)", examples=[{'name':'api_endpoint'}])



@version_router.post("/make-version")
async def make_version(payload: VersionRequest):
    """
    Create a new version with the provided prompts, conditions, and tool credentials.
    This will be used for all calls with the matching category and language in meta_data.
    """
    try:
        db = AsyncDatabaseManager()
        version_id = await db.insert_version(
            agent_name           = payload.agent_name,
            category               = payload.category,
            prompts                = payload.prompts if payload.prompts else None,
            dynamic_variable      = payload.dynamic_variable if payload.dynamic_variable else None,
            first_message          = payload.first_message if payload.first_message else None,
            conditions             = payload.conditions,
            system_posthook_prompt = payload.system_posthook_prompt,
            post_hook_credential         = payload.post_hook_credential.model_dump() if payload.post_hook_credential else None,
            llm_tool               = payload.llm_tool      if payload.llm_tool       else None,
        )
        return {"version_id": version_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@version_router.get("/get-dynamic-variables/{version_id}")
async def get_dynamic_variables(version_id: str):
    """
    Retrieve dynamic variables for a specific version by version ID.
    """
    try:
        db = AsyncDatabaseManager()
        version_data = await db.get_version_data(version_id)
        
        if version_data is None:
            raise HTTPException(status_code=404, detail=f"Version '{version_id}' not found")
        
        return {"version_id": version_id, "dynamic_variable": version_data.get("dynamic_variable")}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    



@version_router.get("/get-call-details/{call_id}")
async def get_call_details(call_id: str):
    """
    Retrieve all details about a call from call_logs by call_id.
    """
    try:
        db = AsyncDatabaseManager()
        call_data = await db.get_conversation_by_call_id(call_id)

        if call_data is None:
            raise HTTPException(status_code=404, detail=f"Call '{call_id}' not found")

        return call_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")