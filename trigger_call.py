from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict
import json
from dotenv import load_dotenv
import os
import requests
from shared import cache, db

load_dotenv()

call_router = APIRouter()

class CallRequest(BaseModel):
    to_number: str = Field(..., description="Customer's Phone Number", examples=['+919299147708'])
    name: str = Field(..., description="Customer's Name", examples=['Ayush Kumar'])
    id: str = Field('1', description="Customer's ID", examples=['1'])
    dynamic_variable: Dict[str, str] = Field(..., description="Dynamic variables to be used in the prompts (e.g., {'customer_name': 'Ayush Kumar', 'item_name': 'Shoes', 'cost_price': '5000', 'address': '123 Street'})", examples=[{'customer_name': 'Ayush Kumar', 'item_name': 'Shoes', 'cost_price': '5000', 'address': '123 Street'}])
    agent_version: str = Field(..., description="Agent's Version", examples=['6azb6zbba71smzk0a'])
    language: str = Field('Hindi', description="Customer's preferred language", examples=['Hindi'])
    category: str = Field('cod_verification', description="category to call customer", examples=['cod_verification'])


# Exotel credentials
EXOTEL_ACCOUNT_SID = os.getenv("EXOTEL_ACCOUNT_SID")
EXOTEL_API_TOKEN   = os.getenv("EXOTEL_API_TOKEN")
EXOTEL_API_KEY     = os.getenv("EXOTEL_API_KEY")
EXOTEL_SUBDOMAIN   = os.getenv("EXOTEL_SUBDOMAIN")
EXOPHONE           = os.getenv("EXOPHONE")
EXOTEL_APP_ID      = os.getenv("EXOTEL_APP_ID")


def trigger_exotel_call(customer_number: str) -> dict:
    url = (
        f"https://{EXOTEL_API_KEY}:{EXOTEL_API_TOKEN}"
        f"@{EXOTEL_SUBDOMAIN}/v1/Accounts/{EXOTEL_ACCOUNT_SID}/Calls/connect.json"
    )
    flow_url = f"http://my.exotel.com/{EXOTEL_ACCOUNT_SID}/exoml/start_voice/{EXOTEL_APP_ID}"

    payload = {
        "From":     customer_number,
        "CallerId": EXOPHONE,
        "Url":      flow_url,
        "CallType": "trans"
    }

    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()


@call_router.post("/trigger-call")
async def trigger_call(payload: CallRequest):
    try:
        result = trigger_exotel_call(payload.to_number)

        # Exotel returns call details under result["Call"]
        call_data = result.get("Call", {})
        call_sid  = call_data.get("Sid") or call_data.get("sid")
        version_id = payload.agent_version
        data = await db.get_version_data(version_id)

        # Keep payload dynamic_variable as a dict
        payload_vars = json.loads(payload.dynamic_variable) if isinstance(payload.dynamic_variable, str) else payload.dynamic_variable

        # Keys from DB (for cache meta), values from payload (for substitution)
        db_var_keys = data['dynamic_variable'] if data['dynamic_variable'] else {}

        print(f"[Debug] prompts keys: {list(data['prompts'].keys())}")
        print(f"[Debug] requested language: {payload.language}")

        def substitute(template: str, variables: dict) -> str:
            for key, value in variables.items():
                template = template.replace(f"{{{{{key}}}}}", value)
            return template

        await cache.set(f"meta:{call_sid}", {
            "id":                  payload.id,
            "number":              payload.to_number,
            "name":                payload.name,
            "language_preference": payload.language,
            "category":            payload.category,
            "speaker":             data['speaker'],
            "dynamic_variable":   db_var_keys  # just keys for meta
        })

        await cache.set(f"version_data:{call_sid}", {
            "agent_name":             data['agent_name'],
            "category":               data['category'],
            "prompts":                substitute(data['prompts'][payload.language], payload_vars),
            "first_message":          substitute(data['first_message'][payload.language], payload_vars),
            "conditions":             data['conditions'],
            "system_posthook_prompt": data['system_posthook_prompt'],
            "post_hook_credential":   data['post_hook_credential'],
            "llm_tool":               data['llm_tool'],
        })


        print(f"[Trigger] Exotel Call {call_sid} → metadata saved")
        return {"status": "call_initiated", "call_sid": call_sid}

    except requests.HTTPError as e:
        print(f"[Exotel HTTP Error] {e.response.status_code} - {e.response.text}")
        raise HTTPException(status_code=400, detail=f"Exotel error: {e.response.text}")

    except Exception as e:
        print(f"[Exotel Error] {e}")
        raise HTTPException(status_code=400, detail=str(e))
    

