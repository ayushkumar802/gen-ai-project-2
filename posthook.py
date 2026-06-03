import json
import os
from typing import Literal
from pydantic import BaseModel, Field
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from uvicorn import logging
from mydb import AsyncDatabaseManager
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import Literal, Optional, Any, List
from tools import send_template_message

db = AsyncDatabaseManager()

load_dotenv()

class FieldUpdate(BaseModel):
    field: str = Field(description="The name of the field being updated (e.g. 'delivery_address', 'phone_number', 'delivery_slot')")
    old_value: Any = Field(description="The current/existing value before the update")
    new_value: Any = Field(description="The new value requested by the customer")

async def run_posthook(
    call_sid: str,
    meta_data: dict,
    version_data: dict,
    conversation_history: list,

):
    if not conversation_history:
        print("[Posthook] Empty conversation — skipping")
        return

    if not meta_data:
        print("[Posthook] No meta_data — skipping")
        return

    system_posthook_prompt = version_data['system_posthook_prompt']
    keys = version_data.get('post_hook_credential',None)
    

    conditions = version_data['conditions']

    if conditions.keys():

        class CallAnalysis(BaseModel):
            summary: str = Field(
                description="Brief summary of the call (2-4 sentences)"
            )
            conclusion: Literal[
                *conditions.keys()  # type: ignore
            ]
            updatation_request: Optional[list[FieldUpdate]] = Field(
                default=None,
                description=(
                    "List of field updates requested by the customer during the call. "
                    "Each entry contains the field name, its old value, and the new value requested. "
                    "Common updatable fields: 'delivery_address', 'phone_number', 'customer_name', "
                    "'delivery_slot', 'payment_method', 'pincode', 'landmark'. "
                    "If the customer did not request any changes, return null."
                )
            )
    else:
        class CallAnalysis(BaseModel):
            summary:    str
            conclusion: List[str]
            updatation_request: Optional[list[FieldUpdate]] = Field(
                default=None,
                description=(
                    "List of field updates requested by the customer during the call. "
                    "Each entry contains the field name, its old value, and the new value requested. "
                    "Common updatable fields: 'delivery_address', 'phone_number', 'customer_name', "
                    "'delivery_slot', 'payment_method', 'pincode', 'landmark'. "
                    "If the customer did not request any changes, return null."
                )
            )

    # ── LLM setup with structured output ─────────────────────────────
    _llm = ChatGroq(
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        api_key=os.getenv("GROQ_API_KEY"),
        temperature=0.3,
        streaming=False,                          # must be False for structured output
    ).with_structured_output(CallAnalysis)

    _prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            "You are a call analyst. Analyze customer service call conversations and return structured output."
        ),
        (
            "human", system_posthook_prompt
        )
    ])

    _chain = _prompt | _llm


    # ── Build flat conversation string ────────────────────────────────
    def build_conversation_string(conversation_history: list) -> str:
        lines = []
        for msg in conversation_history:
            role = "Agent" if msg["role"] == "assistant" else "Customer"
            lines.append(f"{role}: {msg['content']}")
        return "\n".join(lines)


    # ── Generate structured analysis ──────────────────────────────────
    async def generate_analysis(whole_convo: str) -> CallAnalysis:
        result = await _chain.ainvoke({"conversation": whole_convo})
        return result                             # already a validated CallAnalysis instance


# ── Main posthook ─────────────────────────────────────────────────

    try:
        print(f"[Posthook] Starting for call_sid={call_sid}")

        
        # ── Build conversation string ─────────────────────────────
        whole_convo = build_conversation_string(conversation_history)
        print(f"[Posthook] Conversation built ({len(whole_convo)} chars)")

        # ── Generate structured analysis via LLM ──────────────────
        print("[Posthook] Generating analysis...")
        analysis: CallAnalysis = await generate_analysis(whole_convo)
        print(f"[Posthook] Summary:    {analysis.summary[:80]}")
        print(f"[Posthook] Conclusion: {analysis.conclusion}")

        # ── Insert into DB ────────────────────────────────────────
        data = {
            "call_id":             call_sid,
            "customer_id":         meta_data.get("id"),
            "customer_name":       meta_data.get("name"),
            "phone":               meta_data.get("number"),
            "reason":              meta_data.get("category"),
            "whole_convo":         whole_convo,
            "summary":             analysis.summary,
            "total_call_duration": 0,
            "conclusion":          analysis.conclusion,
            "updatation_request":   json.dumps([u.model_dump() for u in analysis.updatation_request]) if analysis.updatation_request else None,
        }

        result = None

        await db.init()
        await db.insert_conversation(data)
        await db.close()

        # ── Send template message ───────────────────────────────────────────────────────────────────────

        

        if keys['MSGCAVO_TOKEN'] != '':  # check if keys are accessible

            dynamic_variables = meta_data['dynamic_variable']  # start with meta_data as base for variables
            template_fields = conditions[analysis.conclusion]
            # template_fields = {'field1': {'meta_data': 'name'}, 'field2': {'constant': '+628182712783'}, 'field3': {'meta_data': 'delivery_address'}} 

            def fields_for_condition(template_fields: dict) -> dict:
                result = {}
                for key, value in template_fields.items():
                    if 'meta_data' in value:
                        result[key] = dynamic_variables.get(value['meta_data'], 'N/A')
                    elif 'constant' in value:
                        result[key] = value['constant']

                    elif 'system_variable' in value:
                        if value['system_variable'] == 'call_id':
                            result[key] = call_sid
                        elif value['system_variable'] == 'name':
                            result[key] = meta_data.get("name", "Customer")
                        else:
                            logging.warning(f"Unknown system variable '{value['system_variable']}' in template fields.")
                    else:
                        logging.warning(f"Unknown field spec for '{key}': {value}")
                return result

            result = await send_template_message(
                keys=keys,
                phone_number=meta_data.get("number").replace("+", ""),
                template_name=analysis.conclusion,
                template_language="en",
                **fields_for_condition(template_fields)
            )

            print("Result:", result)


        print("[Posthook] Inserted into DB ✓")

    except Exception as e:
        import traceback
        print(f"[Posthook] Error: {e}")
        traceback.print_exc()