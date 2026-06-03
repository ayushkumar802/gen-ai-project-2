from langchain.agents import create_agent
from dotenv import load_dotenv
from mydb import AsyncDatabaseManager
from tools import get_tools
from langchain_groq import ChatGroq
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

# Base LLM — no tools bound here, agent handles that
_llm = ChatGroq(
    model="meta-llama/llama-4-scout-17b-16e-instruct",
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0.3,
    streaming=True,
)

async def get_agent(version_data: dict, already_introduced: bool = False, end_call_event: asyncio.Event = None):
    system_prompt = version_data['prompts']

    if already_introduced:
        system_prompt += (
            "\n\nNOTE: Introduction has already been done via a greeting message. "
            "Skip Step 1 and continue from Step 2 onwards based on what the customer says."
        )

    mcp_tools_map = {}
    try:
        mcp_tools_map = version_data.get('llm_tool', {})  # safer than direct key access
    except Exception as e:
        print(f"Warning: Could not fetch custom tools from DB: {e}")

    # ✅ await the async function
    tools = await get_tools(end_call_event, mcp_tools_map=mcp_tools_map)

    return create_agent(
        model=_llm,
        tools=[],
        system_prompt=system_prompt,
    )


# if __name__ == "__main__":

#     version_data = {
#         'prompts': "You are a helpful assistant.",
#         'llm_tool': {}
#     }
#     async def main():
#         import time
#         start_time = time.perf_counter()
#         agent = await get_agent(version_data)
#         print(f"Agent initialized in {time.perf_counter() - start_time:.2f} seconds")
#         stop_event = asyncio.Event()
#         try:
#             async for event in agent.astream_events({"messages": "What date is it today?"}, version="v2"):
#                 if stop_event.is_set():
#                     print("[Agent] stop_event — stopping stream")
#                     break

#                 print(event)



#         except asyncio.CancelledError:
#             print("[PIPELINE] CancelledError — stopping TTS")
#             stop_event.set()
#             raise

#         except Exception as e:
#             print(f"[Agent ERROR] {e}")

            

#     asyncio.run(main())