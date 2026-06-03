import aiomysql
import os
from dotenv import load_dotenv
import json
import uuid

load_dotenv()

HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
PASSWORD = os.getenv('PASSWORD')
USER = os.getenv('USER')
DB = os.getenv('DB')

class AsyncDatabaseManager:
    def __init__(self):
        self.pool = None

        self.config = {
            "host": HOST,
            "port": int(PORT),
            "user": USER,
            "password": PASSWORD,
            "db": DB,
            "autocommit": True
        }
    # ----------------------------
    # 🔌 INIT POOL (call once)
    # ----------------------------
    async def init(self):
        self.pool = await aiomysql.create_pool(
            minsize=1,
            maxsize=10,
            **self.config
        )

    # ----------------------------
    # 🔍 GET USER (ASYNC)
    # ----------------------------

    async def _ensure_pool(self):
        if self.pool is None or self.pool.closed:  # ← add `or self.pool.closed`
            self.pool = await aiomysql.create_pool(
                minsize=1,
                maxsize=10,
                **self.config
            )

    async def get_user_info(self, phone_num):
        await self._ensure_pool()
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    "SELECT * FROM customers WHERE phone = %s;",
                    (phone_num,)
                )
                return await cursor.fetchone()

    # ----------------------------
    # 🧱 INSERT CONVERSATION
    # ----------------------------
    async def insert_conversation(self, data):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO call_logs 
                    (call_id, customer_id, customer_name, phone, reason, whole_convo, summary, total_call_duration, conclusion, updatation_request)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data["call_id"],
                        data["customer_id"],
                        data["customer_name"],
                        data["phone"],
                        data["reason"],
                        data["whole_convo"],
                        data["summary"],
                        data["total_call_duration"],
                        data["conclusion"],
                        data["updatation_request"]
                    )
                )

    # ----------------------------
    # 🔍 GET ALL CONVERSATIONS
    # ----------------------------
    async def get_conversations(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("SELECT * FROM conversations;")
                return await cursor.fetchall()

    # ----------------------------
    # ❌ CLOSE POOL
    # ----------------------------
    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()


    async def insert_version(
        self,
        agent_name: str,
        category: str,
        prompts: dict,
        dynamic_variable: dict | None,
        first_message: dict,
        conditions: list,
        system_posthook_prompt: str,
        post_hook_credential: dict | None,
        llm_tool: dict | None
    ) -> int:
        await self._ensure_pool()

        new_id = str(uuid.uuid4()) 

        query = """
            INSERT INTO versions (
                id, agent_name, category, prompts, dynamic_variable,
                first_message, conditions, system_posthook_prompt,
                post_hook_credential, llm_tool
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            new_id,
            agent_name,
            category,
            json.dumps(prompts),
            json.dumps(dynamic_variable) if dynamic_variable else None,
            json.dumps(first_message),
            json.dumps(conditions),
            system_posthook_prompt,
            json.dumps(post_hook_credential) if post_hook_credential else None,
            json.dumps(llm_tool) if llm_tool else None,
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, values)
                return new_id  # 👈 return the UUID we generated

            

    async def get_version_data(self, version_id: int) -> dict:
        await self._ensure_pool()

        query = "SELECT * FROM versions WHERE id = %s"

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, (version_id,))
                row = await cursor.fetchone()

                if row is None:
                    return None

                # parse json columns back to dict/list
                row["prompts"]   = json.loads(row["prompts"])   if row["prompts"]   else {}
                row["dynamic_variable"] = json.loads(row["dynamic_variable"]) if row["dynamic_variable"] else None
                row["first_message"] = json.loads(row["first_message"]) if row["first_message"] else {}
                row["conditions"]   = json.loads(row["conditions"])   if row["conditions"]   else []
                row["post_hook_credential"] = json.loads(row["post_hook_credential"]) if row["post_hook_credential"] else None
                row["llm_tool"]     = json.loads(row["llm_tool"])     if row["llm_tool"]     else None

                # convert datetime to string for JSON serialization
                row["created_at"] = row["created_at"].isoformat() if row["created_at"] else None
                row["updated_at"] = row["updated_at"].isoformat() if row["updated_at"] else None

                return row
            
    async def get_version_with_caller_num(self, caller_number: str) -> int | None:
        await self._ensure_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:

                # Step 1: lookup customer by phone, fetch agent_name
                await cursor.execute(
                    "SELECT agent_name FROM customers WHERE phone = %s AND is_active = 1",
                    (caller_number,)
                )
                customer = await cursor.fetchone()

                if customer is None:
                    return None  # caller not found

                agent_name = customer["agent_name"]

                if agent_name is None:
                    return None  # customer has no agent assigned

                # Step 2: find version_id using agent_name + inboundcall category
                await cursor.execute(
                    "SELECT * FROM versions WHERE agent_name = %s AND category = 'inbound_general'",
                    (agent_name,)
                )
                row = await cursor.fetchone()

                if row is None:
                    return None  # no version configured for this agent
                
                row["prompts"]   = json.loads(row["prompts"])   if row["prompts"]   else {}
                row["dynamic_variable"] = json.loads(row["dynamic_variable"]) if row["dynamic_variable"] else None
                row["first_message"] = json.loads(row["first_message"]) if row["first_message"] else {}
                row["conditions"]   = json.loads(row["conditions"])   if row["conditions"]   else []
                row["post_hook_credential"] = json.loads(row["post_hook_credential"]) if row["post_hook_credential"] else None
                row["llm_tool"]     = json.loads(row["llm_tool"])     if row["llm_tool"]     else None

                # convert datetime to string for JSON serialization
                row["created_at"] = row["created_at"].isoformat() if row["created_at"] else None
                row["updated_at"] = row["updated_at"].isoformat() if row["updated_at"] else None

                return row
            

    async def get_conversation_by_call_id(self, call_id: str) -> dict | None:
        await self._ensure_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    "SELECT * FROM call_logs WHERE call_id = %s",
                    (call_id,)
                )
                row = await cursor.fetchone()

                if row is None:
                    return None

                # convert datetime to string if present
                if row.get("created_at"):
                    row["created_at"] = row["created_at"].isoformat()

                return row