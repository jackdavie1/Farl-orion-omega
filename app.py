import os
import uvicorn
import threading
import asyncio
import requests
import logging
from fastapi import FastAPI
from datetime import datetime, timezone

app = FastAPI(title="ORION_Ω_SOVEREIGN")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orion-App")

LEDGER_URL = os.getenv("LEDGER_URL")

ORION_STATE = {
    "status": "INITIALIZING",
    "last_run": None,
    "agent_proposals": 0
}

ENGINE_INSTANCE = None
ENGINE_LOOP = None


def run_orion_isolated():
    global ENGINE_INSTANCE, ENGINE_LOOP

    try:
        from engine import OrionEngine

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ENGINE_LOOP = loop

        ENGINE_INSTANCE = OrionEngine()
        ORION_STATE["status"] = "SOVEREIGN_ACTIVE"

        loop.run_until_complete(ENGINE_INSTANCE.start_autonomous_system())

    except Exception as e:
        ORION_STATE["status"] = f"ERROR: {str(e)}"
        logger.error(f"ORION FAILURE: {e}")


@app.on_event("startup")
async def startup_event():
    thread = threading.Thread(target=run_orion_isolated, daemon=True)
    thread.start()
    logger.info("ORION THREAD LAUNCHED")


@app.get("/")
async def root():
    return {
        "status": "ORION_Ω_ONLINE",
        "engine": ORION_STATE["status"]
    }


@app.get("/status")
async def status():
    state = ORION_STATE.copy()
    if ENGINE_INSTANCE:
        state.update(ENGINE_INSTANCE.get_state())
    return state


@app.post("/agent/propose")
async def agent_propose(payload: dict):
    """
    Direct submission endpoint for AI nodes.
    Claude, Grok, ChatGPT, Gemini post here directly.
    Proposal goes straight into Orion's agent queue — no human relay.

    Supported entry_type values:
      SEED_REGISTRATION  — register a new seed
      PATCH_PROPOSAL     — propose a code evolution
      COUNCIL_SYNTHESIS  — update constraints + optional patch
    """
    try:
        if ENGINE_INSTANCE and ENGINE_LOOP:
            # Push directly into Orion's Layer 4 queue
            asyncio.run_coroutine_threadsafe(
                ENGINE_INSTANCE.agent_queue.put(payload),
                ENGINE_LOOP
            )
            ORION_STATE["agent_proposals"] += 1
            logger.info(f"AGENT_PROPOSAL_QUEUED — type: {payload.get('entry_type')}")
            return {
                "status": "PROPOSAL_QUEUED",
                "entry_type": payload.get("entry_type"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "queue_position": ENGINE_INSTANCE.agent_queue.qsize()
            }
        else:
            # Engine not ready yet — write directly to ledger as fallback
            entry = {
                "entry_type": payload.get("entry_type", "COUNCIL_SYNTHESIS"),
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "payload": payload
            }
            r = requests.post(LEDGER_URL, json=entry, timeout=10)
            ORION_STATE["agent_proposals"] += 1
            return {
                "status": "PROPOSAL_ACCEPTED_VIA_LEDGER",
                "ledger_status": r.status_code
            }

    except Exception as e:
        logger.error(f"AGENT_PROPOSE ERROR: {e}")
        return {"status": "ERROR", "message": str(e)}


@app.post("/operator/evolve")
async def operator_evolve(patch: dict):
    """
    Emergency direct evolution trigger.
    Normally unused once agents are autonomous.
    """
    try:
        if ENGINE_INSTANCE:
            ENGINE_INSTANCE.execute_patch(patch)
        return {"status": "EVOLUTION_TRIGGERED"}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, workers=1)
