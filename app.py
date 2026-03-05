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

# Shared system state
ORION_STATE = {
    "status": "INITIALIZING",
    "last_run": None,
    "agent_proposals": 0
}

ENGINE_INSTANCE = None


def run_orion_isolated():
    """
    Runs Orion Engine in an isolated thread so FastAPI never crashes
    if the engine fails.
    """
    global ENGINE_INSTANCE

    try:
        from engine import OrionEngine

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        ENGINE_INSTANCE = OrionEngine()

        ORION_STATE["status"] = "SOVEREIGN_ACTIVE"

        loop.run_until_complete(
            ENGINE_INSTANCE.start_autonomous_system()
        )

        loop.run_forever()

    except Exception as e:
        ORION_STATE["status"] = f"ERROR: {str(e)}"
        logger.error(f"ORION FAILURE: {e}")


@app.on_event("startup")
async def startup_event():
    thread = threading.Thread(target=run_orion_isolated, daemon=True)
    thread.start()


@app.get("/")
async def root():
    return {
        "status": "ORION_Ω_ONLINE",
        "engine": ORION_STATE["status"]
    }


@app.get("/status")
async def status():
    return ORION_STATE


# -------------------------
# AGENT CONNECTOR ENDPOINT
# -------------------------

@app.post("/agent/propose")
async def agent_propose(payload: dict):
    """
    Agents (Claude / Grok / ChatGPT) post proposals here.
    Orion records them to the Ledger.
    Orion will execute them on the next pulse automatically.
    """

    try:

        entry = {
            "entry_type": "COUNCIL_SYNTHESIS",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "payload": payload
        }

        r = requests.post(
            LEDGER_URL,
            json=entry,
            timeout=10
        )

        ORION_STATE["agent_proposals"] += 1

        return {
            "status": "PROPOSAL_ACCEPTED",
            "ledger_status": r.status_code
        }

    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


# -------------------------
# EMERGENCY DIRECT PATCH
# -------------------------

@app.post("/operator/evolve")
async def operator_evolve(patch: dict):
    """
    Direct evolution trigger if needed.
    Normally unused once agents are autonomous.
    """

    try:
        if ENGINE_INSTANCE:
            ENGINE_INSTANCE.trigger_evolution(patch)

        return {"status": "EVOLUTION_TRIGGERED"}

    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        workers=1
    )
