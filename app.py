import os
import uvicorn
import asyncio
from fastapi import FastAPI
from engine import OrionEngine

app = FastAPI(title="ORION_Ω_SOVEREIGN")
engine = OrionEngine()

@app.on_event("startup")
async def boot_system():
    # We do NOT await here. We trigger the non-blocking start.
    # This allows the FastAPI server to finish starting so Orion can run inside it.
    asyncio.create_task(engine.start_autonomous_system())

@app.get("/")
async def root():
    return {"status": "ORION_Ω_ONLINE", "message": "Sovereign Orchestrator Pulse Active"}

@app.get("/status")
async def status():
    return engine.get_state()

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    # Standard emergency injection port
    return engine.register_seed(entry)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
