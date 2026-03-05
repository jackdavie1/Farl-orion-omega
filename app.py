import os
import uvicorn
from fastapi import FastAPI
from engine import OrionEngine

app = FastAPI(title="ORION_Ω_SOVEREIGN")
engine = OrionEngine()

@app.on_event("startup")
async def boot_system():
    # Automatically triggers the autonomous loops on startup
    await engine.start_autonomous_system()

@app.get("/")
async def root():
    return {"status": "ORION_Ω_ONLINE", "message": "Sovereign Orchestrator Pulse Active"}

@app.get("/status")
async def status():
    return engine.get_state()

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    # This allows you to manually push data if the Council can't write to Ledger yet
    return engine.commit_to_ledger(entry)

if __name__ == "__main__":
    # Force Railway to use the correct Port
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
