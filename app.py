import os
import asyncio
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from engine import OrionEngine 

app = FastAPI(title="ORION_Ω_SOVEREIGN")
engine = OrionEngine()

@app.on_event("startup")
async def start_autonomous_loop():
    # Starts the background heartbeat
    asyncio.create_task(engine.run_continuous_cycle())

@app.get("/")
async def root():
    # Adding a root response so you don't get a 404 on the main link
    return {"status": "ORION_Ω_ONLINE", "message": "Sovereign Orchestrator Active"}

@app.get("/status")
async def get_system_status():
    return engine.get_state()

@app.post("/cycle/trigger")
async def manual_trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(engine.execute_cycle())
    return {"message": "Sovereign Cycle Triggered"}

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    # Endpoint for the ChatGPT Council to relay findings
    return engine.commit_to_ledger(entry)

if __name__ == "__main__":
    # This forces Railway to use the correct Port
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
