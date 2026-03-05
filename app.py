import os
import asyncio
from fastapi import FastAPI, BackgroundTasks
# No folder names, just direct imports
from engine import OrionEngine 

app = FastAPI(title="ORION_Ω_FLAT")
engine = OrionEngine()

@app.on_event("startup")
async def start_autonomous_loop():
    asyncio.create_task(engine.run_continuous_cycle())

@app.get("/status")
async def get_system_status():
    return engine.get_state()

@app.post("/cycle/trigger")
async def manual_trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(engine.execute_cycle)
    return {"message": "Sovereign Cycle Triggered"}

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    return engine.commit_to_ledger(entry)
