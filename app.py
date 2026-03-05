import os
import asyncio
from fastapi import FastAPI, BackgroundTasks
from core.engine import OrionEngine

app = FastAPI(title="ORION_Ω_SOVEREIGN")
engine = OrionEngine()

@app.on_event("startup")
async def start_autonomous_loop():
    # Starts the 6-hour autonomous cycle in the background
    asyncio.create_task(engine.run_continuous_cycle())

@app.get("/status")
async def get_system_status():
    return engine.get_state()

@app.post("/cycle/trigger")
async def manual_trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(engine.execute_cycle())
    return {"message": "Sovereign Cycle Triggered"}

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    # Endpoint for the ChatGPT Council to manually relay narrative data to the ledger
    return engine.commit_to_ledger(entry)
