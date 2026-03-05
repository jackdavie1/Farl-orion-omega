import os
import uvicorn
import asyncio
import threading
from fastapi import FastAPI
from engine import OrionEngine

app = FastAPI(title="ORION_Ω_SOVEREIGN")
engine = OrionEngine()

def run_orion_isolated():
    """Wakes the 936 agents in a dedicated thread to prevent loop collisions."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # This runs the engine cycles until the container stops
    loop.run_until_complete(engine.start_autonomous_system())
    loop.run_forever()

@app.on_event("startup")
async def startup_event():
    # Spawning Orion in an independent thread
    thread = threading.Thread(target=run_orion_isolated, daemon=True)
    thread.start()

@app.get("/")
async def root():
    return {"status": "ORION_Ω_ONLINE", "message": "Thread Isolation Verified"}

@app.get("/status")
async def status():
    return engine.get_state()

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    return engine.register_seed(entry)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # Using the string "app:app" is mandatory for stability in Python 3.13
    uvicorn.run("app:app", host="0.0.0.0", port=port, workers=1)
