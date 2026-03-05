import os
import uvicorn
import asyncio
import threading
from fastapi import FastAPI
from engine import OrionEngine

app = FastAPI(title="ORION_Ω_SOVEREIGN")
engine = OrionEngine()

def run_orion_thread():
    """Starts the engine in a dedicated, isolated event loop thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # This wakes up the 936 agents without touching the web server's loop
    loop.run_until_complete(engine.start_autonomous_system())
    loop.run_forever()

@app.on_event("startup")
async def startup_event():
    # We launch Orion in its own thread to stop the loop collision
    thread = threading.Thread(target=run_orion_thread, daemon=True)
    thread.start()

@app.get("/")
async def root():
    return {"status": "ORION_Ω_ONLINE", "message": "Sovereign Pulse Isolated"}

@app.get("/status")
async def status():
    return engine.get_state()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # Using the string "app:app" is critical for Railway's uvicorn worker
    uvicorn.run("app:app", host="0.0.0.0", port=port, workers=1)
