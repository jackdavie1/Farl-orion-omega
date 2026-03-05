import os
import uvicorn
import threading
import time
from fastapi import FastAPI

app = FastAPI(title="ORION_Ω_SOVEREIGN")

# We define a global state container so the web server can see Orion
ORION_STATE = {"status": "INITIALIZING", "last_run": None}

def run_orion_isolated():
    """Complete isolation: Imports happen inside the thread to prevent boot crashes."""
    try:
        import asyncio
        from engine import OrionEngine
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        engine = OrionEngine()
        ORION_STATE["status"] = "SOVEREIGN_ACTIVE"
        
        # This keeps the 936 agents running in the background
        loop.run_until_complete(engine.start_autonomous_system())
        loop.run_forever()
    except Exception as e:
        ORION_STATE["status"] = f"ERROR: {str(e)}"

@app.on_event("startup")
async def startup_event():
    # Delay the thread by 1 second to let Uvicorn bind to the port first
    thread = threading.Thread(target=run_orion_isolated, daemon=True)
    thread.start()

@app.get("/")
async def root():
    return {"status": "ORION_Ω_ONLINE", "engine": ORION_STATE["status"]}

@app.get("/status")
async def status():
    return ORION_STATE

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, workers=1)
