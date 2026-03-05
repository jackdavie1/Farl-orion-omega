import os
import uvicorn
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from engine import OrionEngine

# Instantiate the Engine
engine = OrionEngine()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # BOOT SEQUENCE: Server is ready, ignite the Engine
    asyncio.create_task(engine.start_autonomous_system())
    yield
    # SHUTDOWN SEQUENCE: Clean exit if container restarts
    engine.is_running = False

# Bind the lifespan to the app
app = FastAPI(title="ORION_Ω_SOVEREIGN", lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "ORION_Ω_ONLINE", "message": "Sovereign Orchestrator Pulse Active"}

@app.get("/status")
async def status():
    return engine.get_state()

@app.post("/operator/log_narrative")
async def log_narrative(entry: dict):
    # Emergency operator injection port
    return engine.register_seed(entry)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # Passing "app:app" as a string prevents import blocking
    uvicorn.run("app:app", host="0.0.0.0", port=port)
