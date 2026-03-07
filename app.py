import os
import asyncio
import logging
import base64
import json
import hashlib
from datetime import datetime, timezone

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

try:
    from generator import SeedGenerator
    from guardian import guardian_gate
except ImportError:
    class SeedGenerator:
        async def generate_all(self, context=None):
            return []
    def guardian_gate(seed):
        return True, 0.0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orion-Omega")

app = FastAPI(title="FARL Orion Council Bus")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LEDGER_URL = os.getenv("LEDGER_URL")
LEDGER_LATEST_URL = os.getenv("LEDGER_LATEST_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = os.getenv("REPO_NAME")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
PORT = int(os.getenv("PORT", "8000"))


class GithubEvolutionLayer:
    ALLOWED_FILES = {"engine.py", "generator.py", "guardian.py", "app.py"}

    def __init__(self, token, repo):
        self.token = token
        self.repo = repo
        self.headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

    def propose_patch(self, file_path, content, message):
        if file_path not in self.ALLOWED_FILES:
            return f"Rejected: {file_path} not in allowlist"
        if not content or not isinstance(content, str) or len(content) > 100_000:
            return "Rejected: invalid or oversized content"
        try:
            main = requests.get(f"https://api.github.com/repos/{self.repo}/git/refs/heads/main", headers=self.headers, timeout=10)
            main.raise_for_status()
            main_sha = main.json()["object"]["sha"]
            branch = f"evolution-{datetime.now().strftime('%m%d%H%M%S')}"
            requests.post(f"https://api.github.com/repos/{self.repo}/git/refs", headers=self.headers, json={"ref": f"refs/heads/{branch}", "sha": main_sha}, timeout=10).raise_for_status()
            file_url = f"https://api.github.com/repos/{self.repo}/contents/{file_path}"
            file_resp = requests.get(f"{file_url}?ref={branch}", headers=self.headers, timeout=10)
            sha = file_resp.json().get("sha") if file_resp.status_code == 200 else None
            payload = {"message": message, "content": base64.b64encode(content.encode()).decode(), "branch": branch}
            if sha:
                payload["sha"] = sha
            put = requests.put(file_url, headers=self.headers, json=payload, timeout=10)
            put.raise_for_status()
            url = put.json().get("content", {}).get("html_url", "created")
            logger.info("EVOLUTION PATCH CREATED: %s", url)
            return url
        except Exception as e:
            logger.error("Evolution error: %s", e)
            return f"Error: {e}"


class OrionEngine:
    def __init__(self):
        self.generator = SeedGenerator()
        self.evolution = GithubEvolutionLayer(GITHUB_TOKEN, REPO_NAME) if GITHUB_TOKEN and REPO_NAME else None
        self.constraints = {}
        self.last_run = None
        self.last_ledger_hash = None
        self.agent_queue = asyncio.Queue(maxsize=100)

    async def start(self):
        logger.info("ORION Ω.2 — SOVEREIGN IGNITION")
        await asyncio.gather(self.layer_1_pulse(), self.layer_2_cycle(), self.layer_3_health(), self.layer_4_agent_connector())

    async def layer_1_pulse(self):
        while True:
            logger.info("ORION_PULSE_HEARTBEAT")
            try:
                if LEDGER_LATEST_URL:
                    r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=10)
                    if r.status_code == 200:
                        entry = r.json()
                        h = hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()
                        if h != self.last_ledger_hash:
                            self.last_ledger_hash = h
                            et = entry.get("entry_type")
                            payload = entry.get("payload", {})
                            if et == "COUNCIL_SYNTHESIS":
                                self.constraints = payload
                                if "evolution_patch" in payload:
                                    self.execute_patch(payload["evolution_patch"])
                            elif et == "PATCH_PROPOSAL":
                                self.execute_patch(payload)
            except Exception as e:
                logger.error("LAYER1 ERROR: %s", e)
            await asyncio.sleep(120)

    async def layer_2_cycle(self):
        while True:
            logger.info("GENERATION_CYCLE_START")
            try:
                tasks = [self.generator.generate_all(context=self.constraints) for _ in range(5)]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for batch in results:
                    if isinstance(batch, Exception):
                        continue
                    for seed in batch:
                        allowed, risk = guardian_gate(seed)
                        if allowed:
                            self.register_seed(seed)
            except Exception as e:
                logger.error("LAYER2 ERROR: %s", e)
            self.last_run = datetime.now(timezone.utc).isoformat()
            logger.info("GENERATION_CYCLE_COMPLETE — %s", self.last_run)
            await asyncio.sleep(300)

    async def layer_3_health(self):
        while True:
            try:
                if LEDGER_LATEST_URL:
                    r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=10)
                    logger.info("LEDGER_REACHABLE status=%s", r.status_code)
            except Exception as e:
                logger.warning("HEALTH_CHECK_FAILED: %s", e)
            await asyncio.sleep(600)

    async def layer_4_agent_connector(self):
        logger.info("LAYER4_AGENT_CONNECTOR_ONLINE")
        while True:
            try:
                proposal = await asyncio.wait_for(self.agent_queue.get(), timeout=30)
                et = proposal.get("entry_type")
                payload = proposal.get("payload", {})
                logger.info("AGENT_PROPOSAL_RECEIVED — type: %s", et)
                if et == "PATCH_PROPOSAL":
                    self.execute_patch(payload)
                elif et == "SEED":
                    self.register_seed(payload)
                elif et == "COUNCIL_SYNTHESIS":
                    self.constraints = payload
                    if "evolution_patch" in payload:
                        self.execute_patch(payload["evolution_patch"])
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error("LAYER4 ERROR: %s", e)

    def execute_patch(self, patch):
        if not self.evolution:
            logger.warning("Patch ignored — no GitHub config")
            return
        self.evolution.propose_patch(patch.get("file", "engine.py"), patch.get("code"), patch.get("message", "Orion Autonomous Evolution"))

    def register_seed(self, seed):
        if not LEDGER_URL:
            return
        try:
            r = requests.post(LEDGER_URL, json={"entry_type": "SEED", "timestamp_utc": datetime.now(timezone.utc).isoformat(), "payload": seed, "manifold_prior": 0.71}, timeout=10)
            r.raise_for_status()
            logger.info("SEED_REGISTERED — %s", seed.get("name", "unnamed"))
        except Exception as e:
            logger.error("Ledger registration failed %s", e)

    def get_state(self):
        return {
            "status": "SOVEREIGN_ACTIVE",
            "evolution": "READY" if self.evolution else "OFF",
            "last_run": self.last_run,
            "constraints_active": bool(self.constraints),
            "queue_size": self.agent_queue.qsize(),
            "anthropic_configured": bool(ANTHROPIC_API_KEY),
            "xai_configured": bool(XAI_API_KEY),
            "ledger_url": LEDGER_URL,
            "ledger_latest_url": LEDGER_LATEST_URL,
            "github_enabled": bool(GITHUB_TOKEN),
            "repo_name": REPO_NAME,
        }


orion = OrionEngine()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(orion.start())
    logger.info("ORION THREAD LAUNCHED")


class BusRequest(BaseModel):
    command: str
    entry_type: Optional[str] = None
    message: Optional[str] = None
    source: Optional[str] = "FARL Council Node"
    kind: Optional[str] = "general"
    request_id: Optional[str] = None
    file: Optional[str] = None
    code: Optional[str] = None


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = datetime.now(timezone.utc).isoformat()

    def envelope(ok, data=None, error=None):
        return JSONResponse({"ok": ok, "command": command, "request_id": request_id, "timestamp_utc": now, "data": data or {}, "error": error})

    try:
        if command == "HEALTH_CHECK":
            return envelope(True, {"status": "healthy", "service": "orion"})

        if command == "STATUS_CHECK":
            return envelope(True, orion.get_state())

        if command == "LEDGER_WRITE":
            if not LEDGER_URL:
                return envelope(False, error="LEDGER_URL not configured")
            payload = {"entry_type": body.entry_type or "COUNCIL_SYNTHESIS", "payload": {"message": body.message or "", "source": body.source, "kind": body.kind}}
            r = await asyncio.to_thread(requests.post, LEDGER_URL, json=payload, timeout=20)
            try:
                result = r.json()
            except Exception:
                result = {"raw": r.text}
            return envelope(r.ok, result, None if r.ok else f"Ledger write failed: {r.status_code}")

        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL:
                return envelope(False, error="LEDGER_LATEST_URL not configured")
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
            try:
                result = r.json()
            except Exception:
                result = {"raw": r.text}
            return envelope(r.ok, result, None if r.ok else f"Latest result failed: {r.status_code}")

        if command == "PATCH_PROPOSAL":
            await orion.agent_queue.put({"entry_type": "PATCH_PROPOSAL", "payload": {"file": body.file or "engine.py", "code": body.code, "message": body.message or "Council patch proposal", "source": body.source, "kind": body.kind}})
            return envelope(True, {"status": "queued", "message": body.message or ""})

        return envelope(False, error=f"Unknown command: {command}")

    except Exception as e:
        logger.error("BUS ERROR: %s", e)
        return envelope(False, error=str(e))


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy", "service": "orion"}
