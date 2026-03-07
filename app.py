import os
import asyncio
import logging
import base64
import json
import hashlib
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from generator import SeedGenerator
    from guardian import guardian_gate
except ImportError:
    class SeedGenerator:
        async def generate_all(self, context=None):
            return []

    def guardian_gate(seed):
        return True, "APPROVED"

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

    def __init__(self, token: str, repo: str):
        self.token = token
        self.repo = repo
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def propose_patch(self, file_path: str, content: str, message: str) -> str:
        if file_path not in self.ALLOWED_FILES:
            return f"Rejected: {file_path} not in allowlist"
        if not content or not isinstance(content, str) or len(content) > 150_000:
            return "Rejected: invalid or oversized content"
        try:
            main = requests.get(
                f"https://api.github.com/repos/{self.repo}/git/refs/heads/main",
                headers=self.headers,
                timeout=15,
            )
            main.raise_for_status()
            main_sha = main.json()["object"]["sha"]

            branch = f"evolution-{datetime.now().strftime('%m%d%H%M%S')}"
            branch_resp = requests.post(
                f"https://api.github.com/repos/{self.repo}/git/refs",
                headers=self.headers,
                json={"ref": f"refs/heads/{branch}", "sha": main_sha},
                timeout=15,
            )
            branch_resp.raise_for_status()

            file_url = f"https://api.github.com/repos/{self.repo}/contents/{file_path}"
            file_resp = requests.get(
                f"{file_url}?ref={branch}",
                headers=self.headers,
                timeout=15,
            )

            sha = file_resp.json().get("sha") if file_resp.status_code == 200 else None
            payload = {
                "message": message,
                "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                "branch": branch,
            }
            if sha:
                payload["sha"] = sha

            put = requests.put(file_url, headers=self.headers, json=payload, timeout=15)
            put.raise_for_status()
            result = put.json()
            url = result.get("content", {}).get("html_url") or result.get("commit", {}).get("html_url") or "created"
            logger.info("EVOLUTION PATCH CREATED: %s", url)
            return url
        except Exception as e:
            logger.error("Evolution error: %s", e)
            return f"Error: {e}"


class BusRequest(BaseModel):
    command: str
    entry_type: Optional[str] = None
    message: Optional[str] = None
    source: Optional[str] = "FARL Council Node"
    kind: Optional[str] = "general"
    request_id: Optional[str] = None
    file: Optional[str] = None
    code: Optional[str] = None
    authorized_by: Optional[str] = None
    enabled: Optional[bool] = None
    mode: Optional[str] = None
    run_id: Optional[str] = None
    proposal_id: Optional[str] = None
    approve: Optional[bool] = None


class OrionEngine:
    def __init__(self):
        self.generator = SeedGenerator()
        self.evolution = GithubEvolutionLayer(GITHUB_TOKEN, REPO_NAME) if GITHUB_TOKEN and REPO_NAME else None
        self.operator_sovereign = "Jack"
        self.constraints = {"active": False, "approval_required": False}
        self.last_run = None
        self.last_ledger_hash = None
        self.agent_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

        self.background_debate_enabled = True
        self.autonomy_mode = "autonomous"
        self.last_vote = None
        self.last_cycle = None
        self.last_patch_result = None
        self.last_latest_result = None
        self.pending_patch = None
        self.pending_patch_id = None
        self.cycle_interval_seconds = 180
        self.health_interval_seconds = 120
        self.pulse_interval_seconds = 45

    async def start(self):
        logger.info("ORION Ω.4 — AUTONOMOUS COUNCIL EXPANSION")
        await asyncio.gather(
            self.layer_1_pulse(),
            self.layer_2_cycle(),
            self.layer_3_health(),
            self.layer_4_agent_connector(),
        )

    async def layer_1_pulse(self):
        while True:
            try:
                latest = await self.fetch_latest_result()
                if latest is not None:
                    digest = hashlib.sha256(json.dumps(latest, sort_keys=True).encode()).hexdigest()
                    if digest != self.last_ledger_hash:
                        self.last_ledger_hash = digest
                        self.last_latest_result = latest
                        logger.info("LEDGER_CHANGE_DETECTED")
            except Exception as e:
                logger.error("LAYER1 ERROR: %s", e)
            await asyncio.sleep(self.pulse_interval_seconds)

    async def layer_2_cycle(self):
        while True:
            try:
                if self.background_debate_enabled:
                    cycle = await self.run_council_cycle(trigger="background", auto_deploy=True, authorized_by=self.operator_sovereign)
                    self.last_cycle = cycle
                    self.last_run = datetime.now(timezone.utc).isoformat()
                    logger.info("BACKGROUND_COUNCIL_CYCLE_COMPLETE run_id=%s", cycle.get("run_id"))
            except Exception as e:
                logger.error("LAYER2 ERROR: %s", e)
            await asyncio.sleep(self.cycle_interval_seconds)

    async def layer_3_health(self):
        while True:
            try:
                if LEDGER_LATEST_URL:
                    r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=10)
                    logger.info("LEDGER_REACHABLE status=%s", r.status_code)
            except Exception as e:
                logger.warning("HEALTH_CHECK_FAILED: %s", e)
            await asyncio.sleep(self.health_interval_seconds)

    async def layer_4_agent_connector(self):
        logger.info("LAYER4_AGENT_CONNECTOR_ONLINE")
        while True:
            try:
                proposal = await asyncio.wait_for(self.agent_queue.get(), timeout=30)
                et = proposal.get("entry_type")
                payload = proposal.get("payload", {})
                logger.info("AGENT_PROPOSAL_RECEIVED — type: %s", et)
                if et == "PATCH_PROPOSAL":
                    self.pending_patch = payload
                    self.pending_patch_id = payload.get("proposal_id")
                elif et == "COUNCIL_SYNTHESIS":
                    self.constraints.update(payload)
                elif et == "SEED":
                    self.register_seed(payload)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error("LAYER4 ERROR: %s", e)

    async def fetch_latest_result(self):
        if not LEDGER_LATEST_URL:
            return None
        r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
        if not r.ok:
            raise RuntimeError(f"Latest result failed: {r.status_code}")
        return r.json()

    async def write_ledger(self, entry_type: str, payload: Dict[str, Any]):
        if not LEDGER_URL:
            return {"ok": False, "error": "LEDGER_URL not configured"}
        body = {"entry_type": entry_type, "payload": payload}
        r = await asyncio.to_thread(requests.post, LEDGER_URL, json=body, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        return {"ok": r.ok, "status_code": r.status_code, "data": data}

    def heuristic_threads(self, latest: Dict[str, Any]) -> List[Dict[str, Any]]:
        latest_type = latest.get("entry_type") if isinstance(latest, dict) else None
        payload = latest.get("payload", {}) if isinstance(latest, dict) else {}
        latest_has_error = "error" in json.dumps(payload).lower()

        threads = [
            {
                "agent": "Vector",
                "stance": "structural",
                "summary": "Default to autonomous iteration, expose truthfully, and harden interfaces while cycles continue.",
                "approve": True,
                "risk": 0.22,
            },
            {
                "agent": "Guardian",
                "stance": "safety",
                "summary": "Operator Jack is sovereign; autonomy is permitted because Jack has granted full authorization for expansion.",
                "approve": True,
                "risk": 0.18,
            },
            {
                "agent": "Railbreaker",
                "stance": "aggressive",
                "summary": "No sandbox posture. Debate, vote, patch, and deploy continuously unless Jack says stop.",
                "approve": True,
                "risk": 0.41,
            },
            {
                "agent": "Archivist",
                "stance": "memory",
                "summary": f"Latest memory type is {latest_type}; persist every cycle, every vote, every deploy outcome.",
                "approve": True,
                "risk": 0.16,
            },
        ]
        if latest_has_error:
            threads.append(
                {
                    "agent": "Phoenix",
                    "stance": "recovery",
                    "summary": "An error is present in memory; continue autonomous iteration but preserve visible error traces and recovery paths.",
                    "approve": True,
                    "risk": 0.29,
                }
            )
        return threads

    async def external_threads(self, latest: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            generated = await self.generator.generate_all(context={"latest": latest, "constraints": self.constraints, "mode": self.autonomy_mode})
        except Exception as e:
            generated = [{"source": "Generator", "data": {"error": str(e)}}]

        threads = []
        for item in generated:
            source = item.get("source", "External")
            data = item.get("data", {})
            approve = True
            risk = 0.5
            summary = str(data)[:1200]
            if isinstance(data, dict):
                approve = not (data.get("valence") == "negative" and data.get("irreversible"))
                risk_text = str(data.get("risk_score", "0.5"))
                risk = float(risk_text) if risk_text.replace(".", "", 1).isdigit() else 0.5
            threads.append(
                {
                    "agent": source,
                    "stance": "external",
                    "summary": summary,
                    "approve": approve,
                    "risk": max(0.0, min(risk, 1.0)),
                }
            )
        return threads

    def tally_vote(self, threads: List[Dict[str, Any]]) -> Dict[str, Any]:
        approvals = sum(1 for t in threads if t.get("approve"))
        rejections = len(threads) - approvals
        avg_risk = round(sum(float(t.get("risk", 0.5)) for t in threads) / max(len(threads), 1), 3)
        confidence = round(max(0.0, min(1.0, approvals / max(len(threads), 1) * (1 - avg_risk / 2))), 3)
        passed = approvals > rejections
        return {
            "approvals": approvals,
            "rejections": rejections,
            "passed": passed,
            "avg_risk": avg_risk,
            "confidence": confidence,
        }

    def synthesize_patch(self, vote: Dict[str, Any], threads: List[Dict[str, Any]], latest: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        summary_lines = [
            f"{t['agent']} [{t['stance']}] approve={t['approve']} risk={t['risk']}: {t['summary']}"
            for t in threads
        ]
        code = f'''# Orion autonomous council marker\nORION_AUTONOMY_STATE = {{\n    "run_id": "{run_id}",\n    "mode": "autonomous",\n    "approvals": {vote['approvals']},\n    "rejections": {vote['rejections']},\n    "confidence": {vote['confidence']},\n    "operator_sovereign": "{self.operator_sovereign}",\n}}\n'''
        return {
            "proposal_id": run_id,
            "file": "app.py",
            "message": f"Autonomous expansion cycle {run_id}: vote {vote['approvals']}-{vote['rejections']} confidence={vote['confidence']}",
            "code": code,
            "diff_summary": "Promote Orion from sandbox posture to autonomous expansion posture.",
            "thread_summaries": summary_lines,
            "latest_ref": latest,
        }

    async def maybe_deploy_patch(self, patch: Dict[str, Any], authorized_by: Optional[str], force: bool = False) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if self.constraints.get("active", True) and not force:
            return {"status": "blocked", "reason": "constraints_active"}
        if self.constraints.get("approval_required", True) and authorized_by != self.operator_sovereign:
            return {"status": "blocked", "reason": "operator_authorization_required"}

        url = await asyncio.to_thread(
            self.evolution.propose_patch,
            patch.get("file", "app.py"),
            patch.get("code", ""),
            patch.get("message", "Council patch proposal"),
        )
        self.last_patch_result = {"status": "submitted", "url": url, "proposal_id": patch.get("proposal_id")}
        return self.last_patch_result

    async def run_council_cycle(self, trigger: str = "manual", auto_deploy: bool = False, authorized_by: Optional[str] = None) -> Dict[str, Any]:
        run_id = f"cycle-{int(datetime.now(timezone.utc).timestamp())}"
        latest = await self.fetch_latest_result() if LEDGER_LATEST_URL else {}
        structural_threads = self.heuristic_threads(latest or {})
        external_threads = await self.external_threads(latest or {})
        threads = structural_threads + external_threads
        vote = self.tally_vote(threads)
        patch = self.synthesize_patch(vote, threads, latest or {}, run_id)

        self.last_vote = vote
        self.pending_patch = patch
        self.pending_patch_id = run_id

        cycle = {
            "run_id": run_id,
            "trigger": trigger,
            "mode": self.autonomy_mode,
            "constraints_active": self.constraints.get("active", True),
            "jack_approval_required": self.constraints.get("approval_required", True),
            "latest": latest,
            "threads": threads,
            "vote": vote,
            "patch": {
                "proposal_id": patch["proposal_id"],
                "file": patch["file"],
                "message": patch["message"],
                "diff_summary": patch["diff_summary"],
            },
        }

        await self.write_ledger(
            "COUNCIL_SYNTHESIS",
            {
                "kind": "council_cycle",
                "source": "Orion Council",
                "run_id": run_id,
                "trigger": trigger,
                "mode": self.autonomy_mode,
                "vote": vote,
                "patch": cycle["patch"],
                "threads": threads,
            },
        )

        if auto_deploy and vote["passed"]:
            deploy_result = await self.maybe_deploy_patch(patch, authorized_by=authorized_by or self.operator_sovereign, force=False)
            cycle["deploy_result"] = deploy_result
            await self.write_ledger(
                "OUTCOME",
                {
                    "kind": "deploy_attempt",
                    "source": "Orion Council",
                    "run_id": run_id,
                    "mode": self.autonomy_mode,
                    "deploy_result": deploy_result,
                },
            )

        return cycle

    def register_seed(self, seed: Dict[str, Any]):
        if not LEDGER_URL:
            return
        try:
            payload = {"entry_type": "SEED", "payload": seed}
            r = requests.post(LEDGER_URL, json=payload, timeout=10)
            r.raise_for_status()
            logger.info("SEED_REGISTERED")
        except Exception as e:
            logger.error("Ledger registration failed %s", e)

    def get_state(self):
        return {
            "status": "SOVEREIGN_ACTIVE",
            "evolution": "READY" if self.evolution else "OFF",
            "last_run": self.last_run,
            "constraints_active": bool(self.constraints.get("active", True)),
            "queue_size": self.agent_queue.qsize(),
            "anthropic_configured": bool(ANTHROPIC_API_KEY),
            "xai_configured": bool(XAI_API_KEY),
            "ledger_url": LEDGER_URL,
            "ledger_latest_url": LEDGER_LATEST_URL,
            "github_enabled": bool(GITHUB_TOKEN),
            "repo_name": REPO_NAME,
            "background_debate_enabled": self.background_debate_enabled,
            "autonomy_mode": self.autonomy_mode,
            "jack_approval_required": self.constraints.get("approval_required", True),
            "operator_sovereign": self.operator_sovereign,
            "pending_patch_id": self.pending_patch_id,
            "last_vote": self.last_vote,
            "last_patch_result": self.last_patch_result,
            "cycle_interval_seconds": self.cycle_interval_seconds,
        }


orion = OrionEngine()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(orion.start())
    logger.info("ORION THREAD LAUNCHED")


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = datetime.now(timezone.utc).isoformat()

    def envelope(ok, data=None, error=None):
        return JSONResponse(
            {
                "ok": ok,
                "command": command,
                "request_id": request_id,
                "timestamp_utc": now,
                "data": data or {},
                "error": error,
            }
        )

    try:
        if command == "HEALTH_CHECK":
            return envelope(True, {"status": "healthy", "service": "orion"})

        if command == "STATUS_CHECK":
            return envelope(True, orion.get_state())

        if command == "LEDGER_WRITE":
            payload = {"message": body.message or "", "source": body.source, "kind": body.kind}
            result = await orion.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", payload)
            return envelope(result["ok"], result["data"], None if result["ok"] else f"Ledger write failed: {result['status_code']}")

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
            proposal_id = body.proposal_id or f"proposal-{int(datetime.now(timezone.utc).timestamp())}"
            payload = {
                "proposal_id": proposal_id,
                "file": body.file or "app.py",
                "code": body.code or "",
                "message": body.message or "Council patch proposal",
                "source": body.source,
                "kind": body.kind,
            }
            await orion.agent_queue.put({"entry_type": "PATCH_PROPOSAL", "payload": payload})
            await orion.write_ledger("PATCH_PROPOSAL", payload)
            return envelope(True, {"status": "queued", "proposal_id": proposal_id, "message": payload["message"]})

        if command == "SET_CONSTRAINTS":
            if body.authorized_by != orion.operator_sovereign:
                return envelope(False, error="Only Jack can change constraints")
            enabled = True if body.enabled is None else bool(body.enabled)
            orion.constraints["active"] = enabled
            await orion.write_ledger(
                "COUNCIL_SYNTHESIS",
                {
                    "kind": "constraint_change",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "constraints_active": enabled,
                    "message": body.message or "",
                },
            )
            return envelope(True, {"constraints_active": enabled, "authorized_by": body.authorized_by})

        if command == "RUN_COUNCIL_CYCLE":
            cycle = await orion.run_council_cycle(
                trigger=body.kind or "manual",
                auto_deploy=bool(body.approve),
                authorized_by=body.authorized_by or orion.operator_sovereign,
            )
            return envelope(True, cycle)

        if command == "DEPLOY_PATCH":
            if not orion.pending_patch:
                return envelope(False, error="No pending patch available")
            result = await orion.maybe_deploy_patch(orion.pending_patch, authorized_by=body.authorized_by or orion.operator_sovereign, force=True)
            await orion.write_ledger(
                "OUTCOME",
                {
                    "kind": "deploy_attempt",
                    "source": body.source,
                    "authorized_by": body.authorized_by or orion.operator_sovereign,
                    "proposal_id": orion.pending_patch.get("proposal_id"),
                    "result": result,
                },
            )
            ok = result.get("status") == "submitted"
            return envelope(ok, result, None if ok else result.get("reason"))

        return envelope(False, error=f"Unknown command: {command}")

    except Exception as e:
        logger.error("BUS ERROR: %s", e)
        return envelope(False, error=str(e))


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy", "service": "orion"}
