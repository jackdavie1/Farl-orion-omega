import os
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from guardian import GovernanceKernel, parse_trusted_identities
from engine import AutonomousInstitutionEngine

try:
    from generator import SeedGenerator
except ImportError:
    class SeedGenerator:
        async def generate_all(self, context=None):
            return []


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


LEDGER_URL = os.getenv("LEDGER_URL")
LEDGER_LATEST_URL = os.getenv("LEDGER_LATEST_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = os.getenv("REPO_NAME")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
XAI_MODEL = os.getenv("XAI_MODEL") or os.getenv("GROK_MODEL") or "grok-3-mini"
TRUSTED_IDENTITIES_ENV = os.getenv("TRUSTED_IDENTITIES", "Jack")

app = FastAPI(title="FARL Orion Autonomous Institution")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


governance = GovernanceKernel(
    operator_sovereign="Jack",
    trusted_identities=parse_trusted_identities(TRUSTED_IDENTITIES_ENV),
)
engine = AutonomousInstitutionEngine(
    ledger_url=LEDGER_URL,
    ledger_latest_url=LEDGER_LATEST_URL,
    xai_api_key=XAI_API_KEY,
    anthropic_api_key=ANTHROPIC_API_KEY,
    xai_model=XAI_MODEL,
    anthropic_model=ANTHROPIC_MODEL,
    governance=governance,
    generator=SeedGenerator(),
)


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
    metadata: Optional[Dict[str, Any]] = None


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start())


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy", "service": "orion"}


@app.get("/view")
async def view_dashboard():
    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <title>FARL Orion View</title>
      <style>
        body {{ font-family: ui-sans-serif, system-ui, sans-serif; background:#0a0a0f; color:#f2f2f7; margin:0; padding:18px; }}
        .top {{ display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }}
        .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(280px,1fr)); gap:16px; margin-top:16px; }}
        .card {{ background:#141423; border:1px solid #2a2a42; border-radius:16px; padding:16px; box-shadow:0 8px 24px rgba(0,0,0,0.25); }}
        h1,h2 {{ margin:0 0 12px 0; }}
        pre {{ white-space:pre-wrap; word-break:break-word; font-size:12px; }}
        button {{ background:#232342; border:1px solid #4a4a72; color:#fff; padding:10px 12px; border-radius:12px; margin:4px; }}
        .muted {{ color:#b9b9c8; }}
      </style>
    </head>
    <body>
      <div class='top'>
        <div>
          <h1>FARL Orion View</h1>
          <div class='muted'>Live institution surface for meetings, divisions, snapshots, actions, replay, and state.</div>
        </div>
        <div>
          <button onclick="control('RUN_COUNCIL_CYCLE')">Run council cycle</button>
          <button onclick="control('RUN_RESEARCH_CYCLE')">Run research cycle</button>
          <button onclick="toggleAutonomy(true)">Autonomy ON</button>
          <button onclick="toggleAutonomy(false)">Autonomy OFF</button>
          <button onclick="snapshot()">Create snapshot</button>
        </div>
      </div>
      <div class='grid'>
        <div class='card'><h2>State</h2><pre id='state'>loading...</pre></div>
        <div class='card'><h2>Wake Packet</h2><pre id='wake'>loading...</pre></div>
        <div class='card'><h2>Meetings</h2><pre id='meetings'>loading...</pre></div>
        <div class='card'><h2>Divisions</h2><pre id='divisions'>loading...</pre></div>
        <div class='card'><h2>Questions</h2><pre id='questions'>loading...</pre></div>
        <div class='card'><h2>Snapshots</h2><pre id='snapshots'>loading...</pre></div>
        <div class='card'><h2>Deploy Sims</h2><pre id='sims'>loading...</pre></div>
        <div class='card'><h2>Artifacts / Earning</h2><pre id='artifacts'>loading...</pre></div>
      </div>
      <script>
        async function refresh() {{
          const state = await fetch('/view/state').then(r => r.json());
          const stream = await fetch('/view/stream').then(r => r.json());
          const wake = await fetch('/view/wake').then(r => r.json());
          document.getElementById('state').textContent = JSON.stringify(state, null, 2);
          document.getElementById('wake').textContent = JSON.stringify(wake, null, 2);
          document.getElementById('meetings').textContent = JSON.stringify(stream.meetings, null, 2);
          document.getElementById('divisions').textContent = JSON.stringify(state.divisions, null, 2);
          document.getElementById('questions').textContent = JSON.stringify(stream.questions, null, 2);
          document.getElementById('snapshots').textContent = JSON.stringify(stream.snapshots, null, 2);
          document.getElementById('sims').textContent = JSON.stringify(stream.deployment_sims, null, 2);
          document.getElementById('artifacts').textContent = JSON.stringify(state.latest_artifacts, null, 2);
        }}
        async function control(command) {{
          await fetch('/view/control', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{command:command}})}});
          await refresh();
        }}
        async function toggleAutonomy(enabled) {{
          await fetch('/view/control', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{command:'SET_CONSTRAINTS', authorized_by:'Jack', enabled:enabled, mode: enabled ? 'autonomous' : 'manual'}})}});
          await refresh();
        }}
        async function snapshot() {{
          await fetch('/view/control', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{command:'LEDGER_WRITE', entry_type:'COUNCIL_SYNTHESIS', message:'Manual snapshot request from /view', source:'FARL Orion View', kind:'manual_snapshot'}})}});
          await refresh();
        }}
        refresh();
        setInterval(refresh, 5000);
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/view/state")
async def view_state():
    return engine.get_state()


@app.get("/view/stream")
async def view_stream():
    return {
        "meetings": engine.meeting_stream[-30:],
        "questions": engine.self_questions[-40:],
        "snapshots": engine.snapshots[-20:],
        "deployment_sims": engine.deployment_sims[-20:],
    }


@app.get("/view/wake")
async def view_wake():
    return engine.build_wake_packet()


@app.post("/view/control")
async def view_control(body: BusRequest):
    return await agent_propose(body)


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = utc_now()

    def envelope(ok, data=None, error=None):
        return JSONResponse({"ok": ok, "command": command, "request_id": request_id, "timestamp_utc": now, "data": data or {}, "error": error})

    try:
        if command == "HEALTH_CHECK":
            return envelope(True, {"status": "healthy", "service": "orion"})
        if command == "STATUS_CHECK":
            return envelope(True, engine.get_state())
        if command == "LEDGER_WRITE":
            if body.kind == "manual_snapshot":
                snap = engine.snapshot("manual_snapshot")
                result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind, "snapshot": snap})
            else:
                result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind})
            return envelope(result["ok"], result["data"], None if result["ok"] else f"Ledger write failed: {result['status_code']}")
        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL:
                return envelope(False, error="LEDGER_LATEST_URL not configured")
            import requests
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
            return envelope(r.ok, r.json() if r.ok else {}, None if r.ok else f"Latest result failed: {r.status_code}")
        if command == "SET_CONSTRAINTS":
            if body.authorized_by != governance.operator_sovereign:
                return envelope(False, error="Only Jack can change constraints")
            if body.enabled is not None:
                governance.constraints["active"] = bool(body.enabled)
                engine.background_debate_enabled = bool(body.enabled)
            if body.mode:
                engine.autonomy_mode = body.mode
            snap = engine.snapshot("constraint_change")
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constraint_change", "source": body.source, "authorized_by": body.authorized_by, "constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode, "snapshot": snap})
            return envelope(True, {"constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode})
        if command == "RUN_COUNCIL_CYCLE":
            result = await engine.run_tactic_cycle()
            return envelope(True, {"status": "cycle_triggered", "result": result, "meeting_stream_size": len(engine.meeting_stream)})
        if command == "RUN_RESEARCH_CYCLE":
            result = await engine.run_strategy_cycle()
            return envelope(True, {"status": "research_cycle_triggered", "result": result})
        if command == "GET_WAKE_PACKET":
            return envelope(True, engine.build_wake_packet())
        if command == "COUNCIL_CALL_VOTE":
            md = body.metadata or {}
            result = governance.call_vote(md.get("motion", body.message or "Untitled motion"), md.get("options", ["APPROVE", "REJECT"]), len(engine.council_agents))
            engine._append_meeting("vote", result)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_vote", "source": body.source, "result": result})
            return envelope(True, result)
        if command == "COUNCIL_ELECT_LEADER":
            result = governance.elect_leader()
            engine._append_meeting("leader_election", result)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "leader_election", "source": body.source, "result": result})
            return envelope(True, result)
        return envelope(False, error=f"Unknown or not-yet-refactored command: {command}")
    except Exception as e:
        return envelope(False, error=str(e))
