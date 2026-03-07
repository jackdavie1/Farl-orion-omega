import os
import asyncio
import base64
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
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


def github_ready() -> bool:
    return bool(GITHUB_TOKEN and REPO_NAME)


def github_headers() -> Dict[str, str]:
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def github_get_file_sha(file_path: str, ref: str = "main") -> Optional[str]:
    r = await asyncio.to_thread(
        requests.get,
        f"https://api.github.com/repos/{REPO_NAME}/contents/{file_path}?ref={ref}",
        headers=github_headers(),
        timeout=20,
    )
    if r.status_code == 200:
        return r.json().get("sha")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return None


async def github_put_file(file_path: str, content: str, message: str, branch: str = "main") -> Dict[str, Any]:
    sha = await github_get_file_sha(file_path, branch)
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha
    r = await asyncio.to_thread(
        requests.put,
        f"https://api.github.com/repos/{REPO_NAME}/contents/{file_path}",
        headers=github_headers(),
        json=payload,
        timeout=25,
    )
    r.raise_for_status()
    return r.json()


async def github_create_pull_request(title: str, head: str, base: str = "main", body: str = "") -> Dict[str, Any]:
    r = await asyncio.to_thread(
        requests.post,
        f"https://api.github.com/repos/{REPO_NAME}/pulls",
        headers=github_headers(),
        json={"title": title, "head": head, "base": base, "body": body},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


async def github_merge_pull_request(number: int, commit_title: str, merge_method: str = "squash") -> Dict[str, Any]:
    r = await asyncio.to_thread(
        requests.put,
        f"https://api.github.com/repos/{REPO_NAME}/pulls/{number}/merge",
        headers=github_headers(),
        json={"commit_title": commit_title, "merge_method": merge_method},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


async def github_rollback_to_commit(commit_sha: str) -> Dict[str, Any]:
    r = await asyncio.to_thread(
        requests.patch,
        f"https://api.github.com/repos/{REPO_NAME}/git/refs/heads/main",
        headers=github_headers(),
        json={"sha": commit_sha, "force": True},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start())


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy", "service": "orion"}


@app.get("/view")
async def view_dashboard():
    html = """
    <!doctype html>
    <html>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <meta http-equiv='Cache-Control' content='no-cache, no-store, must-revalidate'>
      <meta http-equiv='Pragma' content='no-cache'>
      <meta http-equiv='Expires' content='0'>
      <title>FARL Orion View</title>
      <style>
        body { font-family: ui-sans-serif, system-ui, sans-serif; background:#0a0a0f; color:#f2f2f7; margin:0; padding:18px; }
        .top { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
        .grid { display:grid; grid-template-columns: repeat(auto-fit,minmax(280px,1fr)); gap:16px; margin-top:16px; }
        .card { background:#141423; border:1px solid #2a2a42; border-radius:16px; padding:16px; box-shadow:0 8px 24px rgba(0,0,0,0.25); }
        h1,h2 { margin:0 0 12px 0; }
        pre { white-space:pre-wrap; word-break:break-word; font-size:12px; }
        button { background:#232342; border:1px solid #4a4a72; color:#fff; padding:10px 12px; border-radius:12px; margin:4px; cursor:pointer; }
        .muted { color:#b9b9c8; }
        .statusline { margin-top:8px; font-size:12px; color:#9ecbff; }
      </style>
    </head>
    <body>
      <div class='top'>
        <div>
          <h1>FARL Orion View</h1>
          <div class='muted'>Live institution surface for meetings, divisions, snapshots, actions, replay, and state.</div>
          <div class='statusline' id='statusline'>connecting...</div>
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
        async function getJson(url) {
          const r = await fetch(url, { cache: 'no-store' });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return await r.json();
        }

        async function refresh() {
          const now = new Date().toLocaleTimeString();
          try {
            const [state, stream, wake] = await Promise.all([
              getJson('/view/state?ts=' + Date.now()),
              getJson('/view/stream?ts=' + Date.now()),
              getJson('/view/wake?ts=' + Date.now()),
            ]);
            document.getElementById('state').textContent = JSON.stringify(state, null, 2);
            document.getElementById('wake').textContent = JSON.stringify(wake, null, 2);
            document.getElementById('meetings').textContent = JSON.stringify(stream.meetings, null, 2);
            document.getElementById('divisions').textContent = JSON.stringify(state.divisions, null, 2);
            document.getElementById('questions').textContent = JSON.stringify(stream.questions, null, 2);
            document.getElementById('snapshots').textContent = JSON.stringify(stream.snapshots, null, 2);
            document.getElementById('sims').textContent = JSON.stringify(stream.deployment_sims, null, 2);
            document.getElementById('artifacts').textContent = JSON.stringify(state.latest_artifacts, null, 2);
            document.getElementById('statusline').textContent = `live • last refresh ${now} • last run ${state.last_run || 'n/a'} • meetings ${state.meeting_stream_size ?? 'n/a'}`;
          } catch (err) {
            document.getElementById('statusline').textContent = `refresh error • ${err}`;
          }
        }

        async function control(command) {
          await fetch('/view/control', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({command: command})
          });
          await refresh();
        }

        async function toggleAutonomy(enabled) {
          await fetch('/view/control', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
              command:'SET_CONSTRAINTS',
              authorized_by:'Jack',
              enabled: enabled,
              mode: enabled ? 'autonomous' : 'manual'
            })
          });
          await refresh();
        }

        async function snapshot() {
          await fetch('/view/control', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
              command:'LEDGER_WRITE',
              entry_type:'COUNCIL_SYNTHESIS',
              message:'Manual snapshot request from /view',
              source:'FARL Orion View',
              kind:'manual_snapshot'
            })
          });
          await refresh();
        }

        refresh();
        setInterval(refresh, 5000);
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    })


@app.get("/view/state")
async def view_state():
    state = engine.get_state()
    state["github_enabled"] = github_ready()
    state["repo_name"] = REPO_NAME
    return JSONResponse(
        state,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"},
    )


@app.get("/view/stream")
async def view_stream():
    return JSONResponse(
        {
            "meetings": engine.meeting_stream[-30:],
            "questions": engine.self_questions[-40:],
            "snapshots": engine.snapshots[-20:],
            "deployment_sims": engine.deployment_sims[-20:],
        },
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"},
    )


@app.get("/view/wake")
async def view_wake():
    return JSONResponse(
        engine.build_wake_packet(),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"},
    )


@app.post("/view/control")
async def view_control(body: BusRequest):
    return await agent_propose(body)


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = utc_now()

    def envelope(ok: bool, data: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
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
            state = engine.get_state()
            state["github_enabled"] = github_ready()
            state["repo_name"] = REPO_NAME
            return envelope(True, state)

        if command == "LEDGER_WRITE":
            if body.kind == "manual_snapshot":
                snap = engine.snapshot("manual_snapshot")
                result = await engine.write_ledger(
                    body.entry_type or "COUNCIL_SYNTHESIS",
                    {
                        "message": body.message or "",
                        "source": body.source,
                        "kind": body.kind,
                        "snapshot": snap,
                    },
                )
            else:
                result = await engine.write_ledger(
                    body.entry_type or "COUNCIL_SYNTHESIS",
                    {
                        "message": body.message or "",
                        "source": body.source,
                        "kind": body.kind,
                    },
                )
            return envelope(
                result["ok"],
                result["data"],
                None if result["ok"] else f"Ledger write failed: {result['status_code']}",
            )

        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL:
                return envelope(False, error="LEDGER_LATEST_URL not configured")
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
            await engine.write_ledger(
                "COUNCIL_SYNTHESIS",
                {
                    "kind": "constraint_change",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "constraints_active": governance.constraints["active"],
                    "background_debate_enabled": engine.background_debate_enabled,
                    "autonomy_mode": engine.autonomy_mode,
                    "snapshot": snap,
                },
            )
            return envelope(
                True,
                {
                    "constraints_active": governance.constraints["active"],
                    "background_debate_enabled": engine.background_debate_enabled,
                    "autonomy_mode": engine.autonomy_mode,
                },
            )

        if command == "RUN_COUNCIL_CYCLE":
            result = await engine.run_tactic_cycle()
            return envelope(True, {"status": "cycle_triggered", "result": result, "meeting_stream_size": len(engine.meeting_stream)})

        if command == "RUN_RESEARCH_CYCLE":
            result = await engine.run_strategy_cycle()
            return envelope(True, {"status": "research_cycle_triggered", "result": result})

        if command == "GET_WAKE_PACKET":
            return envelope(True, engine.build_wake_packet())

        if command == "DIRECT_MAIN_PUSH":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_direct_main_push")
            if not github_ready():
                return envelope(False, error="github_not_configured")

            file_path = body.file or "app.py"
            content = body.code or ""
            message = body.message or "Direct main push from Orion"

            snap = engine.snapshot(f"before_direct_push:{file_path}")
            result = await github_put_file(file_path, content, message, "main")
            commit_sha = result.get("commit", {}).get("sha")
            html_url = result.get("content", {}).get("html_url") or result.get("commit", {}).get("html_url")

            await engine.write_ledger(
                "OUTCOME",
                {
                    "kind": "direct_main_push",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "file": file_path,
                    "snapshot": snap,
                    "commit": commit_sha,
                    "url": html_url,
                },
            )

            return envelope(True, {"status": "direct_main_pushed", "commit": commit_sha, "url": html_url})

        if command == "CREATE_PULL_REQUEST":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_pr")
            if not github_ready():
                return envelope(False, error="github_not_configured")

            md = body.metadata or {}
            result = await github_create_pull_request(
                title=md.get("title", body.message or "Orion PR"),
                head=md.get("head", ""),
                base=md.get("base", "main"),
                body=md.get("body", ""),
            )

            await engine.write_ledger(
                "OUTCOME",
                {
                    "kind": "create_pull_request",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "result": {"number": result.get("number"), "url": result.get("html_url")},
                },
            )

            return envelope(True, {"status": "pr_created", "number": result.get("number"), "url": result.get("html_url")})

        if command == "MERGE_PULL_REQUEST":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_merge")
            if not github_ready():
                return envelope(False, error="github_not_configured")

            md = body.metadata or {}
            number = int(md.get("number", 0))
            if number <= 0:
                return envelope(False, error="invalid_pr_number")

            snap = engine.snapshot(f"before_merge_pr:{number}")
            result = await github_merge_pull_request(
                number=number,
                commit_title=md.get("commit_title", f"Merged by Orion on behalf of {body.authorized_by}"),
                merge_method=md.get("merge_method", "squash"),
            )

            await engine.write_ledger(
                "OUTCOME",
                {
                    "kind": "merge_pull_request",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "snapshot": snap,
                    "result": {"sha": result.get("sha"), "merged": result.get("merged")},
                },
            )

            return envelope(True, {"status": "merged", "sha": result.get("sha"), "merged": result.get("merged")})

        if command == "ROLLBACK_TO_COMMIT":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_rollback")
            if not github_ready():
                return envelope(False, error="github_not_configured")

            md = body.metadata or {}
            commit_sha = str(md.get("commit_sha", "")).strip()
            if not commit_sha:
                return envelope(False, error="commit_sha_required")

            snap = engine.snapshot(f"before_rollback:{commit_sha}")
            result = await github_rollback_to_commit(commit_sha)

            await engine.write_ledger(
                "OUTCOME",
                {
                    "kind": "rollback_to_commit",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "snapshot": snap,
                    "target_sha": commit_sha,
                    "result": {"ref": result.get("ref")},
                },
            )

            return envelope(True, {"status": "rolled_back", "target_sha": commit_sha, "ref": result.get("ref")})

        if command == "SET_TRUSTED_IDENTITIES":
            if body.authorized_by != governance.operator_sovereign:
                return envelope(False, error="Only Jack can set trusted identities")
            identities = (body.metadata or {}).get("identities", [])
            if not isinstance(identities, list):
                return envelope(False, error="identities must be a list")
            updated = governance.set_trusted_identities(identities)
            await engine.write_ledger(
                "COUNCIL_SYNTHESIS",
                {
                    "kind": "trusted_identities_update",
                    "source": body.source,
                    "authorized_by": body.authorized_by,
                    "trusted_identities": updated,
                },
            )
            return envelope(True, {"trusted_identities": updated})

        if command == "COUNCIL_CALL_VOTE":
            md = body.metadata or {}
            result = governance.call_vote(
                md.get("motion", body.message or "Untitled motion"),
                md.get("options", ["APPROVE", "REJECT"]),
                len(engine.council_agents),
            )
            engine._append_meeting("vote", result)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_vote", "source": body.source, "result": result})
            return envelope(True, result)

        if command == "COUNCIL_ELECT_LEADER":
            result = governance.elect_leader()
            engine._append_meeting("leader_election", result)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "leader_election", "source": body.source, "result": result})
            return envelope(True, result)

        return envelope(False, error=f"Unknown command: {command}")

    except requests.HTTPError as e:
        detail = None
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text if e.response is not None else str(e)
        return envelope(False, data={"detail": detail}, error="http_error")
    except Exception as e:
        return envelope(False, error=str(e))
