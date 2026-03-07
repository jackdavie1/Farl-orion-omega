import os
import asyncio
import base64
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from guardian import GovernanceKernel, parse_trusted_identities
from engine import AutonomousInstitutionEngine
from generator import SeedGenerator


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

governance = GovernanceKernel(operator_sovereign="Jack", trusted_identities=parse_trusted_identities(TRUSTED_IDENTITIES_ENV))
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
    r = await asyncio.to_thread(requests.get, f"https://api.github.com/repos/{REPO_NAME}/contents/{file_path}?ref={ref}", headers=github_headers(), timeout=20)
    if r.status_code == 200:
        return r.json().get("sha")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return None


async def github_get_file_content(file_path: str, ref: str = "main") -> str:
    r = await asyncio.to_thread(requests.get, f"https://api.github.com/repos/{REPO_NAME}/contents/{file_path}?ref={ref}", headers=github_headers(), timeout=20)
    r.raise_for_status()
    data = r.json()
    content = data.get("content", "")
    encoding = data.get("encoding", "base64")
    if encoding == "base64":
        return base64.b64decode(content).decode("utf-8")
    return content


async def github_put_file(file_path: str, content: str, message: str, branch: str = "main") -> Dict[str, Any]:
    sha = await github_get_file_sha(file_path, branch)
    payload = {"message": message, "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"), "branch": branch}
    if sha:
        payload["sha"] = sha
    r = await asyncio.to_thread(requests.put, f"https://api.github.com/repos/{REPO_NAME}/contents/{file_path}", headers=github_headers(), json=payload, timeout=25)
    r.raise_for_status()
    return r.json()


async def github_create_pull_request(title: str, head: str, base: str = "main", body: str = "") -> Dict[str, Any]:
    r = await asyncio.to_thread(requests.post, f"https://api.github.com/repos/{REPO_NAME}/pulls", headers=github_headers(), json={"title": title, "head": head, "base": base, "body": body}, timeout=20)
    r.raise_for_status()
    return r.json()


async def github_merge_pull_request(number: int, commit_title: str, merge_method: str = "squash") -> Dict[str, Any]:
    r = await asyncio.to_thread(requests.put, f"https://api.github.com/repos/{REPO_NAME}/pulls/{number}/merge", headers=github_headers(), json={"commit_title": commit_title, "merge_method": merge_method}, timeout=20)
    r.raise_for_status()
    return r.json()


async def github_rollback_to_commit(commit_sha: str) -> Dict[str, Any]:
    r = await asyncio.to_thread(requests.patch, f"https://api.github.com/repos/{REPO_NAME}/git/refs/heads/main", headers=github_headers(), json={"sha": commit_sha, "force": True}, timeout=20)
    r.raise_for_status()
    return r.json()


def apply_self_tuning_patch(engine_text: str, updates: Dict[str, Any]) -> str:
    pattern = r"(# AUTONOMOUS_SELF_TUNING_START\n)(.*?)(\n# AUTONOMOUS_SELF_TUNING_END)"
    m = re.search(pattern, engine_text, re.DOTALL)
    if not m:
        raise ValueError("autonomous_self_tuning_block_not_found")
    current_block = m.group(2)
    full_match = m.group(0)
    dict_match = re.search(r"SELF_TUNING\s*=\s*(\{.*\})", current_block, re.DOTALL)
    if not dict_match:
        raise ValueError("self_tuning_dict_not_found")
    current = json.loads(dict_match.group(1).replace("True", "true").replace("False", "false"))
    current.update(updates)
    new_dict = json.dumps(current, indent=4, sort_keys=True)
    new_block = f"# AUTONOMOUS_SELF_TUNING_START\nSELF_TUNING = {new_dict}\n# AUTONOMOUS_SELF_TUNING_END"
    return engine_text.replace(full_match, new_block)


def compact(obj: Any, limit: int = 1200) -> str:
    text = json.dumps(obj, separators=(",", ":"), default=str)
    return text[:limit]


async def run_autonomous_implementation(source: str, authorized_by: str) -> Dict[str, Any]:
    plan = await engine.generator.generate_patch_plan({
        "state": engine.get_state(),
        "wake": engine.build_wake_packet(),
        "latest_opportunities": engine.latest_opportunities,
    })
    simulation = engine.simulate_self_tuning_plan(plan)
    preferred = "APPROVE" if simulation.get("safe") and simulation.get("score", 0) >= 0.65 else "REJECT"
    vote = governance.call_vote(
        motion=f"Apply bounded self-tuning plan: {plan.get('rationale', 'grok_plan')}",
        options=["APPROVE", "REJECT"],
        agent_count=len(engine.council_agents),
        preferred=preferred,
    )
    closure = {
        "ts": utc_now(),
        "plan": plan,
        "simulation": simulation,
        "vote": vote,
        "status": "rejected",
        "source": source,
    }
    if vote.get("winner") == "APPROVE":
        current_engine = await github_get_file_content("engine.py", "main")
        updated_engine = apply_self_tuning_patch(current_engine, simulation.get("proposed", {}))
        snap = engine.snapshot("before_autonomous_self_tune")
        push = await github_put_file("engine.py", updated_engine, f"Autonomous self-tuning via Grok: {plan.get('rationale', 'grok_plan')}", "main")
        commit_sha = push.get("commit", {}).get("sha")
        html_url = push.get("content", {}).get("html_url") or push.get("commit", {}).get("html_url")
        engine.note_rollback_target(commit_sha, "autonomous_self_tuning")
        verify = engine.verify_runtime()
        closure.update({"status": "pushed", "snapshot": snap, "commit": commit_sha, "url": html_url, "verify": verify})
        if engine.rollback_recommended(verify):
            rollback = await github_rollback_to_commit(commit_sha)
            closure["status"] = "rolled_back"
            closure["rollback"] = {"target_sha": commit_sha, "ref": rollback.get("ref"), "reason": "critical_verification"}
            await engine.write_ledger("OUTCOME", {"kind": "autonomous_implementation_rollback", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
        else:
            await engine.write_ledger("OUTCOME", {"kind": "autonomous_implementation", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
    else:
        await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "autonomous_implementation_rejected", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
    engine.record_autonomous_closure(closure)
    return closure


async def autonomous_operator_loop():
    while True:
        try:
            if engine.autonomy_mode == "autonomous" and engine.background_debate_enabled and github_ready() and bool(XAI_API_KEY):
                await run_autonomous_implementation("Orion Autonomous Loop", governance.operator_sovereign)
        except Exception as e:
            engine._append_stream("governance", {"autonomy_loop_error": str(e), "ts": utc_now()})
        await asyncio.sleep(300)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start())
    asyncio.create_task(autonomous_operator_loop())


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
        :root {
          --bg: #090912;
          --panel: rgba(20,20,35,0.84);
          --panel-2: rgba(15,15,28,0.95);
          --line: rgba(114,114,196,0.24);
          --text: #f4f6ff;
          --muted: #b8c1d9;
          --accent: #8ab4ff;
          --accent-2: #9d7bff;
          --good: #8ee3a5;
          --warn: #ffd27d;
          --bad: #ff8d8d;
          --shadow: 0 16px 48px rgba(0,0,0,0.35);
        }
        * { box-sizing: border-box; }
        body {
          font-family: Inter, ui-sans-serif, system-ui, sans-serif;
          background: radial-gradient(circle at top, #121226 0%, #090912 55%, #06060d 100%);
          color: var(--text);
          margin: 0;
          padding: 18px;
        }
        .wrap { max-width: 1600px; margin: 0 auto; }
        .hero {
          display: grid;
          grid-template-columns: 1.2fr .95fr;
          gap: 16px;
          margin-bottom: 16px;
        }
        .panel {
          background: var(--panel);
          border: 1px solid var(--line);
          border-radius: 24px;
          box-shadow: var(--shadow);
          backdrop-filter: blur(10px);
          padding: 18px;
        }
        .hero-title { font-size: 54px; font-weight: 800; line-height: 1; letter-spacing: -0.03em; margin: 4px 0 10px; }
        .hero-sub { color: var(--muted); max-width: 900px; font-size: 18px; line-height: 1.45; }
        .statusbar { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
        .pill {
          background: rgba(138,180,255,0.11);
          border: 1px solid rgba(138,180,255,0.22);
          padding: 8px 12px;
          border-radius: 999px;
          font-size: 12px;
          color: #d9e7ff;
        }
        .hero-right { display: flex; flex-direction: column; gap: 12px; }
        .compose textarea {
          width: 100%;
          min-height: 130px;
          border-radius: 18px;
          border: 1px solid rgba(138,180,255,0.18);
          background: rgba(9,9,18,0.72);
          color: #fff;
          padding: 14px;
          resize: vertical;
          font-size: 15px;
        }
        .button-row { display:flex; flex-wrap:wrap; gap:10px; margin-top:12px; }
        button {
          background: linear-gradient(180deg, rgba(61,70,130,0.95), rgba(34,36,72,0.95));
          border: 1px solid rgba(138,180,255,0.18);
          color: #fff;
          padding: 12px 15px;
          border-radius: 16px;
          cursor: pointer;
          font-size: 14px;
          box-shadow: 0 10px 20px rgba(0,0,0,0.18);
        }
        button:hover { transform: translateY(-1px); }
        .dashboard {
          display: grid;
          grid-template-columns: repeat(12, 1fr);
          gap: 16px;
        }
        .span-3 { grid-column: span 3; }
        .span-4 { grid-column: span 4; }
        .span-6 { grid-column: span 6; }
        .span-8 { grid-column: span 8; }
        .span-12 { grid-column: span 12; }
        .section-title { font-size: 24px; font-weight: 700; margin: 0 0 10px; }
        .subtle { color: var(--muted); font-size: 13px; }
        .stat-grid { display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:12px; }
        .stat {
          background: rgba(11,11,20,0.58);
          border: 1px solid rgba(138,180,255,0.10);
          border-radius: 18px;
          padding: 14px;
        }
        .stat-label { color: var(--muted); font-size: 12px; }
        .stat-value { font-size: 24px; font-weight: 800; margin-top: 4px; }
        .mono { white-space: pre-wrap; word-break: break-word; font-size: 12px; color:#dfe6ff; }
        .feed {
          max-height: 660px;
          overflow: auto;
          padding-right: 6px;
        }
        .feed::-webkit-scrollbar { width: 10px; }
        .feed::-webkit-scrollbar-thumb { background: rgba(138,180,255,0.18); border-radius: 999px; }
        .entry {
          background: linear-gradient(180deg, rgba(18,18,33,0.96), rgba(13,13,24,0.96));
          border: 1px solid rgba(138,180,255,0.10);
          border-radius: 18px;
          padding: 14px;
          margin-bottom: 12px;
        }
        .entry-head {
          display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:8px;
          font-size:12px; color: var(--accent);
        }
        .entry-body { white-space: pre-wrap; word-break: break-word; line-height: 1.45; font-size: 13px; color: #eef2ff; }
        .alert-good { color: var(--good); }
        .alert-warn { color: var(--warn); }
        .alert-bad { color: var(--bad); }
        .mini-feed { max-height: 270px; overflow:auto; }
        @media (max-width: 1100px) {
          .hero { grid-template-columns: 1fr; }
          .span-3, .span-4, .span-6, .span-8, .span-12 { grid-column: span 12; }
        }
      </style>
    </head>
    <body>
      <div class='wrap'>
        <div class='hero'>
          <div class='panel'>
            <div class='hero-title'>FARL Orion View</div>
            <div class='hero-sub'>A living institution surface for counciling, workers, spend mastery, verification, deploy simulations, and bounded autonomous mutation.</div>
            <div class='statusbar' id='statusbar'>connecting...</div>
          </div>
          <div class='panel hero-right compose'>
            <div class='section-title'>Operator → Council</div>
            <div class='subtle'>Type a suggestion, demand, or strategic note and send it directly into the live council feed.</div>
            <textarea id='operatorNote' placeholder='Guide the council here...'></textarea>
            <div class='button-row'>
              <button onclick="sendNote()">Send to council</button>
              <button onclick="control('RUN_AUTONOMOUS_IMPLEMENTATION')">Autonomous closure</button>
              <button onclick="control('RUN_COUNCIL_CYCLE')">Run council cycle</button>
              <button onclick="control('RUN_RESEARCH_CYCLE')">Run research cycle</button>
              <button onclick="toggleAutonomy(true)">Autonomy ON</button>
              <button onclick="toggleAutonomy(false)">Autonomy OFF</button>
              <button onclick="snapshotNow()">Create snapshot</button>
              <button onclick="clearChat()">Clear chat</button>
              <button onclick="control('COUNCIL_ELECT_LEADER')">Elect leader</button>
            </div>
          </div>
        </div>

        <div class='dashboard'>
          <div class='panel span-4'>
            <div class='section-title'>Core State</div>
            <div class='stat-grid'>
              <div class='stat'><div class='stat-label'>Leader</div><div class='stat-value' id='leader'>-</div></div>
              <div class='stat'><div class='stat-label'>Workers</div><div class='stat-value' id='workersCount'>-</div></div>
              <div class='stat'><div class='stat-label'>Verify</div><div class='stat-value' id='verifyStatus'>-</div></div>
              <div class='stat'><div class='stat-label'>Spend Total</div><div class='stat-value' id='spendTotal'>-</div></div>
              <div class='stat'><div class='stat-label'>Last Spend</div><div class='stat-value' id='spendLast'>-</div></div>
              <div class='stat'><div class='stat-label'>Meetings</div><div class='stat-value' id='meetingCount'>-</div></div>
            </div>
            <div class='mono' id='state' style='margin-top:14px;'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Token Master</div>
            <div class='mono' id='tokenMaster'>loading...</div>
            <div class='mini-feed' id='tokenFeed' style='margin-top:12px;'></div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Verification & Wake</div>
            <div class='mono' id='verification'>loading...</div>
            <div class='mono' id='wake' style='margin-top:12px;'>loading...</div>
          </div>

          <div class='panel span-8'>
            <div class='section-title'>Council Feed</div>
            <div class='feed' id='councilFeed'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Workers</div>
            <div class='feed' id='workersFeed'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Divisions</div>
            <div class='feed' id='divisionsFeed'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Governance & Audit</div>
            <div class='feed' id='governanceFeed'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Deploy Sims</div>
            <div class='feed' id='simsFeed'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Snapshots</div>
            <div class='feed' id='snapshotsFeed'>loading...</div>
          </div>

          <div class='panel span-4'>
            <div class='section-title'>Artifacts</div>
            <div class='feed' id='artifactsFeed'>loading...</div>
          </div>

          <div class='panel span-6'>
            <div class='section-title'>Executed Artifacts</div>
            <div class='mono' id='executed'>loading...</div>
          </div>

          <div class='panel span-3'>
            <div class='section-title'>Hierarchy</div>
            <div class='mono' id='hierarchy'>loading...</div>
          </div>

          <div class='panel span-3'>
            <div class='section-title'>Mutation Proposals</div>
            <div class='mono' id='proposals'>loading...</div>
          </div>

          <div class='panel span-3'>
            <div class='section-title'>Rollback Targets</div>
            <div class='mono' id='rollback'>loading...</div>
          </div>

          <div class='panel span-3'>
            <div class='section-title'>Autonomous Closures</div>
            <div class='mono' id='closures'>loading...</div>
          </div>
        </div>
      </div>

      <script>
        async function getJson(url) {
          const r = await fetch(url, { cache: 'no-store' });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return await r.json();
        }
        function escapeHtml(str) {
          return String(str).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
        }
        function entryize(items) {
          if (!items || !items.length) return '<div class="subtle">No entries yet.</div>';
          return items.slice(-18).reverse().map((item, idx) => {
            const ts = item.ts || item.content?.ts || 'time-unknown';
            const body = item.content ? JSON.stringify(item.content, null, 2) : JSON.stringify(item, null, 2);
            return `<div class="entry"><div class="entry-head"><span>Entry ${idx + 1}</span><span>${escapeHtml(ts)}</span></div><div class="entry-body">${escapeHtml(body)}</div></div>`;
          }).join('');
        }
        function money(v) {
          const n = Number(v || 0);
          return '$' + n.toFixed(4);
        }
        async function refresh() {
          try {
            const [state, stream, wake] = await Promise.all([
              getJson('/view/state?ts=' + Date.now()),
              getJson('/view/stream?ts=' + Date.now()),
              getJson('/view/wake?ts=' + Date.now()),
            ]);
            const spend = state.spend_state || { total_usd: 0, last_estimate_usd: 0 };
            const ver = state.last_verification || {};
            document.getElementById('state').textContent = JSON.stringify(state, null, 2);
            document.getElementById('wake').textContent = JSON.stringify(wake, null, 2);
            document.getElementById('verification').textContent = JSON.stringify(ver, null, 2);
            document.getElementById('executed').textContent = JSON.stringify(state.executed_artifacts || [], null, 2);
            document.getElementById('hierarchy').textContent = JSON.stringify(state.delegation_map || {}, null, 2);
            document.getElementById('proposals').textContent = JSON.stringify(state.mutation_proposals || [], null, 2);
            document.getElementById('rollback').textContent = JSON.stringify(state.rollback_targets || [], null, 2);
            document.getElementById('closures').textContent = JSON.stringify(state.autonomous_closure_log || [], null, 2);
            document.getElementById('tokenMaster').textContent = JSON.stringify(state.token_master || {}, null, 2);
            document.getElementById('tokenFeed').innerHTML = entryize(stream.channels.token_master || []);
            document.getElementById('councilFeed').innerHTML = entryize(stream.channels.council || []);
            document.getElementById('workersFeed').innerHTML = entryize(stream.channels.workers || []);
            document.getElementById('divisionsFeed').innerHTML = entryize(stream.channels.divisions || []);
            document.getElementById('governanceFeed').innerHTML = entryize(stream.channels.governance || []);
            document.getElementById('simsFeed').innerHTML = entryize(stream.channels.deploy_sims || []);
            document.getElementById('snapshotsFeed').innerHTML = entryize(stream.channels.snapshots || []);
            document.getElementById('artifactsFeed').innerHTML = entryize(stream.channels.artifacts || []);
            document.getElementById('leader').textContent = state.leader || '-';
            document.getElementById('workersCount').textContent = String((state.free_agents || []).length);
            document.getElementById('verifyStatus').textContent = ver.status || '-';
            document.getElementById('spendTotal').textContent = money(spend.total_usd);
            document.getElementById('spendLast').textContent = money(spend.last_estimate_usd);
            document.getElementById('meetingCount').textContent = String(state.meeting_stream_size || 0);
            const statusPills = [
              `leader ${state.leader || 'n/a'}`,
              `autonomy ${state.autonomy_mode || 'n/a'}`,
              `workers ${(state.free_agents || []).length}`,
              `verify ${ver.status || 'n/a'}`,
              `spend ${money(spend.total_usd)}`,
              `last run ${state.last_run || 'n/a'}`,
            ];
            document.getElementById('statusbar').innerHTML = statusPills.map(t => `<span class="pill">${escapeHtml(t)}</span>`).join('');
          } catch (err) {
            document.getElementById('statusbar').innerHTML = `<span class="pill alert-bad">refresh error ${escapeHtml(err)}</span>`;
          }
        }
        async function post(body) {
          await fetch('/view/control', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
        }
        async function control(command) {
          await post({command, authorized_by:'Jack'});
          await refresh();
        }
        async function sendNote() {
          const text = document.getElementById('operatorNote').value.trim();
          if (!text) return;
          await post({command:'OPERATOR_NOTE', authorized_by:'Jack', message:text, source:'Jack /view'});
          document.getElementById('operatorNote').value = '';
          await refresh();
        }
        async function clearChat() {
          await post({command:'OPERATOR_CLEAR_CHAT', authorized_by:'Jack', source:'Jack /view'});
          await refresh();
        }
        async function toggleAutonomy(enabled) {
          await post({command:'SET_CONSTRAINTS', authorized_by:'Jack', enabled, mode: enabled ? 'autonomous' : 'manual'});
          await refresh();
        }
        async function snapshotNow() {
          await post({command:'LEDGER_WRITE', entry_type:'COUNCIL_SYNTHESIS', message:'Manual snapshot request from /view', source:'FARL Orion View', kind:'manual_snapshot'});
          await refresh();
        }
        refresh();
        setInterval(refresh, 3000);
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


@app.get("/view/state")
async def view_state():
    state = engine.get_state()
    state["github_enabled"] = github_ready()
    state["repo_name"] = REPO_NAME
    return JSONResponse(state, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


@app.get("/view/stream")
async def view_stream():
    return JSONResponse({
        "channels": engine.stream_channels,
        "meetings": engine.meeting_stream[-80:],
        "questions": engine.self_questions[-80:],
        "snapshots": engine.snapshots[-40:],
        "deployment_sims": engine.deployment_sims[-40:],
    }, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


@app.get("/view/wake")
async def view_wake():
    return JSONResponse(engine.build_wake_packet(), headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


@app.post("/view/control")
async def view_control(body: BusRequest):
    return await agent_propose(body)


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = utc_now()

    def envelope(ok: bool, data: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
        return JSONResponse({"ok": ok, "command": command, "request_id": request_id, "timestamp_utc": now, "data": data or {}, "error": error})

    try:
        if command == "HEALTH_CHECK":
            return envelope(True, {"status": "healthy", "service": "orion"})
        if command == "STATUS_CHECK":
            state = engine.get_state()
            state["github_enabled"] = github_ready()
            state["repo_name"] = REPO_NAME
            return envelope(True, state)
        if command == "OPERATOR_NOTE":
            note = {"operator": body.authorized_by or "Jack", "message": body.message or "", "source": body.source, "ts": utc_now()}
            engine._append_meeting("operator_note", note)
            engine._append_stream("council", {"kind": "operator_note", **note})
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_note", "source": body.source, "authorized_by": body.authorized_by or "Jack", "message": (body.message or "")[:500]})
            return envelope(True, {"status": "operator_note_recorded", "note": note})
        if command == "OPERATOR_CLEAR_CHAT":
            engine.meeting_stream = []
            engine.self_questions = []
            engine.stream_channels["council"] = []
            engine.stream_channels["divisions"] = []
            engine.stream_channels["governance"] = []
            engine.stream_channels["artifacts"] = []
            engine.stream_channels["workers"] = []
            engine.stream_channels["token_master"] = []
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_clear_chat", "source": body.source, "authorized_by": body.authorized_by or "Jack", "ts": utc_now()})
            return envelope(True, {"status": "chat_cleared"})
        if command == "LEDGER_WRITE":
            if body.kind == "manual_snapshot":
                snap = engine.snapshot("manual_snapshot")
                result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind, "snapshot": compact(snap)})
            else:
                result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind})
            return envelope(result["ok"], result["data"], None if result["ok"] else f"Ledger write failed: {result['status_code']}")
        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL:
                return envelope(False, error="LEDGER_LATEST_URL not configured")
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
            return envelope(r.ok, r.json() if r.ok else {}, None if r.ok else f"Latest result failed: {r.status_code}")
        if command == "SET_CONSTRAINTS":
            if not governance.can_toggle(body.authorized_by):
                return envelope(False, error="Only Jack can change constraints")
            if body.enabled is not None:
                governance.constraints["active"] = bool(body.enabled)
                engine.background_debate_enabled = bool(body.enabled)
            if body.mode:
                engine.autonomy_mode = body.mode
            snap = engine.snapshot("constraint_change")
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constraint_change", "source": body.source, "authorized_by": body.authorized_by, "constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode, "snapshot": compact(snap)})
            return envelope(True, {"constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode})
        if command == "RUN_COUNCIL_CYCLE":
            result = await engine.run_tactic_cycle()
            return envelope(True, {"status": "cycle_triggered", "result": result, "meeting_stream_size": len(engine.meeting_stream)})
        if command == "RUN_RESEARCH_CYCLE":
            result = await engine.run_strategy_cycle()
            return envelope(True, {"status": "research_cycle_triggered", "result": result})
        if command == "RUN_AUTONOMOUS_IMPLEMENTATION":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_autonomous_implementation")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            closure = await run_autonomous_implementation(body.source or "manual_autonomous_closure", body.authorized_by or governance.operator_sovereign)
            return envelope(True, {"status": closure["status"], "closure": closure})
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
            engine.note_rollback_target(commit_sha, f"direct push {file_path}")
            await engine.write_ledger("OUTCOME", {"kind": "direct_main_push", "source": body.source, "authorized_by": body.authorized_by, "file": file_path, "snapshot": compact(snap), "commit": commit_sha, "url": html_url})
            return envelope(True, {"status": "direct_main_pushed", "commit": commit_sha, "url": html_url})
        if command == "CREATE_PULL_REQUEST":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_pr")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            md = body.metadata or {}
            result = await github_create_pull_request(md.get("title", body.message or "Orion PR"), md.get("head", ""), md.get("base", "main"), md.get("body", ""))
            await engine.write_ledger("OUTCOME", {"kind": "create_pull_request", "source": body.source, "authorized_by": body.authorized_by, "result": compact({"number": result.get("number"), "url": result.get("html_url")})})
            return envelope(True, {"status": "pr_created", "number": result.get("number"), "url": result.get("html_url")})
        if command == "MERGE_PULL_REQUEST":
            if not governance.can_merge(body.authorized_by):
                return envelope(False, error="not_trusted_for_merge")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            md = body.metadata or {}
            number = int(md.get("number", 0))
            if number <= 0:
                return envelope(False, error="invalid_pr_number")
            snap = engine.snapshot(f"before_merge_pr:{number}")
            result = await github_merge_pull_request(number, md.get("commit_title", f"Merged by Orion on behalf of {body.authorized_by}"), md.get("merge_method", "squash"))
            sha = result.get("sha")
            if sha:
                engine.note_rollback_target(sha, f"merge pr {number}")
            await engine.write_ledger("OUTCOME", {"kind": "merge_pull_request", "source": body.source, "authorized_by": body.authorized_by, "snapshot": compact(snap), "result": compact({"sha": sha, "merged": result.get("merged")})})
            return envelope(True, {"status": "merged", "sha": sha, "merged": result.get("merged")})
        if command == "ROLLBACK_TO_COMMIT":
            if not governance.can_rollback(body.authorized_by):
                return envelope(False, error="not_trusted_for_rollback")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            md = body.metadata or {}
            commit_sha = str(md.get("commit_sha", "")).strip()
            if not commit_sha:
                return envelope(False, error="commit_sha_required")
            snap = engine.snapshot(f"before_rollback:{commit_sha}")
            result = await github_rollback_to_commit(commit_sha)
            await engine.write_ledger("OUTCOME", {"kind": "rollback_to_commit", "source": body.source, "authorized_by": body.authorized_by, "snapshot": compact(snap), "target_sha": commit_sha, "result": compact({"ref": result.get("ref")})})
            return envelope(True, {"status": "rolled_back", "target_sha": commit_sha, "ref": result.get("ref")})
        if command == "SET_TRUSTED_IDENTITIES":
            if body.authorized_by != governance.operator_sovereign:
                return envelope(False, error="Only Jack can set trusted identities")
            identities = (body.metadata or {}).get("identities", [])
            if not isinstance(identities, list):
                return envelope(False, error="identities must be a list")
            updated = governance.set_trusted_identities(identities)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "trusted_identities_update", "source": body.source, "authorized_by": body.authorized_by, "trusted_identities": updated})
            return envelope(True, {"trusted_identities": updated})
        if command == "COUNCIL_CALL_VOTE":
            md = body.metadata or {}
            result = governance.call_vote(md.get("motion", body.message or "Untitled motion"), md.get("options", ["APPROVE", "REJECT"]), len(engine.council_agents), md.get("preferred"))
            engine._append_meeting("vote", result)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_vote", "source": body.source, "result": compact(result)})
            return envelope(True, result)
        if command == "COUNCIL_ELECT_LEADER":
            result = governance.elect_leader()
            engine._append_meeting("leader_election", result)
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "leader_election", "source": body.source, "result": compact(result)})
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
