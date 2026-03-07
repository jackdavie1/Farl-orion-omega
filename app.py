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
  <title>FARL Orion Control Room</title>
  <style>
    :root {
      --bg:#070812;
      --bg2:#0d1020;
      --panel:#11152aee;
      --panel2:#0c1020ee;
      --line:#2b3563;
      --soft:#9fb2ff;
      --text:#f6f7ff;
      --muted:#9ea6c8;
      --good:#77e7a7;
      --warn:#ffd36e;
      --bad:#ff8d98;
      --shadow:0 20px 60px rgba(0,0,0,.35);
      --radius:22px;
    }
    *{box-sizing:border-box}
    html,body{margin:0;padding:0;background:radial-gradient(circle at top left,#1a2248 0,#0b0f1f 32%,#070812 100%);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,sans-serif;min-height:100%}
    .shell{max-width:1600px;margin:0 auto;padding:18px}
    .topbar{display:grid;grid-template-columns:1.2fr .8fr;gap:16px;margin-bottom:16px}
    .panel{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);backdrop-filter:blur(12px)}
    .hero{padding:22px}
    .title{font-size:52px;font-weight:900;letter-spacing:-.04em;line-height:1;margin:0 0 10px}
    .subtitle{color:var(--muted);font-size:17px;max-width:900px;line-height:1.45}
    .chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}
    .chip{border:1px solid #33427d;border-radius:999px;padding:8px 12px;background:#121a36;color:#dfe7ff;font-size:12px}
    .control{padding:18px}
    .control h2{margin:0 0 10px;font-size:20px}
    textarea{width:100%;min-height:120px;border-radius:18px;border:1px solid #33427d;background:#0a0d1be6;color:#fff;padding:14px;font-size:15px;resize:vertical;outline:none}
    .buttons{display:flex;flex-wrap:wrap;gap:10px;margin-top:12px}
    button,select{appearance:none;border:1px solid #3b4b88;background:linear-gradient(180deg,#2d3a70,#1b2348);color:#fff;padding:11px 14px;border-radius:14px;font-size:14px}
    button:hover{filter:brightness(1.08)}
    select{padding-right:36px}
    .stats{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px;margin-bottom:16px}
    .stat{padding:16px;border-radius:20px;border:1px solid #26315e;background:#0d1225c7}
    .stat .k{font-size:12px;color:var(--muted)}
    .stat .v{font-size:30px;font-weight:900;margin-top:4px;letter-spacing:-.03em}
    .main{display:grid;grid-template-columns:1.1fr .9fr;gap:16px}
    .feedPanel{padding:16px;display:flex;flex-direction:column;min-height:720px}
    .feedHead{display:flex;gap:10px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin-bottom:12px}
    .feedTitle{font-size:24px;font-weight:800}
    .feedTools{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .feed{flex:1;overflow:auto;padding-right:4px;scroll-behavior:smooth;border-radius:18px;background:#0a0d1ae6;border:1px solid #1f2852;padding:12px}
    .feed::-webkit-scrollbar{width:10px}.feed::-webkit-scrollbar-thumb{background:#3a4a89;border-radius:999px}
    .entry{padding:14px 14px 12px;border:1px solid #26315e;background:linear-gradient(180deg,#131935,#0d1225);border-radius:18px;margin-bottom:12px;box-shadow:0 10px 24px rgba(0,0,0,.18)}
    .entryTop{display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:8px}
    .entryType{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--soft);font-weight:700}
    .entryTs{font-size:11px;color:var(--muted)}
    .entryBody{font-size:14px;line-height:1.5;color:#eef2ff}
    .entryGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:10px}
    .mini{padding:10px;border-radius:14px;background:#0b0f1fe6;border:1px solid #222b55;font-size:12px;color:#dce4ff}
    .side{display:grid;grid-template-columns:1fr;gap:16px}
    .card{padding:16px}
    .card h3{margin:0 0 8px;font-size:18px}
    .mono{white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.4;color:#deE6ff;max-height:320px;overflow:auto}
    .miniFeed{max-height:260px;overflow:auto;padding-right:4px}
    .tabbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
    .tab{padding:8px 12px;border-radius:999px;border:1px solid #31417c;background:#12193a;color:#dfe7ff;font-size:12px;cursor:pointer}
    .tab.active{background:#24336c;color:#fff}
    .hidden{display:none}
    .twoCol{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .good{color:var(--good)} .warn{color:var(--warn)} .bad{color:var(--bad)}
    .footerNote{margin-top:8px;color:var(--muted);font-size:12px}
    @media (max-width:1200px){.main,.topbar{grid-template-columns:1fr}.stats{grid-template-columns:repeat(3,minmax(0,1fr))}}
    @media (max-width:700px){.stats{grid-template-columns:repeat(2,minmax(0,1fr))}.title{font-size:38px}.feedPanel{min-height:560px}}
  </style>
</head>
<body>
  <div class='shell'>
    <div class='topbar'>
      <section class='panel hero'>
        <h1 class='title'>FARL Orion Control Room</h1>
        <div class='subtitle'>A readable live website for council autonomy, workers, verification, spend mastery, and bounded self-mutation. Threads are selectable, feeds float in one box, and proof of internal autonomy is visible at a glance.</div>
        <div class='chips' id='chips'><span class='chip'>connecting…</span></div>
      </section>
      <section class='panel control'>
        <h2>Operator → Council</h2>
        <textarea id='operatorNote' placeholder='Type a suggestion, direction, or challenge for the council…'></textarea>
        <div class='buttons'>
          <button onclick="sendNote()">Send to council</button>
          <button onclick="control('RUN_AUTONOMOUS_IMPLEMENTATION')">Autonomous closure</button>
          <button onclick="control('RUN_COUNCIL_CYCLE')">Council cycle</button>
          <button onclick="control('RUN_RESEARCH_CYCLE')">Research cycle</button>
          <button onclick="toggleAutonomy(true)">Autonomy ON</button>
          <button onclick="toggleAutonomy(false)">Autonomy OFF</button>
          <button onclick="snapshotNow()">Snapshot</button>
          <button onclick="clearChat()">Clear feed</button>
          <button onclick="control('COUNCIL_ELECT_LEADER')">Elect leader</button>
        </div>
      </section>
    </div>

    <section class='stats'>
      <div class='panel stat'><div class='k'>Leader</div><div class='v' id='sLeader'>-</div></div>
      <div class='panel stat'><div class='k'>Workers</div><div class='v' id='sWorkers'>-</div></div>
      <div class='panel stat'><div class='k'>Verify</div><div class='v' id='sVerify'>-</div></div>
      <div class='panel stat'><div class='k'>Spend Total</div><div class='v' id='sSpendTotal'>-</div></div>
      <div class='panel stat'><div class='k'>Last Spend</div><div class='v' id='sSpendLast'>-</div></div>
      <div class='panel stat'><div class='k'>Meetings</div><div class='v' id='sMeetings'>-</div></div>
    </section>

    <div class='main'>
      <section class='panel feedPanel'>
        <div class='feedHead'>
          <div class='feedTitle'>Live Feed</div>
          <div class='feedTools'>
            <select id='threadSelect' onchange='renderAll()'>
              <option value='council'>Council</option>
              <option value='workers'>Workers</option>
              <option value='divisions'>Divisions</option>
              <option value='governance'>Governance</option>
              <option value='deploy_sims'>Deploy sims</option>
              <option value='snapshots'>Snapshots</option>
              <option value='artifacts'>Artifacts</option>
              <option value='token_master'>Token master</option>
            </select>
            <select id='entryCount' onchange='renderAll()'>
              <option value='10'>10 latest</option>
              <option value='20' selected>20 latest</option>
              <option value='40'>40 latest</option>
            </select>
            <button onclick='scrollFeedTop()'>Top</button>
            <button onclick='scrollFeedBottom()'>Bottom</button>
          </div>
        </div>
        <div id='liveFeed' class='feed'>loading…</div>
        <div class='footerNote'>Choose a thread from the dropdown instead of scrolling through the entire page.</div>
      </section>

      <section class='side'>
        <section class='panel card'>
          <h3>Autonomy Proof</h3>
          <div class='tabbar'>
            <div class='tab active' data-tab='summary' onclick='switchTab(this)'>Summary</div>
            <div class='tab' data-tab='verification' onclick='switchTab(this)'>Verification</div>
            <div class='tab' data-tab='workers' onclick='switchTab(this)'>Workers</div>
            <div class='tab' data-tab='spend' onclick='switchTab(this)'>Spend</div>
            <div class='tab' data-tab='mutation' onclick='switchTab(this)'>Mutation</div>
          </div>
          <div id='tab-summary' class='tabpane'>
            <div class='twoCol'>
              <div class='mini'><strong>Status</strong><div id='summaryStatus'>-</div></div>
              <div class='mini'><strong>Last run</strong><div id='summaryRun'>-</div></div>
              <div class='mini'><strong>Autonomy</strong><div id='summaryAutonomy'>-</div></div>
              <div class='mini'><strong>Grok live</strong><div id='summaryGrok'>-</div></div>
            </div>
          </div>
          <div id='tab-verification' class='tabpane hidden'><div id='verificationPane' class='mono'>loading…</div></div>
          <div id='tab-workers' class='tabpane hidden'><div id='workersPane' class='miniFeed'>loading…</div></div>
          <div id='tab-spend' class='tabpane hidden'><div id='spendPane' class='mono'>loading…</div><div id='spendAlerts' class='miniFeed' style='margin-top:10px;'></div></div>
          <div id='tab-mutation' class='tabpane hidden'><div id='mutationPane' class='mono'>loading…</div></div>
        </section>

        <section class='panel card'>
          <h3>Token Master</h3>
          <div id='tokenMasterPane' class='mono'>loading…</div>
        </section>

        <section class='panel card'>
          <h3>Current Opportunity Queue</h3>
          <div id='opportunitiesPane' class='miniFeed'>loading…</div>
        </section>

        <section class='panel card'>
          <h3>State Snapshots</h3>
          <div id='miniSnapshots' class='miniFeed'>loading…</div>
        </section>
      </section>
    </div>
  </div>

  <script>
    let latestState = null;
    let latestStream = null;
    let latestWake = null;

    function escapeHtml(str){return String(str).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}
    async function getJson(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json()}
    async function post(body){await fetch('/view/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})}
    function money(v){const n=Number(v||0);return '$'+n.toFixed(4)}
    function short(obj){return JSON.stringify(obj,null,2)}
    function extractLabel(content){
      if(!content) return 'event';
      if(content.kind) return content.kind;
      if(content.division) return content.division;
      if(content.report) return 'token-report';
      if(content.alert) return 'token-alert';
      if(content.rollback_target) return 'rollback-target';
      if(content.verification) return 'verification';
      return 'entry';
    }
    function renderFeedItems(items){
      if(!items || !items.length) return '<div class="entry"><div class="entryBody">No entries yet.</div></div>';
      const count = Number(document.getElementById('entryCount').value || 20);
      return items.slice(-count).reverse().map((row)=>{
        const content = row.content || row;
        const label = extractLabel(content);
        const ts = row.ts || content.ts || 'time-unknown';
        const body = summarizeContent(content);
        return `<article class="entry"><div class="entryTop"><div class="entryType">${escapeHtml(label)}</div><div class="entryTs">${escapeHtml(ts)}</div></div><div class="entryBody">${body}</div></article>`;
      }).join('');
    }
    function summarizeContent(c){
      if(!c) return '';
      if(c.message) return escapeHtml(c.message);
      if(c.alert && c.alert.message) return `<span class='warn'>${escapeHtml(c.alert.message)}</span>`;
      if(c.report) return `<div class='entryGrid'><div class='mini'><strong>Estimate</strong><br>${money(c.report.estimate_usd)}</div><div class='mini'><strong>Total</strong><br>${money(c.report.total_usd)}</div></div>`;
      if(c.verification) return `<div class='entryGrid'><div class='mini'><strong>Status</strong><br>${escapeHtml(c.verification.status)}</div><div class='mini'><strong>Score</strong><br>${escapeHtml(c.verification.score)}</div></div>`;
      if(c.rollback_target) return `<div class='entryGrid'><div class='mini'><strong>Commit</strong><br>${escapeHtml(c.rollback_target.commit_sha)}</div><div class='mini'><strong>Reason</strong><br>${escapeHtml(c.rollback_target.reason)}</div></div>`;
      if(c.latest && c.division) return `<strong>${escapeHtml(c.division)}</strong><br>${escapeHtml((c.latest.finding||short(c.latest)).slice(0,260))}`;
      if(c.question && c.division) return `<strong>${escapeHtml(c.division)}</strong><br>${escapeHtml(c.question)}`;
      if(c.name && c.mission) return `<strong>${escapeHtml(c.name)}</strong><br>${escapeHtml(c.mission)}`;
      if(c.trigger && c.vote) return `<div class='entryGrid'><div class='mini'><strong>Trigger</strong><br>${escapeHtml(c.trigger)}</div><div class='mini'><strong>Confidence</strong><br>${escapeHtml(c.vote.confidence)}</div></div>`;
      return `<div class='mono'>${escapeHtml(short(c).slice(0,2000))}</div>`;
    }
    function switchTab(el){
      document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
      el.classList.add('active');
      const tab = el.dataset.tab;
      document.querySelectorAll('.tabpane').forEach(p=>p.classList.add('hidden'));
      document.getElementById('tab-'+tab).classList.remove('hidden');
    }
    function scrollFeedTop(){const f=document.getElementById('liveFeed');f.scrollTop=0}
    function scrollFeedBottom(){const f=document.getElementById('liveFeed');f.scrollTop=f.scrollHeight}
    async function control(command){await post({command,authorized_by:'Jack'});await refresh()}
    async function sendNote(){const text=document.getElementById('operatorNote').value.trim(); if(!text) return; await post({command:'OPERATOR_NOTE',authorized_by:'Jack',message:text,source:'Jack /view'}); document.getElementById('operatorNote').value=''; await refresh()}
    async function clearChat(){await post({command:'OPERATOR_CLEAR_CHAT',authorized_by:'Jack',source:'Jack /view'}); await refresh()}
    async function toggleAutonomy(enabled){await post({command:'SET_CONSTRAINTS',authorized_by:'Jack',enabled,mode:enabled?'autonomous':'manual'}); await refresh()}
    async function snapshotNow(){await post({command:'LEDGER_WRITE',entry_type:'COUNCIL_SYNTHESIS',message:'Manual snapshot request from /view',source:'FARL Orion View',kind:'manual_snapshot'}); await refresh()}

    function renderAll(){
      if(!latestState || !latestStream || !latestWake) return;
      const spend = latestState.spend_state || {total_usd:0,last_estimate_usd:0,alerts:[]};
      const ver = latestState.last_verification || {};
      document.getElementById('sLeader').textContent = latestState.leader || '-';
      document.getElementById('sWorkers').textContent = String((latestState.free_agents||[]).length);
      document.getElementById('sVerify').textContent = ver.status || '-';
      document.getElementById('sVerify').className = 'v ' + (ver.status==='healthy'?'good':ver.status==='critical'?'bad':'warn');
      document.getElementById('sSpendTotal').textContent = money(spend.total_usd);
      document.getElementById('sSpendLast').textContent = money(spend.last_estimate_usd);
      document.getElementById('sMeetings').textContent = String(latestState.meeting_stream_size||0);
      const chips = [
        `leader ${latestState.leader||'n/a'}`,
        `autonomy ${latestState.autonomy_mode||'n/a'}`,
        `verify ${ver.status||'n/a'}`,
        `workers ${(latestState.free_agents||[]).length}`,
        `spend ${money(spend.total_usd)}`,
        `last run ${latestState.last_run||'n/a'}`,
      ];
      document.getElementById('chips').innerHTML = chips.map(t=>`<span class='chip'>${escapeHtml(t)}</span>`).join('');
      document.getElementById('summaryStatus').textContent = latestState.status || '-';
      document.getElementById('summaryRun').textContent = latestState.last_run || '-';
      document.getElementById('summaryAutonomy').textContent = latestState.autonomy_mode || '-';
      document.getElementById('summaryGrok').textContent = String((latestState.world_model||{}).resources?.grok_live ?? false);
      document.getElementById('verificationPane').textContent = short(ver);
      document.getElementById('workersPane').innerHTML = renderFeedItems((latestStream.channels||{}).workers || []);
      document.getElementById('spendPane').textContent = short(spend);
      document.getElementById('spendAlerts').innerHTML = renderFeedItems(((latestStream.channels||{}).token_master || []).filter(x=>x.content && (x.content.alert || x.content.report)));
      document.getElementById('mutationPane').textContent = short({proposals:latestState.mutation_proposals||[],rollback_targets:latestState.rollback_targets||[],closures:latestState.autonomous_closure_log||[]});
      document.getElementById('tokenMasterPane').textContent = short(latestState.token_master || {});
      document.getElementById('opportunitiesPane').innerHTML = renderFeedItems((latestState.latest_opportunities || []).map(o=>({ts:o.id,content:o})));
      document.getElementById('miniSnapshots').innerHTML = renderFeedItems((latestStream.channels||{}).snapshots || []);
      const selected = document.getElementById('threadSelect').value;
      document.getElementById('liveFeed').innerHTML = renderFeedItems((latestStream.channels||{})[selected] || []);
    }

    async function refresh(){
      try{
        const [state,stream,wake] = await Promise.all([
          getJson('/view/state?ts='+Date.now()),
          getJson('/view/stream?ts='+Date.now()),
          getJson('/view/wake?ts='+Date.now())
        ]);
        latestState = state; latestStream = stream; latestWake = wake; renderAll();
      }catch(err){
        document.getElementById('chips').innerHTML = `<span class='chip'>refresh error ${escapeHtml(err)}</span>`;
      }
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
