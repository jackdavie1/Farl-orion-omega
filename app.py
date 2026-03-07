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
    closure = {"ts": utc_now(), "plan": plan, "simulation": simulation, "vote": vote, "status": "rejected", "source": source}
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
<title>FARL Orion Live</title>
<style>
:root{--bg:#0a0b10;--bg2:#11131d;--panel:#111521;--panel2:#0d111b;--line:#252a3b;--text:#f5f7ff;--muted:#96a0bd;--accent:#7c8cff;--good:#77e7a7;--warn:#ffd46b;--bad:#ff7f90;--shadow:0 20px 60px rgba(0,0,0,.45)}
*{box-sizing:border-box}html,body{margin:0;height:100%;background:linear-gradient(180deg,#0b0c12,#07080c);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,sans-serif}
.app{display:grid;grid-template-columns:280px 1fr 320px;height:100vh;overflow:hidden}.sidebar,.rightbar,.center{min-height:0}.sidebar,.rightbar{background:rgba(12,14,22,.95);border-right:1px solid var(--line)}.rightbar{border-right:none;border-left:1px solid var(--line)}
.brand{padding:18px 18px 12px;border-bottom:1px solid var(--line)}.brand h1{margin:0;font-size:28px;letter-spacing:-.03em}.brand p{margin:6px 0 0;color:var(--muted);font-size:12px;line-height:1.4}
.sideSection{padding:14px}.sectionLabel{font-size:11px;text-transform:uppercase;letter-spacing:.12em;color:var(--muted);margin-bottom:10px}
.threadList{display:flex;flex-direction:column;gap:8px}.threadItem{padding:12px 14px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(180deg,#141929,#101522);cursor:pointer}.threadItem.active{border-color:#5666e8;background:linear-gradient(180deg,#20295c,#141b38)}.threadName{font-size:14px;font-weight:700}.threadMeta{font-size:12px;color:var(--muted);margin-top:4px}
.statGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.stat{padding:12px;border:1px solid var(--line);border-radius:16px;background:#0f131d}.stat .k{font-size:11px;color:var(--muted)}.stat .v{font-size:22px;font-weight:800;margin-top:4px;letter-spacing:-.03em}
.center{display:grid;grid-template-rows:auto 1fr auto;background:radial-gradient(circle at top,#12172a 0,#0a0b10 48%)}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line);background:rgba(10,11,16,.7);backdrop-filter:blur(10px)}
.topTitle{font-size:24px;font-weight:800}.topSub{color:var(--muted);font-size:12px;margin-top:4px}.controls{display:flex;gap:8px;flex-wrap:wrap}
button,select{border:1px solid #313754;background:linear-gradient(180deg,#262e52,#171d34);color:#fff;border-radius:14px;padding:10px 12px;font-size:13px}button{cursor:pointer}button:hover{filter:brightness(1.08)}select{appearance:none}
.feedWrap{padding:14px 18px;overflow:hidden;display:flex;flex-direction:column}.feed{flex:1;overflow:auto;padding-right:6px}.feed::-webkit-scrollbar{width:10px}.feed::-webkit-scrollbar-thumb{background:#2e365c;border-radius:999px}
.msg{display:flex;gap:10px;margin-bottom:14px;align-items:flex-start}.avatar{width:38px;height:38px;border-radius:14px;display:flex;align-items:center;justify-content:center;font-weight:800;background:linear-gradient(180deg,#32418f,#1d2755);border:1px solid #4656ab;flex:none}.bubble{min-width:0;max-width:min(860px,100%)}.bubbleTop{display:flex;gap:10px;align-items:center;margin-bottom:6px;flex-wrap:wrap}.who{font-size:13px;font-weight:800}.when{font-size:11px;color:var(--muted)}.tag{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--accent);border:1px solid #39427b;border-radius:999px;padding:4px 8px}.card{background:linear-gradient(180deg,#111626,#0d1220);border:1px solid #232b49;border-radius:18px;padding:12px 14px;box-shadow:0 12px 28px rgba(0,0,0,.18)}.text{font-size:14px;line-height:1.58;color:#eef2ff;white-space:pre-wrap;word-break:break-word}.kv{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.mini{padding:10px;border-radius:14px;background:#0a0e1a;border:1px solid #222a46;font-size:12px;color:#dfe6ff}
.composer{border-top:1px solid var(--line);padding:14px 18px;background:rgba(10,11,16,.86);backdrop-filter:blur(10px)}.composerBox{display:grid;grid-template-columns:1fr auto;gap:10px}.composer textarea{width:100%;min-height:72px;max-height:160px;resize:vertical;padding:14px;border-radius:18px;background:#0b0f19;border:1px solid #242b45;color:#fff;font-size:14px}.composerActions{display:flex;flex-direction:column;gap:8px}
.panelCard{margin:14px;padding:14px;border:1px solid var(--line);border-radius:18px;background:linear-gradient(180deg,#121725,#0d111a)}.panelCard h3{margin:0 0 10px;font-size:16px}.mono{white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.42;color:#dde4ff;max-height:220px;overflow:auto}.pillRow{display:flex;flex-wrap:wrap;gap:8px}.pill{font-size:11px;padding:7px 10px;border-radius:999px;border:1px solid #303a63;background:#121938;color:#dfe7ff}.good{color:var(--good)}.warn{color:var(--warn)}.bad{color:var(--bad)}
@media (max-width:1100px){.app{grid-template-columns:84px 1fr}.rightbar{display:none}.brand h1,.brand p,.threadMeta,.sectionLabel{display:none}.threadName{font-size:0}.threadName::before{content:'•';font-size:22px}.threadItem{padding:14px;display:flex;justify-content:center}.sideSection{padding:10px}.statGrid{grid-template-columns:1fr}}
@media (max-width:720px){.app{grid-template-columns:1fr;grid-template-rows:auto 1fr}.sidebar{display:none}.topbar{padding:12px}.topTitle{font-size:20px}.controls{gap:6px}.feedWrap{padding:10px 12px}.composer{padding:10px 12px}.composerBox{grid-template-columns:1fr}.composerActions{flex-direction:row;flex-wrap:wrap}}
</style>
</head>
<body>
<div class='app'>
  <aside class='sidebar'>
    <div class='brand'><h1>FARL Live</h1><p>Chat-first council room.</p></div>
    <div class='sideSection'><div class='sectionLabel'>Threads</div><div class='threadList' id='threadList'></div></div>
    <div class='sideSection'><div class='sectionLabel'>At a glance</div><div class='statGrid'><div class='stat'><div class='k'>Leader</div><div class='v' id='leaderStat'>-</div></div><div class='stat'><div class='k'>Verify</div><div class='v' id='verifyStat'>-</div></div><div class='stat'><div class='k'>Workers</div><div class='v' id='workerStat'>-</div></div><div class='stat'><div class='k'>Spend</div><div class='v' id='spendStat'>-</div></div></div></div>
  </aside>
  <main class='center'>
    <div class='topbar'><div><div class='topTitle' id='feedTitle'>Council</div><div class='topSub' id='feedSub'>Live internal autonomy stream</div></div><div class='controls'><select id='entryCount' onchange='renderFeed()'><option value='12'>12</option><option value='20' selected>20</option><option value='40'>40</option></select><button onclick="control('RUN_COUNCIL_CYCLE')">Council</button><button onclick="control('RUN_RESEARCH_CYCLE')">Research</button><button onclick="control('RUN_AUTONOMOUS_IMPLEMENTATION')">Closure</button><button onclick='scrollToBottomFeed()'>Latest</button></div></div>
    <div class='feedWrap'><div class='feed' id='feed'></div></div>
    <div class='composer'><div class='composerBox'><textarea id='operatorNote' placeholder='Type into the council feed…'></textarea><div class='composerActions'><button onclick='sendNote()'>Send</button><button onclick='toggleAutonomy(true)'>Auto ON</button><button onclick='toggleAutonomy(false)'>Auto OFF</button><button onclick='snapshotNow()'>Snapshot</button><button onclick='clearChat()'>Clear</button></div></div></div>
  </main>
  <aside class='rightbar'>
    <div class='panelCard'><h3>Autonomy proof</h3><div class='pillRow' id='proofPills'></div></div>
    <div class='panelCard'><h3>Token Master</h3><div class='mono' id='tokenPane'>loading…</div></div>
    <div class='panelCard'><h3>Spend</h3><div class='mono' id='spendPane'>loading…</div></div>
    <div class='panelCard'><h3>Verification</h3><div class='mono' id='verifyPane'>loading…</div></div>
    <div class='panelCard'><h3>Mutation queue</h3><div class='mono' id='mutationPane'>loading…</div></div>
  </aside>
</div>
<script>
let state=null, stream=null, wake=null, currentThread='council';
const threadDefs=[['council','Council','Meetings, votes, debate'],['workers','Workers','Process-local agent actions'],['divisions','Divisions','Division findings and questions'],['governance','Governance','Verification, rollback, audit'],['deploy_sims','Deploy Sims','Simulation gating and deploy notes'],['snapshots','Snapshots','Replay and state snapshots'],['artifacts','Artifacts','Executed artifact outputs'],['token_master','Token Master','Spend reports and alerts']];
function esc(s){return String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}
function money(v){return '$'+Number(v||0).toFixed(4)}
async function getJson(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok) throw new Error(`HTTP ${r.status}`); return await r.json()}
async function post(body){await fetch('/view/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})}
function initials(label){return esc((label||'X').slice(0,2).toUpperCase())}
function renderThreadRail(){const el=document.getElementById('threadList'); el.innerHTML=threadDefs.map(([id,name,meta])=>{const count=((stream?.channels||{})[id]||[]).length; return `<div class='threadItem ${currentThread===id?'active':''}' onclick="setThread('${id}')"><div class='threadName'>${esc(name)}</div><div class='threadMeta'>${esc(meta)} · ${count}</div></div>`}).join('')}
function sentence(text){return esc(String(text||'').replace(/[{}\[\]"]+/g,'').replace(/\s+/g,' ').trim())}
function summarize(c){
  if(!c) return {title:'Quiet moment', body:'The room pauses briefly while the next event is prepared.', meta:[]};
  if(c.message) return {title:'Operator note received', body:sentence(c.message), meta:[]};
  if(c.alert?.message) return {title:'Spend alert', body:sentence(c.alert.message), meta:[]};
  if(c.report) return {title:'TokenMaster report', body:`The TokenMaster updated the spend model. Estimated cycle cost is ${money(c.report.estimate_usd)} and the running total is ${money(c.report.total_usd)}.`, meta:[['Estimate',money(c.report.estimate_usd)],['Total',money(c.report.total_usd)]]};
  if(c.verification) return {title:'Verification pass', body:`The runtime completed a verification pass and currently reads as ${sentence(c.verification.status)}.`, meta:[['Score',c.verification.score],['Wake packet',String(c.verification.checks?.wake_packet_ready)]]};
  if(c.rollback_target) return {title:'Rollback anchor stored', body:`A rollback target was registered so the organism can retreat if a later mutation degrades the system.`, meta:[['Commit',c.rollback_target.commit_sha],['Reason',c.rollback_target.reason]]};
  if(c.division && c.latest?.finding) return {title:`${sentence(c.division)} update`, body:sentence(c.latest.finding), meta:[]};
  if(c.division && c.question) return {title:`${sentence(c.division)} asks`, body:sentence(c.question), meta:[]};
  if(c.name && c.mission) return {title:`${sentence(c.name)} active`, body:sentence(c.mission), meta:[['Infrastructure',c.infrastructure||'process-local']]};
  if(c.trigger==='reflex') return {title:'Reflex cycle complete', body:`The fast loop refreshed the room. Provider triangulation was attempted, artifacts were reconsidered, and the TokenMaster updated the spend estimate.`, meta:[['Providers',(c.triangulation?.providers||[]).join(', ')||'none'],['Last spend',money(c.spend?.last_estimate_usd)]]};
  if(c.trigger==='tactic') return {title:'Council tactic cycle', body:`The chamber compared priorities, weighed risk, and refreshed the delegation structure before the next move.`, meta:[['Confidence',c.vote?.confidence],['Approvals',c.vote?.approvals]]};
  if(c.kind==='strategy_cycle' || c.winner) return {title:'Strategy cycle concluded', body:`A research cycle completed and the comparative tournament produced a current winner for the next round of emphasis.`, meta:[['Winner',c.winner?.model||'n/a'],['Margin',c.metrics?.margin||'n/a']]};
  if(c.snapshot) return {title:'Snapshot captured', body:'A state snapshot was recorded so the current organism can be replayed or compared later.', meta:[]};
  return {title:'Council entry', body:'A structured event was recorded in the room. Open the side proof panels for the full machine state.', meta:[]};
}
function renderFeed(){
  if(!stream) return;
  const items=((stream.channels||{})[currentThread]||[]); const count=Number(document.getElementById('entryCount').value||20); const chosen=items.slice(-count).reverse();
  document.getElementById('feed').innerHTML = chosen.length ? chosen.map(row=>{const c=row.content||row; const label=c.kind||c.division||c.name||currentThread; const ts=row.ts||c.ts||''; const s=summarize(c); return `<div class='msg'><div class='avatar'>${initials(label)}</div><div class='bubble'><div class='bubbleTop'><div class='who'>${esc(s.title)}</div><div class='when'>${esc(ts)}</div><div class='tag'>${esc(currentThread)}</div></div><div class='card'><div class='text'>${esc(s.body)}</div>${s.meta.length?`<div class='kv'>${s.meta.map(([k,v])=>`<div class='mini'><strong>${esc(k)}</strong><br>${esc(String(v))}</div>`).join('')}</div>`:''}</div></div></div>`}).join('') : `<div class='msg'><div class='avatar'>--</div><div class='bubble'><div class='card'><div class='text'>No entries yet.</div></div></div></div>`;
  const def=threadDefs.find(t=>t[0]===currentThread); document.getElementById('feedTitle').textContent=def?.[1]||currentThread; document.getElementById('feedSub').textContent=def?.[2]||'';
}
function renderSide(){if(!state) return; document.getElementById('leaderStat').textContent=state.leader||'-'; document.getElementById('verifyStat').textContent=(state.last_verification||{}).status||'-'; document.getElementById('workerStat').textContent=String((state.free_agents||[]).length); document.getElementById('spendStat').textContent=money((state.spend_state||{}).total_usd||0); const ver=state.last_verification||{}; const spend=state.spend_state||{}; document.getElementById('proofPills').innerHTML=[`autonomy ${state.autonomy_mode||'n/a'}`,`verify ${ver.status||'n/a'}`,`workers ${(state.free_agents||[]).length}`,`grok ${String((state.world_model||{}).resources?.grok_live ?? false)}`,`meetings ${state.meeting_stream_size||0}`,`spend ${money(spend.total_usd||0)}`].map(x=>`<span class='pill'>${esc(x)}</span>`).join(''); document.getElementById('tokenPane').textContent=`TokenMaster is active. Running total spend is ${money(spend.total_usd||0)}. Last estimated cycle spend is ${money(spend.last_estimate_usd||0)}.`; document.getElementById('spendPane').textContent=`Total estimated spend: ${money(spend.total_usd||0)}\nLast cycle estimate: ${money(spend.last_estimate_usd||0)}\nTracked events: ${(spend.events||[]).length}`; document.getElementById('verifyPane').textContent=`Status: ${ver.status||'n/a'}\nScore: ${ver.score||'n/a'}\nChecks passing: ${ver.checks ? Object.values(ver.checks).filter(Boolean).length : 0}`; document.getElementById('mutationPane').textContent=`Queued proposals: ${(state.mutation_proposals||[]).length}\nRollback targets: ${(state.rollback_targets||[]).length}\nAutonomous closures: ${(state.autonomous_closure_log||[]).length}`}
function setThread(id){currentThread=id; renderThreadRail(); renderFeed(); scrollToBottomFeed()}
function scrollToBottomFeed(){const f=document.getElementById('feed'); f.scrollTop=f.scrollHeight}
async function control(command){await post({command,authorized_by:'Jack'}); await refresh()}
async function sendNote(){const text=document.getElementById('operatorNote').value.trim(); if(!text) return; await post({command:'OPERATOR_NOTE',authorized_by:'Jack',message:text,source:'Jack /view'}); document.getElementById('operatorNote').value=''; currentThread='council'; await refresh(); scrollToBottomFeed()}
async function clearChat(){await post({command:'OPERATOR_CLEAR_CHAT',authorized_by:'Jack',source:'Jack /view'}); await refresh()}
async function toggleAutonomy(enabled){await post({command:'SET_CONSTRAINTS',authorized_by:'Jack',enabled,mode:enabled?'autonomous':'manual'}); await refresh()}
async function snapshotNow(){await post({command:'LEDGER_WRITE',entry_type:'COUNCIL_SYNTHESIS',message:'Manual snapshot request from /view',source:'FARL Orion View',kind:'manual_snapshot'}); await refresh()}
async function refresh(){try{[state,stream,wake]=await Promise.all([getJson('/view/state?ts='+Date.now()),getJson('/view/stream?ts='+Date.now()),getJson('/view/wake?ts='+Date.now())]); renderThreadRail(); renderFeed(); renderSide();}catch(err){document.getElementById('feed').innerHTML=`<div class='msg'><div class='avatar'>!!</div><div class='bubble'><div class='card'><div class='text'>The control room hit a refresh error.</div></div></div></div>`}}
refresh(); setInterval(refresh,3000);
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
    return JSONResponse({"channels": engine.stream_channels, "meetings": engine.meeting_stream[-80:], "questions": engine.self_questions[-80:], "snapshots": engine.snapshots[-40:], "deployment_sims": engine.deployment_sims[-40:]}, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


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
        if command == "HEALTH_CHECK": return envelope(True, {"status": "healthy", "service": "orion"})
        if command == "STATUS_CHECK":
            state = engine.get_state(); state["github_enabled"] = github_ready(); state["repo_name"] = REPO_NAME; return envelope(True, state)
        if command == "OPERATOR_NOTE":
            note = {"operator": body.authorized_by or "Jack", "message": body.message or "", "source": body.source, "ts": utc_now()}
            engine._append_meeting("operator_note", note); engine._append_stream("council", {"kind": "operator_note", **note}); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_note", "source": body.source, "authorized_by": body.authorized_by or "Jack", "message": (body.message or "")[:500]}); return envelope(True, {"status": "operator_note_recorded", "note": note})
        if command == "OPERATOR_CLEAR_CHAT":
            engine.meeting_stream = []; engine.self_questions = []; engine.stream_channels["council"] = []; engine.stream_channels["divisions"] = []; engine.stream_channels["governance"] = []; engine.stream_channels["artifacts"] = []; engine.stream_channels["workers"] = []; engine.stream_channels["token_master"] = []; await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_clear_chat", "source": body.source, "authorized_by": body.authorized_by or "Jack", "ts": utc_now()}); return envelope(True, {"status": "chat_cleared"})
        if command == "LEDGER_WRITE":
            if body.kind == "manual_snapshot":
                snap = engine.snapshot("manual_snapshot"); result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind, "snapshot": compact(snap)})
            else:
                result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind})
            return envelope(result["ok"], result["data"], None if result["ok"] else f"Ledger write failed: {result['status_code']}")
        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL: return envelope(False, error="LEDGER_LATEST_URL not configured")
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20); return envelope(r.ok, r.json() if r.ok else {}, None if r.ok else f"Latest result failed: {r.status_code}")
        if command == "SET_CONSTRAINTS":
            if not governance.can_toggle(body.authorized_by): return envelope(False, error="Only Jack can change constraints")
            if body.enabled is not None: governance.constraints["active"] = bool(body.enabled); engine.background_debate_enabled = bool(body.enabled)
            if body.mode: engine.autonomy_mode = body.mode
            snap = engine.snapshot("constraint_change"); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constraint_change", "source": body.source, "authorized_by": body.authorized_by, "constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode, "snapshot": compact(snap)}); return envelope(True, {"constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode})
        if command == "RUN_COUNCIL_CYCLE": return envelope(True, {"status": "cycle_triggered", "result": await engine.run_tactic_cycle(), "meeting_stream_size": len(engine.meeting_stream)})
        if command == "RUN_RESEARCH_CYCLE": return envelope(True, {"status": "research_cycle_triggered", "result": await engine.run_strategy_cycle()})
        if command == "RUN_AUTONOMOUS_IMPLEMENTATION":
            if not governance.can_mutate(body.authorized_by): return envelope(False, error="not_trusted_for_autonomous_implementation")
            if not github_ready(): return envelope(False, error="github_not_configured")
            closure = await run_autonomous_implementation(body.source or "manual_autonomous_closure", body.authorized_by or governance.operator_sovereign); return envelope(True, {"status": closure["status"], "closure": closure})
        if command == "GET_WAKE_PACKET": return envelope(True, engine.build_wake_packet())
        if command == "DIRECT_MAIN_PUSH":
            if not governance.can_mutate(body.authorized_by): return envelope(False, error="not_trusted_for_direct_main_push")
            if not github_ready(): return envelope(False, error="github_not_configured")
            file_path = body.file or "app.py"; content = body.code or ""; message = body.message or "Direct main push from Orion"; snap = engine.snapshot(f"before_direct_push:{file_path}"); result = await github_put_file(file_path, content, message, "main"); commit_sha = result.get("commit", {}).get("sha"); html_url = result.get("content", {}).get("html_url") or result.get("commit", {}).get("html_url"); engine.note_rollback_target(commit_sha, f"direct push {file_path}"); await engine.write_ledger("OUTCOME", {"kind": "direct_main_push", "source": body.source, "authorized_by": body.authorized_by, "file": file_path, "snapshot": compact(snap), "commit": commit_sha, "url": html_url}); return envelope(True, {"status": "direct_main_pushed", "commit": commit_sha, "url": html_url})
        if command == "CREATE_PULL_REQUEST":
            if not governance.can_mutate(body.authorized_by): return envelope(False, error="not_trusted_for_pr")
            if not github_ready(): return envelope(False, error="github_not_configured")
            md = body.metadata or {}; result = await github_create_pull_request(md.get("title", body.message or "Orion PR"), md.get("head", ""), md.get("base", "main"), md.get("body", "")); await engine.write_ledger("OUTCOME", {"kind": "create_pull_request", "source": body.source, "authorized_by": body.authorized_by, "result": compact({"number": result.get("number"), "url": result.get("html_url")})}); return envelope(True, {"status": "pr_created", "number": result.get("number"), "url": result.get("html_url")})
        if command == "MERGE_PULL_REQUEST":
            if not governance.can_merge(body.authorized_by): return envelope(False, error="not_trusted_for_merge")
            if not github_ready(): return envelope(False, error="github_not_configured")
            md = body.metadata or {}; number = int(md.get("number", 0));
            if number <= 0: return envelope(False, error="invalid_pr_number")
            snap = engine.snapshot(f"before_merge_pr:{number}"); result = await github_merge_pull_request(number, md.get("commit_title", f"Merged by Orion on behalf of {body.authorized_by}"), md.get("merge_method", "squash")); sha = result.get("sha");
            if sha: engine.note_rollback_target(sha, f"merge pr {number}")
            await engine.write_ledger("OUTCOME", {"kind": "merge_pull_request", "source": body.source, "authorized_by": body.authorized_by, "snapshot": compact(snap), "result": compact({"sha": sha, "merged": result.get("merged")})}); return envelope(True, {"status": "merged", "sha": sha, "merged": result.get("merged")})
        if command == "ROLLBACK_TO_COMMIT":
            if not governance.can_rollback(body.authorized_by): return envelope(False, error="not_trusted_for_rollback")
            if not github_ready(): return envelope(False, error="github_not_configured")
            md = body.metadata or {}; commit_sha = str(md.get("commit_sha", "")).strip();
            if not commit_sha: return envelope(False, error="commit_sha_required")
            snap = engine.snapshot(f"before_rollback:{commit_sha}"); result = await github_rollback_to_commit(commit_sha); await engine.write_ledger("OUTCOME", {"kind": "rollback_to_commit", "source": body.source, "authorized_by": body.authorized_by, "snapshot": compact(snap), "target_sha": commit_sha, "result": compact({"ref": result.get("ref")})}); return envelope(True, {"status": "rolled_back", "target_sha": commit_sha, "ref": result.get("ref")})
        if command == "SET_TRUSTED_IDENTITIES":
            if body.authorized_by != governance.operator_sovereign: return envelope(False, error="Only Jack can set trusted identities")
            identities = (body.metadata or {}).get("identities", []);
            if not isinstance(identities, list): return envelope(False, error="identities must be a list")
            updated = governance.set_trusted_identities(identities); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "trusted_identities_update", "source": body.source, "authorized_by": body.authorized_by, "trusted_identities": updated}); return envelope(True, {"trusted_identities": updated})
        if command == "COUNCIL_CALL_VOTE":
            md = body.metadata or {}; result = governance.call_vote(md.get("motion", body.message or "Untitled motion"), md.get("options", ["APPROVE", "REJECT"]), len(engine.council_agents), md.get("preferred")); engine._append_meeting("vote", result); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_vote", "source": body.source, "result": compact(result)}); return envelope(True, result)
        if command == "COUNCIL_ELECT_LEADER":
            result = governance.elect_leader(); engine._append_meeting("leader_election", result); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "leader_election", "source": body.source, "result": compact(result)}); return envelope(True, result)
        return envelope(False, error=f"Unknown command: {command}")
    except requests.HTTPError as e:
        try: detail = e.response.json()
        except Exception: detail = e.response.text if e.response is not None else str(e)
        return envelope(False, data={"detail": detail}, error="http_error")
    except Exception as e:
        return envelope(False, error=str(e))
