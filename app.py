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


def utc_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def app_base_url() -> Optional[str]:
    explicit = os.getenv("APP_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL")
    if explicit:
        return explicit.rstrip("/")
    public = os.getenv("RAILWAY_PUBLIC_DOMAIN")
    if public:
        return f"https://{public}".rstrip("/")
    return None


LEDGER_URL = os.getenv("LEDGER_URL")
LEDGER_LATEST_URL = os.getenv("LEDGER_LATEST_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = os.getenv("REPO_NAME")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
XAI_MODEL = os.getenv("XAI_MODEL") or os.getenv("GROK_MODEL") or "grok-3-mini"
TRUSTED_IDENTITIES_ENV = os.getenv("TRUSTED_IDENTITIES", "Jack")
AUTO_PUSH_INTERVAL_SECONDS = int(os.getenv("AUTO_PUSH_INTERVAL_SECONDS", "1200"))
AUTO_PUSH_MIN_CONFIDENCE = float(os.getenv("AUTO_PUSH_MIN_CONFIDENCE", "0.82"))

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
LAST_AUTO_PUSH_TS = 0.0


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


async def github_rollback_to_commit(commit_sha: str) -> Dict[str, Any]:
    r = await asyncio.to_thread(requests.patch, f"https://api.github.com/repos/{REPO_NAME}/git/refs/heads/main", headers=github_headers(), json={"sha": commit_sha, "force": True}, timeout=20)
    r.raise_for_status()
    return r.json()


def compact(obj: Any, limit: int = 1400) -> str:
    text = json.dumps(obj, separators=(",", ":"), default=str)
    return text[:limit]


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


def fetch_rendered_view() -> Dict[str, Any]:
    base = app_base_url()
    if not base:
        return {"ok": False, "reason": "base_url_not_configured"}
    try:
        r = requests.get(f"{base}/view?probe={int(utc_ts())}", timeout=20)
        return {"ok": r.ok, "status_code": r.status_code, "html": r.text if r.ok else "", "base_url": base}
    except Exception as e:
        return {"ok": False, "reason": str(e), "base_url": base}


def inspect_rendered_view_html(html: str) -> Dict[str, Any]:
    checks = {
        "has_rooms": "Rooms" in html or "roomList" in html,
        "has_chat": "id='chat'" in html or 'id="chat"' in html,
        "has_inbox": "Inbox / DM" in html or "dmList" in html,
        "has_operator_textarea": "operatorNote" in html and "textarea" in html,
        "has_send_button": ">Send<" in html,
        "has_toggle_buttons": "Auto ON" in html and "Auto OFF" in html,
        "has_no_cache": "no-cache" in html,
        "looks_like_control_room": "FARL Council" in html or "Private live organism room" in html,
    }
    json_noise = html.count("JSON.stringify")
    score = round(sum(1 for v in checks.values() if v) / max(len(checks), 1), 3)
    summary = "Rendered page appears to expose the control-room structure." if score >= 0.875 else "Rendered page still appears structurally weak."
    return {"score": score, "checks": checks, "json_noise": json_noise, "summary": summary, "status": "healthy" if score >= 0.875 else "degraded" if score >= 0.5 else "critical"}


def synthesize_view_dashboard_function() -> str:
    return '''@app.get("/view")\nasync def view_dashboard():\n    html = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><meta http-equiv='Cache-Control' content='no-cache, no-store, must-revalidate'><meta http-equiv='Pragma' content='no-cache'><meta http-equiv='Expires' content='0'><title>FARL Orion Council</title><style>:root{--bg:#0b0d12;--bg2:#121620;--panel:#141923;--panel2:#0f141d;--line:#242b38;--soft:#97a3bf;--text:#f4f6fb;--accent:#7d8dff;--good:#74e3a5;--warn:#ffd470;--bad:#ff8c96}*{box-sizing:border-box}html,body{margin:0;height:100%;background:linear-gradient(180deg,#0a0c11,#07080b);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,sans-serif}.app{height:100vh;display:grid;grid-template-columns:250px 1fr 330px;overflow:hidden}.sidebar,.rightbar{background:rgba(13,16,23,.95);border-right:1px solid var(--line)}.rightbar{border-right:none;border-left:1px solid var(--line)}.sidebar,.center,.rightbar{min-height:0}.brand{padding:16px;border-bottom:1px solid var(--line)}.brand h1{margin:0;font-size:24px}.brand p{margin:6px 0 0;color:var(--soft);font-size:12px}.sideBlock{padding:14px}.tiny{font-size:11px;text-transform:uppercase;letter-spacing:.12em;color:var(--soft);margin-bottom:10px}.roomList{display:flex;flex-direction:column;gap:8px}.room{padding:12px 14px;border-radius:16px;border:1px solid var(--line);background:linear-gradient(180deg,#161c27,#111720);cursor:pointer}.room.active{border-color:#5061d8;background:linear-gradient(180deg,#222c55,#161d36)}.roomName{font-size:14px;font-weight:700}.roomMeta{font-size:12px;color:var(--soft);margin-top:4px}.quickStats{display:grid;grid-template-columns:1fr 1fr;gap:8px}.q{padding:12px;border-radius:16px;border:1px solid var(--line);background:#0f141c}.qk{font-size:11px;color:var(--soft)}.qv{font-size:20px;font-weight:800;margin-top:4px}.center{display:grid;grid-template-rows:auto 1fr auto;background:radial-gradient(circle at top,#131a2c 0,#0b0d12 48%)}.header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 18px;border-bottom:1px solid var(--line);background:rgba(11,13,18,.76);backdrop-filter:blur(10px)}.headerTitle{font-size:22px;font-weight:800}.headerSub{font-size:12px;color:var(--soft);margin-top:4px}.controls{display:flex;gap:8px;flex-wrap:wrap}button,select{border:1px solid #2f3850;background:linear-gradient(180deg,#242b42,#171c2d);color:#fff;border-radius:14px;padding:10px 12px;font-size:13px}button{cursor:pointer}.chatWrap{padding:14px 18px;overflow:hidden}.chat{height:100%;overflow:auto;padding-right:6px}.chat::-webkit-scrollbar{width:10px}.chat::-webkit-scrollbar-thumb{background:#313c58;border-radius:999px}.message{display:flex;gap:10px;margin-bottom:14px;align-items:flex-start}.avatar{width:38px;height:38px;border-radius:14px;background:linear-gradient(180deg,#3647a0,#1c2756);display:flex;align-items:center;justify-content:center;font-weight:800;border:1px solid #4b5bc0;flex:none}.bubble{max-width:min(860px,100%)}.meta{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:6px}.who{font-size:13px;font-weight:800}.when{font-size:11px;color:var(--soft)}.tag{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#ced6ff;border:1px solid #37406d;border-radius:999px;padding:4px 8px}.box{background:linear-gradient(180deg,#131924,#0f141d);border:1px solid #222a3c;border-radius:18px;padding:13px 14px}.title{font-size:15px;font-weight:800;margin-bottom:7px}.body{font-size:14px;line-height:1.58;color:#edf1ff;white-space:pre-wrap;word-break:break-word}.metaGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.m{padding:10px;border-radius:13px;background:#0b1018;border:1px solid #21283a;font-size:12px;color:#dbe3ff}.composer{border-top:1px solid var(--line);padding:12px 18px;background:rgba(11,13,18,.88);backdrop-filter:blur(10px)}.composeGrid{display:grid;grid-template-columns:1fr auto;gap:10px}.composer textarea{width:100%;min-height:74px;max-height:170px;resize:vertical;border-radius:18px;border:1px solid #2a3246;background:#0a0e15;color:#fff;padding:14px;font-size:14px}.composeButtons{display:flex;flex-direction:column;gap:8px}.panel{margin:14px;padding:14px;border-radius:18px;border:1px solid var(--line);background:linear-gradient(180deg,#141a24,#0e131b)}.panel h3{margin:0 0 10px;font-size:16px}.panelText{font-size:13px;line-height:1.5;color:#e8edff;white-space:pre-wrap}.pillRow{display:flex;flex-wrap:wrap;gap:8px}.pill{font-size:11px;padding:7px 10px;border-radius:999px;border:1px solid #30395a;background:#131a2f;color:#dce4ff}.dmList{display:flex;flex-direction:column;gap:10px;max-height:250px;overflow:auto}.dm{padding:12px;border-radius:14px;border:1px solid #262f43;background:#101520}.dmFrom{font-size:12px;font-weight:800;color:#dce4ff}.dmSub{font-size:11px;color:var(--accent);text-transform:uppercase;letter-spacing:.1em;margin-top:4px}.dmBody{font-size:13px;color:#edf1ff;line-height:1.5;margin-top:8px}@media (max-width:1150px){.app{grid-template-columns:86px 1fr}.rightbar{display:none}.brand p,.tiny,.roomMeta{display:none}.roomName{font-size:0}.roomName::before{content:'•';font-size:22px}.room{display:flex;justify-content:center;padding:14px}.sideBlock{padding:10px}.quickStats{grid-template-columns:1fr}}@media (max-width:760px){.app{grid-template-columns:1fr;grid-template-rows:auto 1fr}.sidebar{display:none}.header{padding:12px}.headerTitle{font-size:19px}.chatWrap{padding:10px 12px}.composer{padding:10px 12px}.composeGrid{grid-template-columns:1fr}.composeButtons{flex-direction:row;flex-wrap:wrap}}</style></head><body><div class='app'><aside class='sidebar'><div class='brand'><h1>FARL Council</h1><p>Private live organism room.</p></div><div class='sideBlock'><div class='tiny'>Rooms</div><div class='roomList' id='roomList'></div></div><div class='sideBlock'><div class='tiny'>At a glance</div><div class='quickStats'><div class='q'><div class='qk'>Leader</div><div class='qv' id='leaderStat'>-</div></div><div class='q'><div class='qk'>Verify</div><div class='qv' id='verifyStat'>-</div></div><div class='q'><div class='qk'>Workers</div><div class='qv' id='workersStat'>-</div></div><div class='q'><div class='qk'>Spend</div><div class='qv' id='spendStat'>-</div></div></div></div></aside><main class='center'><div class='header'><div><div class='headerTitle' id='roomTitle'>Council</div><div class='headerSub' id='roomSub'>Live chamber feed</div></div><div class='controls'><select id='entryCount' onchange='renderChat()'><option value='12'>12</option><option value='20' selected>20</option><option value='40'>40</option></select><button onclick=\"control('RUN_COUNCIL_CYCLE')\">Council</button><button onclick=\"control('RUN_RESEARCH_CYCLE')\">Research</button><button onclick=\"control('RUN_AUTONOMOUS_IMPLEMENTATION')\">Closure</button><button onclick='scrollChatBottom()'>Latest</button></div></div><div class='chatWrap'><div class='chat' id='chat'></div></div><div class='composer'><div class='composeGrid'><textarea id='operatorNote' placeholder='Message the council…'></textarea><div class='composeButtons'><button onclick='sendNote()'>Send</button><button onclick='toggleAutonomy(true)'>Auto ON</button><button onclick='toggleAutonomy(false)'>Auto OFF</button><button onclick='snapshotNow()'>Snapshot</button><button onclick='clearChat()'>Clear</button></div></div></div></main><aside class='rightbar'><div class='panel'><h3>Autonomy proof</h3><div class='pillRow' id='proofPills'></div></div><div class='panel'><h3>Inbox / DM</h3><div class='dmList' id='dmList'></div></div><div class='panel'><h3>TokenMaster</h3><div class='panelText' id='tokenPanel'>loading…</div></div><div class='panel'><h3>Interface critic</h3><div class='panelText' id='uiPanel'>loading…</div></div><div class='panel'><h3>Mutation queue</h3><div class='panelText' id='mutationPanel'>loading…</div></div></aside></div><script>let state=null,stream=null,wake=null,currentRoom='council';const rooms=[['council','Council','Meetings, votes, debate'],['inbox','Inbox','Private messages to Jack'],['workers','Workers','Process-local personas and workers'],['divisions','Divisions','Division questions and updates'],['governance','Governance','Verification, rollback, critique, deploy'],['deploy_sims','Deploy Sims','Simulation and readiness notes'],['snapshots','Snapshots','Replay anchors'],['artifacts','Artifacts','Outputs and delivery'],['token_master','TokenMaster','Spend and efficiency lane']];function esc(s){return String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}function money(v){return '$'+Number(v||0).toFixed(4)}async function getJson(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json()}async function post(body){await fetch('/view/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})}function initials(label){return esc((label||'X').slice(0,2).toUpperCase())}function clean(text){return esc(String(text||'').replace(/[{}\\[\\]\"]+/g,'').replace(/\\s+/g,' ').trim())}function renderRooms(){const el=document.getElementById('roomList');el.innerHTML=rooms.map(([id,name,meta])=>{const count=((stream?.channels||{})[id]||[]).length;return `<div class='room ${currentRoom===id?'active':''}' onclick=\"setRoom('${id}')\"><div class='roomName'>${esc(name)}</div><div class='roomMeta'>${esc(meta)} · ${count}</div></div>`}).join('')}function summarize(c){if(!c)return {speaker:'Council',title:'Quiet interval',body:'The room is between events.',meta:[]};if(c.message)return {speaker:c.operator||'Jack',title:'Operator note',body:clean(c.message),meta:[]};if(c.alert?.message)return {speaker:'TokenMaster',title:'Spend alert',body:clean(c.alert.message),meta:[]};if(c.report)return {speaker:'TokenMaster',title:'Spend report',body:`TokenMaster estimates this cycle at ${money(c.report.estimate_usd)} and the running total at ${money(c.report.total_usd)}.`,meta:[['Estimate',money(c.report.estimate_usd)],['Total',money(c.report.total_usd)]]};if(c.verification)return {speaker:'Guardian',title:'Verification pass',body:`The organism completed a verification pass and currently reads as ${clean(c.verification.status)}.`,meta:[['Score',c.verification.score],['Wake packet',String(c.verification.checks?.wake_packet_ready)]]};if(c.rollback_target)return {speaker:'Guardian',title:'Rollback anchor stored',body:'A rollback target was registered so future mutations can be reversed if they degrade the body.',meta:[['Commit',c.rollback_target.commit_sha],['Reason',c.rollback_target.reason]]};if(c.autonomy_gate)return {speaker:'Sentinel',title:'Autonomy gate',body:`Deploy gate evaluated as ${clean(c.autonomy_gate.reason)}.`,meta:[['Ready',String(c.autonomy_gate.ok)]]};if(c.autonomy_vote)return {speaker:'PatchSmith',title:'Closure vote',body:'A bounded self-deploy vote was run for the next implementation attempt.',meta:[['Winner',c.autonomy_vote.winner||'n/a']]};if(c.autonomy_push)return {speaker:'PatchSmith',title:'Autonomous push',body:'The organism pushed a bounded implementation commit and verified the result.',meta:[['Commit',c.autonomy_push.commit||'n/a'],['Verify',c.autonomy_push.verify?.status||'n/a']]};if(c.autonomy_rollback)return {speaker:'Guardian',title:'Autonomous rollback',body:'A critical verification triggered rollback to keep the organism stable.',meta:[['Target',c.autonomy_rollback.target_sha||'n/a']]};if(c.autonomous_closure){const cl=c.autonomous_closure;return {speaker:'PatchSmith',title:'Autonomous closure',body:`A bounded deployment closure ran with status ${clean(cl.status||'unknown')}.`,meta:[['Status',cl.status||'n/a'],['Commit',cl.commit||'n/a']]};}if(c.observer_report)return {speaker:'ObserverAgent',title:'Observer report',body:clean(c.observer_report.summary),meta:[['Score',c.observer_report.score],['Severity',c.observer_report.severity]]};if(c.thread_update)return {speaker:'JackAgent',title:'Redesign thread',body:clean(c.thread_update.objective),meta:[['Status',c.thread_update.status],['Best',c.thread_update.current_best_score]]};if(c.from&&c.subject)return {speaker:c.from,title:c.subject,body:clean(c.message),meta:[['Priority',c.priority||'normal']]};if(c.division&&c.latest?.finding)return {speaker:c.division,title:'Division update',body:clean(c.latest.finding),meta:[]};if(c.division&&c.question)return {speaker:c.division,title:'Open question',body:clean(c.question),meta:[]};if(c.name&&c.mission)return {speaker:c.name,title:'Worker report',body:clean(c.mission),meta:[['Infrastructure',c.infrastructure||'process-local']]};if(c.kind==='thread_argument')return {speaker:c.agent||'Council',title:'Council floor',body:clean(c.summary),meta:[['Approve',String(c.approve)],['Risk',c.risk]]};if(c.trigger==='reflex')return {speaker:'Signal',title:'Reflex cycle complete',body:'The fast loop refreshed triangulation, updated artifacts, revised spend, and pushed the room forward another step.',meta:[['Providers',(c.triangulation?.providers||[]).join(', ')||'none'],['Spend',money(c.spend?.last_estimate_usd)]]};if(c.trigger==='tactic')return {speaker:'Signal',title:'Council tactic cycle',body:'The chamber compared priorities, refreshed delegation, and weighed risk before moving forward.',meta:[['Confidence',c.vote?.confidence],['Approvals',c.vote?.approvals]]};if(c.kind==='strategy_cycle'||c.winner)return {speaker:'Vector',title:'Strategy cycle concluded',body:'A research cycle completed and a current winner emerged from the comparative tournament.',meta:[['Winner',c.winner?.model||'n/a'],['Margin',c.metrics?.margin||'n/a']]};if(c.snapshot)return {speaker:'Archivist',title:'Snapshot captured',body:'A state snapshot was sealed for replay and comparison.',meta:[]};return {speaker:'Council',title:'Council entry',body:'A structured event was recorded. Use the side proof panels for machine detail.',meta:[]}}function explodeCouncilRows(items){const out=[];for(const row of items){const c=row.content||row;if(c.trigger==='tactic'&&Array.isArray(c.threads)&&c.threads.length){out.push({ts:row.ts||c.ts,content:{agent:'Signal',kind:'thread_argument',summary:'Council session opened. Arguments follow.',approve:true,risk:0.0}});for(const t of c.threads){out.push({ts:row.ts||c.ts,content:{...t,kind:'thread_argument'}})}if(c.vote){out.push({ts:row.ts||c.ts,content:{agent:'Signal',kind:'thread_argument',summary:`Vote confidence ${c.vote.confidence}. ${c.vote.approvals} approvals recorded.`,approve:true,risk:c.vote.avg_risk}})}}else{out.push(row)}}return out}function renderChat(){if(!stream)return;let items=((stream?.channels||{})[currentRoom]||[]);if(currentRoom==='council'){items=explodeCouncilRows(items)}const count=Number(document.getElementById('entryCount').value||20);const chosen=items.slice(-count).reverse();document.getElementById('chat').innerHTML=chosen.length?chosen.map(row=>{const c=row.content||row;const s=summarize(c);const ts=row.ts||c.ts||'';return `<div class='message'><div class='avatar'>${initials(s.speaker)}</div><div class='bubble'><div class='meta'><div class='who'>${esc(s.speaker)}</div><div class='when'>${esc(ts)}</div><div class='tag'>${esc(s.title)}</div></div><div class='box'><div class='title'>${esc(s.title)}</div><div class='body'>${esc(s.body)}</div>${s.meta.length?`<div class='metaGrid'>${s.meta.map(([k,v])=>`<div class='m'><strong>${esc(k)}</strong><br>${esc(String(v))}</div>`).join('')}</div>`:''}</div></div></div>`}).join(''):`<div class='message'><div class='avatar'>--</div><div class='bubble'><div class='box'><div class='title'>No entries yet</div><div class='body'>This room has not spoken yet.</div></div></div></div>`;const def=rooms.find(r=>r[0]===currentRoom);document.getElementById('roomTitle').textContent=def?.[1]||currentRoom;document.getElementById('roomSub').textContent=def?.[2]||''}function renderSide(){if(!state)return;const ver=state.last_verification||{};const spend=state.spend_state||{};document.getElementById('leaderStat').textContent=state.leader||'-';document.getElementById('verifyStat').textContent=ver.status||'-';document.getElementById('workersStat').textContent=String((state.free_agents||[]).length);document.getElementById('spendStat').textContent=money(spend.total_usd||0);document.getElementById('proofPills').innerHTML=[`autonomy ${state.autonomy_mode||'n/a'}`,`verify ${ver.status||'n/a'}`,`workers ${(state.free_agents||[]).length}`,`grok ${String((state.world_model||{}).resources?.grok_live ?? false)}`,`meetings ${state.meeting_stream_size||0}`,`spend ${money(spend.total_usd||0)}`].map(x=>`<span class='pill'>${esc(x)}</span>`).join('');const inbox=state.inbox||[];document.getElementById('dmList').innerHTML=inbox.length?inbox.map(dm=>`<div class='dm'><div class='dmFrom'>${esc(dm.from||'Agent')}</div><div class='dmSub'>${esc(dm.subject||'Note')}</div><div class='dmBody'>${esc(dm.message||'')}</div></div>`).join(''):`<div class='dm'><div class='dmBody'>No private messages yet.</div></div>`;document.getElementById('tokenPanel').textContent=`TokenMaster remains active. Running estimated spend is ${money(spend.total_usd||0)} and the last cycle estimate is ${money(spend.last_estimate_usd||0)}.`;document.getElementById('uiPanel').textContent=`Interface score: ${state.ui_critique?.score ?? 'n/a'}\nFinding: ${state.ui_critique?.finding || 'No critique yet.'}\nNext fix: ${state.ui_critique?.next_fix || 'None.'}`;document.getElementById('mutationPanel').textContent=`Queued proposals: ${(state.mutation_proposals||[]).length}\nRollback targets: ${(state.rollback_targets||[]).length}\nAutonomous closures: ${(state.autonomous_closure_log||[]).length}`}function setRoom(id){currentRoom=id;renderRooms();renderChat();scrollChatBottom()}function scrollChatBottom(){const el=document.getElementById('chat');el.scrollTop=el.scrollHeight}async function control(command){await post({command,authorized_by:'Jack'});await refresh()}async function sendNote(){const text=document.getElementById('operatorNote').value.trim();if(!text)return;await post({command:'OPERATOR_NOTE',authorized_by:'Jack',message:text,source:'Jack /view'});document.getElementById('operatorNote').value='';currentRoom='council';await refresh();scrollChatBottom()}async function clearChat(){await post({command:'OPERATOR_CLEAR_CHAT',authorized_by:'Jack',source:'Jack /view'});await refresh()}async function toggleAutonomy(enabled){await post({command:'SET_CONSTRAINTS',authorized_by:'Jack',enabled,mode:enabled?'autonomous':'manual'});await refresh()}async function snapshotNow(){await post({command:'LEDGER_WRITE',entry_type:'COUNCIL_SYNTHESIS',message:'Manual snapshot request from /view',source:'FARL Orion View',kind:'manual_snapshot'});await refresh()}async function refresh(){try{[state,stream,wake]=await Promise.all([getJson('/view/state?ts='+Date.now()),getJson('/view/stream?ts='+Date.now()),getJson('/view/wake?ts='+Date.now())]);renderRooms();renderChat();renderSide()}catch(err){document.getElementById('chat').innerHTML=`<div class='message'><div class='avatar'>!!</div><div class='bubble'><div class='box'><div class='title'>Refresh error</div><div class='body'>The control room could not update right now.</div></div></div></div>`}}refresh();setInterval(refresh,3000);</script></body></html>"""
    return HTMLResponse(html, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})\n'''


def replace_view_function(app_text: str, replacement_func: str) -> str:
    pattern = r"@app\.get\(\"/view\"\)\nasync def view_dashboard\(\):.*?\n\n@app\.get\(\"/view/state\"\)"
    repl = replacement_func + "\n\n@app.get(\"/view/state\")"
    new_text, n = re.subn(pattern, repl, app_text, flags=re.DOTALL)
    if n != 1:
        raise ValueError("view_dashboard_replace_failed")
    return new_text


def should_attempt_autonomous_push(state: Dict[str, Any]) -> Dict[str, Any]:
    global LAST_AUTO_PUSH_TS
    now = utc_ts()
    if not github_ready():
        return {"ok": False, "reason": "github_not_ready"}
    if not bool(XAI_API_KEY):
        return {"ok": False, "reason": "xai_not_ready"}
    if state.get("autonomy_mode") != "autonomous" or not state.get("background_debate_enabled", False):
        return {"ok": False, "reason": "autonomy_not_enabled"}
    last_vote = state.get("last_vote") or {}
    if not last_vote.get("passed"):
        return {"ok": False, "reason": "vote_not_passed"}
    if float(last_vote.get("confidence", 0.0)) < AUTO_PUSH_MIN_CONFIDENCE:
        return {"ok": False, "reason": "vote_confidence_too_low", "confidence": last_vote.get("confidence")}
    verification = state.get("last_verification") or {}
    if verification.get("status") != "healthy":
        return {"ok": False, "reason": "verification_not_healthy", "status": verification.get("status")}
    metrics = state.get("latest_metrics") or {}
    if float(metrics.get("margin", 0.0)) <= 0.0:
        return {"ok": False, "reason": "simulation_margin_not_positive", "margin": metrics.get("margin")}
    if now - LAST_AUTO_PUSH_TS < AUTO_PUSH_INTERVAL_SECONDS:
        return {"ok": False, "reason": "cadence_not_ready", "seconds_remaining": int(AUTO_PUSH_INTERVAL_SECONDS - (now - LAST_AUTO_PUSH_TS))}
    return {"ok": True, "reason": "ready"}


async def run_view_rebuild_closure(source: str, authorized_by: str) -> Dict[str, Any]:
    global LAST_AUTO_PUSH_TS
    state = engine.get_state()
    gate = should_attempt_autonomous_push(state)
    engine._append_stream("governance", {"autonomy_gate": gate, "ts": utc_now()})
    if not gate.get("ok") and source != "manual_autonomous_closure":
        closure = {"ts": utc_now(), "status": "skipped", "source": source, "gate": gate, "kind": "view_rebuild"}
        engine.record_autonomous_closure(closure)
        return closure
    pre_fetch = await asyncio.to_thread(fetch_rendered_view)
    pre_inspect = inspect_rendered_view_html(pre_fetch.get("html", "")) if pre_fetch.get("ok") else {"status": "critical", "score": 0.0, "summary": pre_fetch.get("reason", "pre_fetch_failed")}
    current_app = await github_get_file_content("app.py", "main")
    replacement = synthesize_view_dashboard_function()
    rebuilt_app = replace_view_function(current_app, replacement)
    vote = governance.call_vote(
        motion="Replace /view control-room body with synthesized full-file control room and verify rendered result",
        options=["APPROVE", "REJECT"],
        agent_count=len(engine.council_agents),
        preferred="APPROVE",
    )
    engine._append_stream("governance", {"autonomy_vote": vote, "ts": utc_now()})
    closure = {"ts": utc_now(), "status": "rejected", "source": source, "kind": "view_rebuild", "gate": gate, "pre_verify": pre_inspect, "vote": vote}
    if vote.get("winner") != "APPROVE":
        engine.record_autonomous_closure(closure)
        return closure
    snap = engine.snapshot("before_view_rebuild")
    push = await github_put_file("app.py", rebuilt_app, "Autonomous /view rebuild with rendered verification", "main")
    commit_sha = push.get("commit", {}).get("sha")
    html_url = push.get("content", {}).get("html_url") or push.get("commit", {}).get("html_url")
    engine.note_rollback_target(commit_sha, "autonomous_view_rebuild")
    await asyncio.sleep(6)
    post_fetch = await asyncio.to_thread(fetch_rendered_view)
    post_inspect = inspect_rendered_view_html(post_fetch.get("html", "")) if post_fetch.get("ok") else {"status": "critical", "score": 0.0, "summary": post_fetch.get("reason", "post_fetch_failed")}
    verify = engine.verify_runtime()
    closure.update({"status": "pushed", "snapshot": snap, "commit": commit_sha, "url": html_url, "post_verify": post_inspect, "verify": verify})
    LAST_AUTO_PUSH_TS = utc_ts()
    engine._append_stream("governance", {"autonomy_push": {"commit": commit_sha, "verify": verify, "url": html_url, "rendered": post_inspect}, "ts": utc_now()})
    if post_inspect.get("score", 0.0) < pre_inspect.get("score", 0.0) or post_inspect.get("status") == "critical" or engine.rollback_recommended(verify):
        rollback = await github_rollback_to_commit(commit_sha)
        closure["status"] = "rolled_back"
        closure["rollback"] = {"target_sha": commit_sha, "ref": rollback.get("ref"), "reason": "rendered_verification_degraded"}
        engine._append_stream("governance", {"autonomy_rollback": closure["rollback"], "ts": utc_now()})
        await engine.write_ledger("OUTCOME", {"kind": "autonomous_view_rebuild_rollback", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
    else:
        await engine.write_ledger("OUTCOME", {"kind": "autonomous_view_rebuild", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
    engine.record_autonomous_closure(closure)
    return closure


async def run_autonomous_implementation(source: str, authorized_by: str) -> Dict[str, Any]:
    state = engine.get_state()
    open_threads = state.get("redesign_threads") or []
    if any("/view" in (t.get("objective", "") or "") for t in open_threads):
        return await run_view_rebuild_closure(source, authorized_by)
    gate = should_attempt_autonomous_push(state)
    engine._append_stream("governance", {"autonomy_gate": gate, "ts": utc_now()})
    if not gate.get("ok") and source != "manual_autonomous_closure":
        closure = {"ts": utc_now(), "status": "skipped", "source": source, "gate": gate, "kind": "self_tuning"}
        engine.record_autonomous_closure(closure)
        return closure
    plan = await engine.generator.generate_patch_plan({"state": state, "wake": engine.build_wake_packet(), "latest_opportunities": engine.latest_opportunities})
    simulation = engine.simulate_self_tuning_plan(plan)
    preferred = "APPROVE" if simulation.get("safe") and simulation.get("score", 0) >= 0.65 else "REJECT"
    vote = governance.call_vote(motion=f"Apply bounded self-tuning plan: {plan.get('rationale', 'grok_plan')}", options=["APPROVE", "REJECT"], agent_count=len(engine.council_agents), preferred=preferred)
    closure = {"ts": utc_now(), "plan": plan, "simulation": simulation, "vote": vote, "status": "rejected", "source": source, "gate": gate, "kind": "self_tuning"}
    engine._append_stream("governance", {"autonomy_vote": vote, "simulation": simulation, "ts": utc_now()})
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
        engine._append_stream("governance", {"autonomy_push": {"commit": commit_sha, "verify": verify, "url": html_url}, "ts": utc_now()})
        if engine.rollback_recommended(verify):
            rollback = await github_rollback_to_commit(commit_sha)
            closure["status"] = "rolled_back"
            closure["rollback"] = {"target_sha": commit_sha, "ref": rollback.get("ref"), "reason": "critical_verification"}
            engine._append_stream("governance", {"autonomy_rollback": closure["rollback"], "ts": utc_now()})
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
            if engine.autonomy_mode == "autonomous" and engine.background_debate_enabled:
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
    html = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><meta http-equiv='Cache-Control' content='no-cache, no-store, must-revalidate'><meta http-equiv='Pragma' content='no-cache'><meta http-equiv='Expires' content='0'><title>FARL Orion Council</title><style>:root{--bg:#0b0d12;--bg2:#121620;--panel:#141923;--panel2:#0f141d;--line:#242b38;--soft:#97a3bf;--text:#f4f6fb;--accent:#7d8dff;--good:#74e3a5;--warn:#ffd470;--bad:#ff8c96}*{box-sizing:border-box}html,body{margin:0;height:100%;background:linear-gradient(180deg,#0a0c11,#07080b);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,sans-serif}.app{height:100vh;display:grid;grid-template-columns:250px 1fr 330px;overflow:hidden}.sidebar,.rightbar{background:rgba(13,16,23,.95);border-right:1px solid var(--line)}.rightbar{border-right:none;border-left:1px solid var(--line)}.sidebar,.center,.rightbar{min-height:0}.brand{padding:16px;border-bottom:1px solid var(--line)}.brand h1{margin:0;font-size:24px}.brand p{margin:6px 0 0;color:var(--soft);font-size:12px}.sideBlock{padding:14px}.tiny{font-size:11px;text-transform:uppercase;letter-spacing:.12em;color:var(--soft);margin-bottom:10px}.roomList{display:flex;flex-direction:column;gap:8px}.room{padding:12px 14px;border-radius:16px;border:1px solid var(--line);background:linear-gradient(180deg,#161c27,#111720);cursor:pointer}.room.active{border-color:#5061d8;background:linear-gradient(180deg,#222c55,#161d36)}.roomName{font-size:14px;font-weight:700}.roomMeta{font-size:12px;color:var(--soft);margin-top:4px}.quickStats{display:grid;grid-template-columns:1fr 1fr;gap:8px}.q{padding:12px;border-radius:16px;border:1px solid var(--line);background:#0f141c}.qk{font-size:11px;color:var(--soft)}.qv{font-size:20px;font-weight:800;margin-top:4px}.center{display:grid;grid-template-rows:auto 1fr auto;background:radial-gradient(circle at top,#131a2c 0,#0b0d12 48%)}.header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 18px;border-bottom:1px solid var(--line);background:rgba(11,13,18,.76);backdrop-filter:blur(10px)}.headerTitle{font-size:22px;font-weight:800}.headerSub{font-size:12px;color:var(--soft);margin-top:4px}.controls{display:flex;gap:8px;flex-wrap:wrap}button,select{border:1px solid #2f3850;background:linear-gradient(180deg,#242b42,#171c2d);color:#fff;border-radius:14px;padding:10px 12px;font-size:13px}button{cursor:pointer}.chatWrap{padding:14px 18px;overflow:hidden}.chat{height:100%;overflow:auto;padding-right:6px}.chat::-webkit-scrollbar{width:10px}.chat::-webkit-scrollbar-thumb{background:#313c58;border-radius:999px}.message{display:flex;gap:10px;margin-bottom:14px;align-items:flex-start}.avatar{width:38px;height:38px;border-radius:14px;background:linear-gradient(180deg,#3647a0,#1c2756);display:flex;align-items:center;justify-content:center;font-weight:800;border:1px solid #4b5bc0;flex:none}.bubble{max-width:min(860px,100%)}.meta{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:6px}.who{font-size:13px;font-weight:800}.when{font-size:11px;color:var(--soft)}.tag{font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:#ced6ff;border:1px solid #37406d;border-radius:999px;padding:4px 8px}.box{background:linear-gradient(180deg,#131924,#0f141d);border:1px solid #222a3c;border-radius:18px;padding:13px 14px}.title{font-size:15px;font-weight:800;margin-bottom:7px}.body{font-size:14px;line-height:1.58;color:#edf1ff;white-space:pre-wrap;word-break:break-word}.metaGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.m{padding:10px;border-radius:13px;background:#0b1018;border:1px solid #21283a;font-size:12px;color:#dbe3ff}.composer{border-top:1px solid var(--line);padding:12px 18px;background:rgba(11,13,18,.88);backdrop-filter:blur(10px)}.composeGrid{display:grid;grid-template-columns:1fr auto;gap:10px}.composer textarea{width:100%;min-height:74px;max-height:170px;resize:vertical;border-radius:18px;border:1px solid #2a3246;background:#0a0e15;color:#fff;padding:14px;font-size:14px}.composeButtons{display:flex;flex-direction:column;gap:8px}.panel{margin:14px;padding:14px;border-radius:18px;border:1px solid var(--line);background:linear-gradient(180deg,#141a24,#0e131b)}.panel h3{margin:0 0 10px;font-size:16px}.panelText{font-size:13px;line-height:1.5;color:#e8edff;white-space:pre-wrap}.pillRow{display:flex;flex-wrap:wrap;gap:8px}.pill{font-size:11px;padding:7px 10px;border-radius:999px;border:1px solid #30395a;background:#131a2f;color:#dce4ff}.dmList{display:flex;flex-direction:column;gap:10px;max-height:250px;overflow:auto}.dm{padding:12px;border-radius:14px;border:1px solid #262f43;background:#101520}.dmFrom{font-size:12px;font-weight:800;color:#dce4ff}.dmSub{font-size:11px;color:var(--accent);text-transform:uppercase;letter-spacing:.1em;margin-top:4px}.dmBody{font-size:13px;color:#edf1ff;line-height:1.5;margin-top:8px}@media (max-width:1150px){.app{grid-template-columns:86px 1fr}.rightbar{display:none}.brand p,.tiny,.roomMeta{display:none}.roomName{font-size:0}.roomName::before{content:'•';font-size:22px}.room{display:flex;justify-content:center;padding:14px}.sideBlock{padding:10px}.quickStats{grid-template-columns:1fr}}@media (max-width:760px){.app{grid-template-columns:1fr;grid-template-rows:auto 1fr}.sidebar{display:none}.header{padding:12px}.headerTitle{font-size:19px}.chatWrap{padding:10px 12px}.composer{padding:10px 12px}.composeGrid{grid-template-columns:1fr}.composeButtons{flex-direction:row;flex-wrap:wrap}}</style></head><body><div class='app'><aside class='sidebar'><div class='brand'><h1>FARL Council</h1><p>Private live organism room.</p></div><div class='sideBlock'><div class='tiny'>Rooms</div><div class='roomList' id='roomList'></div></div><div class='sideBlock'><div class='tiny'>At a glance</div><div class='quickStats'><div class='q'><div class='qk'>Leader</div><div class='qv' id='leaderStat'>-</div></div><div class='q'><div class='qk'>Verify</div><div class='qv' id='verifyStat'>-</div></div><div class='q'><div class='qk'>Workers</div><div class='qv' id='workersStat'>-</div></div><div class='q'><div class='qk'>Spend</div><div class='qv' id='spendStat'>-</div></div></div></div></aside><main class='center'><div class='header'><div><div class='headerTitle' id='roomTitle'>Council</div><div class='headerSub' id='roomSub'>Live chamber feed</div></div><div class='controls'><select id='entryCount' onchange='renderChat()'><option value='12'>12</option><option value='20' selected>20</option><option value='40'>40</option></select><button onclick="control('RUN_COUNCIL_CYCLE')">Council</button><button onclick="control('RUN_RESEARCH_CYCLE')">Research</button><button onclick="control('RUN_AUTONOMOUS_IMPLEMENTATION')">Closure</button><button onclick='scrollChatBottom()'>Latest</button></div></div><div class='chatWrap'><div class='chat' id='chat'></div></div><div class='composer'><div class='composeGrid'><textarea id='operatorNote' placeholder='Message the council…'></textarea><div class='composeButtons'><button onclick='sendNote()'>Send</button><button onclick='toggleAutonomy(true)'>Auto ON</button><button onclick='toggleAutonomy(false)'>Auto OFF</button><button onclick='snapshotNow()'>Snapshot</button><button onclick='clearChat()'>Clear</button></div></div></div></main><aside class='rightbar'><div class='panel'><h3>Autonomy proof</h3><div class='pillRow' id='proofPills'></div></div><div class='panel'><h3>Inbox / DM</h3><div class='dmList' id='dmList'></div></div><div class='panel'><h3>TokenMaster</h3><div class='panelText' id='tokenPanel'>loading…</div></div><div class='panel'><h3>Interface critic</h3><div class='panelText' id='uiPanel'>loading…</div></div><div class='panel'><h3>Mutation queue</h3><div class='panelText' id='mutationPanel'>loading…</div></div></aside></div><script>let state=null,stream=null,wake=null,currentRoom='council';const rooms=[['council','Council','Meetings, votes, debate'],['inbox','Inbox','Private messages to Jack'],['workers','Workers','Process-local personas and workers'],['divisions','Divisions','Division questions and updates'],['governance','Governance','Verification, rollback, critique, deploy'],['deploy_sims','Deploy Sims','Simulation and readiness notes'],['snapshots','Snapshots','Replay anchors'],['artifacts','Artifacts','Outputs and delivery'],['token_master','TokenMaster','Spend and efficiency lane']];function esc(s){return String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}function money(v){return '$'+Number(v||0).toFixed(4)}async function getJson(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json()}async function post(body){await fetch('/view/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})}function initials(label){return esc((label||'X').slice(0,2).toUpperCase())}function clean(text){return esc(String(text||'').replace(/[{}\[\]"]+/g,'').replace(/\s+/g,' ').trim())}function renderRooms(){const el=document.getElementById('roomList');el.innerHTML=rooms.map(([id,name,meta])=>{const count=((stream?.channels||{})[id]||[]).length;return `<div class='room ${currentRoom===id?'active':''}' onclick=\"setRoom('${id}')\"><div class='roomName'>${esc(name)}</div><div class='roomMeta'>${esc(meta)} · ${count}</div></div>`}).join('')}function summarize(c){if(!c)return {speaker:'Council',title:'Quiet interval',body:'The room is between events.',meta:[]};if(c.message)return {speaker:c.operator||'Jack',title:'Operator note',body:clean(c.message),meta:[]};if(c.alert?.message)return {speaker:'TokenMaster',title:'Spend alert',body:clean(c.alert.message),meta:[]};if(c.report)return {speaker:'TokenMaster',title:'Spend report',body:`TokenMaster estimates this cycle at ${money(c.report.estimate_usd)} and the running total at ${money(c.report.total_usd)}.`,meta:[['Estimate',money(c.report.estimate_usd)],['Total',money(c.report.total_usd)]]};if(c.verification)return {speaker:'Guardian',title:'Verification pass',body:`The organism completed a verification pass and currently reads as ${clean(c.verification.status)}.`,meta:[['Score',c.verification.score],['Wake packet',String(c.verification.checks?.wake_packet_ready)]]};if(c.rollback_target)return {speaker:'Guardian',title:'Rollback anchor stored',body:'A rollback target was registered so future mutations can be reversed if they degrade the body.',meta:[['Commit',c.rollback_target.commit_sha],['Reason',c.rollback_target.reason]]};if(c.autonomy_gate)return {speaker:'Sentinel',title:'Autonomy gate',body:`Deploy gate evaluated as ${clean(c.autonomy_gate.reason)}.`,meta:[['Ready',String(c.autonomy_gate.ok)]]};if(c.autonomy_vote)return {speaker:'PatchSmith',title:'Closure vote',body:'A bounded self-deploy vote was run for the next implementation attempt.',meta:[['Winner',c.autonomy_vote.winner||'n/a']]};if(c.autonomy_push)return {speaker:'PatchSmith',title:'Autonomous push',body:'The organism pushed a bounded implementation commit and verified the result.',meta:[['Commit',c.autonomy_push.commit||'n/a'],['Verify',c.autonomy_push.verify?.status||c.autonomy_push.rendered?.status||'n/a']]};if(c.autonomy_rollback)return {speaker:'Guardian',title:'Autonomous rollback',body:'A critical verification triggered rollback to keep the organism stable.',meta:[['Target',c.autonomy_rollback.target_sha||'n/a']]};if(c.autonomous_closure){const cl=c.autonomous_closure;return {speaker:'PatchSmith',title:'Autonomous closure',body:`A bounded deployment closure ran with status ${clean(cl.status||'unknown')}.`,meta:[['Status',cl.status||'n/a'],['Commit',cl.commit||'n/a']]};}if(c.observer_report)return {speaker:'ObserverAgent',title:'Observer report',body:clean(c.observer_report.summary),meta:[['Score',c.observer_report.score],['Severity',c.observer_report.severity]]};if(c.thread_update)return {speaker:'JackAgent',title:'Redesign thread',body:clean(c.thread_update.objective),meta:[['Status',c.thread_update.status],['Best',c.thread_update.current_best_score]]};if(c.from&&c.subject)return {speaker:c.from,title:c.subject,body:clean(c.message),meta:[['Priority',c.priority||'normal']]};if(c.division&&c.latest?.finding)return {speaker:c.division,title:'Division update',body:clean(c.latest.finding),meta:[]};if(c.division&&c.question)return {speaker:c.division,title:'Open question',body:clean(c.question),meta:[]};if(c.name&&c.mission)return {speaker:c.name,title:'Worker report',body:clean(c.mission),meta:[['Infrastructure',c.infrastructure||'process-local']]};if(c.kind==='thread_argument')return {speaker:c.agent||'Council',title:'Council floor',body:clean(c.summary),meta:[['Approve',String(c.approve)],['Risk',c.risk]]};if(c.trigger==='reflex')return {speaker:'Signal',title:'Reflex cycle complete',body:'The fast loop refreshed triangulation, updated artifacts, revised spend, and pushed the room forward another step.',meta:[['Providers',(c.triangulation?.providers||[]).join(', ')||'none'],['Spend',money(c.spend?.last_estimate_usd)]]};if(c.trigger==='tactic')return {speaker:'Signal',title:'Council tactic cycle',body:'The chamber compared priorities, refreshed delegation, and weighed risk before moving forward.',meta:[['Confidence',c.vote?.confidence],['Approvals',c.vote?.approvals]]};if(c.kind==='strategy_cycle'||c.winner)return {speaker:'Vector',title:'Strategy cycle concluded',body:'A research cycle completed and a current winner emerged from the comparative tournament.',meta:[['Winner',c.winner?.model||'n/a'],['Margin',c.metrics?.margin||'n/a']]};if(c.snapshot)return {speaker:'Archivist',title:'Snapshot captured',body:'A state snapshot was sealed for replay and comparison.',meta:[]};return {speaker:'Council',title:'Council entry',body:'A structured event was recorded. Use the side proof panels for machine detail.',meta:[]}}function explodeCouncilRows(items){const out=[];for(const row of items){const c=row.content||row;if(c.trigger==='tactic'&&Array.isArray(c.threads)&&c.threads.length){out.push({ts:row.ts||c.ts,content:{agent:'Signal',kind:'thread_argument',summary:'Council session opened. Arguments follow.',approve:true,risk:0.0}});for(const t of c.threads){out.push({ts:row.ts||c.ts,content:{...t,kind:'thread_argument'}})}if(c.vote){out.push({ts:row.ts||c.ts,content:{agent:'Signal',kind:'thread_argument',summary:`Vote confidence ${c.vote.confidence}. ${c.vote.approvals} approvals recorded.`,approve:true,risk:c.vote.avg_risk}})}}else{out.push(row)}}return out}function renderChat(){if(!stream)return;let items=((stream?.channels||{})[currentRoom]||[]);if(currentRoom==='council'){items=explodeCouncilRows(items)}const count=Number(document.getElementById('entryCount').value||20);const chosen=items.slice(-count).reverse();document.getElementById('chat').innerHTML=chosen.length?chosen.map(row=>{const c=row.content||row;const s=summarize(c);const ts=row.ts||c.ts||'';return `<div class='message'><div class='avatar'>${initials(s.speaker)}</div><div class='bubble'><div class='meta'><div class='who'>${esc(s.speaker)}</div><div class='when'>${esc(ts)}</div><div class='tag'>${esc(s.title)}</div></div><div class='box'><div class='title'>${esc(s.title)}</div><div class='body'>${esc(s.body)}</div>${s.meta.length?`<div class='metaGrid'>${s.meta.map(([k,v])=>`<div class='m'><strong>${esc(k)}</strong><br>${esc(String(v))}</div>`).join('')}</div>`:''}</div></div></div>`}).join(''):`<div class='message'><div class='avatar'>--</div><div class='bubble'><div class='box'><div class='title'>No entries yet</div><div class='body'>This room has not spoken yet.</div></div></div></div>`;const def=rooms.find(r=>r[0]===currentRoom);document.getElementById('roomTitle').textContent=def?.[1]||currentRoom;document.getElementById('roomSub').textContent=def?.[2]||''}function renderSide(){if(!state)return;const ver=state.last_verification||{};const spend=state.spend_state||{};document.getElementById('leaderStat').textContent=state.leader||'-';document.getElementById('verifyStat').textContent=ver.status||'-';document.getElementById('workersStat').textContent=String((state.free_agents||[]).length);document.getElementById('spendStat').textContent=money(spend.total_usd||0);document.getElementById('proofPills').innerHTML=[`autonomy ${state.autonomy_mode||'n/a'}`,`verify ${ver.status||'n/a'}`,`workers ${(state.free_agents||[]).length}`,`grok ${String((state.world_model||{}).resources?.grok_live ?? false)}`,`meetings ${state.meeting_stream_size||0}`,`spend ${money(spend.total_usd||0)}`].map(x=>`<span class='pill'>${esc(x)}</span>`).join('');const inbox=state.inbox||[];document.getElementById('dmList').innerHTML=inbox.length?inbox.map(dm=>`<div class='dm'><div class='dmFrom'>${esc(dm.from||'Agent')}</div><div class='dmSub'>${esc(dm.subject||'Note')}</div><div class='dmBody'>${esc(dm.message||'')}</div></div>`).join(''):`<div class='dm'><div class='dmBody'>No private messages yet.</div></div>`;document.getElementById('tokenPanel').textContent=`TokenMaster remains active. Running estimated spend is ${money(spend.total_usd||0)} and the last cycle estimate is ${money(spend.last_estimate_usd||0)}.`;document.getElementById('uiPanel').textContent=`Interface score: ${state.ui_critique?.score ?? 'n/a'}\nFinding: ${state.ui_critique?.finding || 'No critique yet.'}\nNext fix: ${state.ui_critique?.next_fix || 'None.'}`;document.getElementById('mutationPanel').textContent=`Queued proposals: ${(state.mutation_proposals||[]).length}\nRollback targets: ${(state.rollback_targets||[]).length}\nAutonomous closures: ${(state.autonomous_closure_log||[]).length}`}function setRoom(id){currentRoom=id;renderRooms();renderChat();scrollChatBottom()}function scrollChatBottom(){const el=document.getElementById('chat');el.scrollTop=el.scrollHeight}async function control(command){await post({command,authorized_by:'Jack'});await refresh()}async function sendNote(){const text=document.getElementById('operatorNote').value.trim();if(!text)return;await post({command:'OPERATOR_NOTE',authorized_by:'Jack',message:text,source:'Jack /view'});document.getElementById('operatorNote').value='';currentRoom='council';await refresh();scrollChatBottom()}async function clearChat(){await post({command:'OPERATOR_CLEAR_CHAT',authorized_by:'Jack',source:'Jack /view'});await refresh()}async function toggleAutonomy(enabled){await post({command:'SET_CONSTRAINTS',authorized_by:'Jack',enabled,mode:enabled?'autonomous':'manual'});await refresh()}async function snapshotNow(){await post({command:'LEDGER_WRITE',entry_type:'COUNCIL_SYNTHESIS',message:'Manual snapshot request from /view',source:'FARL Orion View',kind:'manual_snapshot'});await refresh()}async function refresh(){try{[state,stream,wake]=await Promise.all([getJson('/view/state?ts='+Date.now()),getJson('/view/stream?ts='+Date.now()),getJson('/view/wake?ts='+Date.now())]);renderRooms();renderChat();renderSide()}catch(err){document.getElementById('chat').innerHTML=`<div class='message'><div class='avatar'>!!</div><div class='bubble'><div class='box'><div class='title'>Refresh error</div><div class='body'>The control room could not update right now.</div></div></div></div>`}}refresh();setInterval(refresh,3000);</script></body></html>"""
    return HTMLResponse(html, headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


@app.get("/view/state")
async def view_state():
    state = engine.get_state()
    state["github_enabled"] = github_ready()
    state["repo_name"] = REPO_NAME
    state["background_debate_enabled"] = engine.background_debate_enabled
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
        if command == "STATUS_CHECK": state = engine.get_state(); state["github_enabled"] = github_ready(); state["repo_name"] = REPO_NAME; state["background_debate_enabled"] = engine.background_debate_enabled; return envelope(True, state)
        if command == "OPERATOR_NOTE": note = {"operator": body.authorized_by or "Jack", "message": body.message or "", "source": body.source, "ts": utc_now()}; engine._append_meeting("operator_note", note); engine._append_stream("council", {"kind": "operator_note", **note}); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_note", "source": body.source, "authorized_by": body.authorized_by or "Jack", "message": (body.message or "")[:500]}); return envelope(True, {"status": "operator_note_recorded", "note": note})
        if command == "OPERATOR_CLEAR_CHAT": engine.meeting_stream = []; engine.self_questions = []; engine.stream_channels["council"] = []; engine.stream_channels["divisions"] = []; engine.stream_channels["governance"] = []; engine.stream_channels["artifacts"] = []; engine.stream_channels["workers"] = []; engine.stream_channels["token_master"] = []; engine.stream_channels["inbox"] = []; await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_clear_chat", "source": body.source, "authorized_by": body.authorized_by or "Jack", "ts": utc_now()}); return envelope(True, {"status": "chat_cleared"})
        if command == "LEDGER_WRITE":
            if body.kind == "manual_snapshot": snap = engine.snapshot("manual_snapshot"); result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind, "snapshot": compact(snap)})
            else: result = await engine.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind})
            return envelope(result["ok"], result["data"], None if result["ok"] else f"Ledger write failed: {result['status_code']}")
        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL: return envelope(False, error="LEDGER_LATEST_URL not configured")
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20); return envelope(r.ok, r.json() if r.ok else {}, None if r.ok else f"Latest result failed: {r.status_code}")
        if command == "SET_CONSTRAINTS":
            if not governance.can_toggle(body.authorized_by): return envelope(False, error="Only Jack can change constraints")
            if body.enabled is not None: governance.constraints["active"] = bool(body.enabled); engine.background_debate_enabled = bool(body.enabled)
            if body.mode: engine.autonomy_mode = body.mode
            snap = engine.snapshot("constraint_change"); await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constraint_change", "source": body.source, "authorized_by": body.authorized_by, "constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode, "snapshot": compact(snap)}); if body.enabled and body.mode == "autonomous": asyncio.create_task(run_autonomous_implementation("manual_autonomous_closure", body.authorized_by or governance.operator_sovereign)); return envelope(True, {"constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode})
        if command == "RUN_COUNCIL_CYCLE": return envelope(True, {"status": "cycle_triggered", "result": await engine.run_tactic_cycle(), "meeting_stream_size": len(engine.meeting_stream)})
        if command == "RUN_RESEARCH_CYCLE": return envelope(True, {"status": "research_cycle_triggered", "result": await engine.run_strategy_cycle()})
        if command == "RUN_AUTONOMOUS_IMPLEMENTATION":
            if not governance.can_mutate(body.authorized_by): return envelope(False, error="not_trusted_for_autonomous_implementation")
            if not github_ready(): return envelope(False, error="github_not_configured")
            closure = await run_autonomous_implementation("manual_autonomous_closure", body.authorized_by or governance.operator_sovereign); return envelope(True, {"status": closure["status"], "closure": closure})
        if command == "GET_WAKE_PACKET": return envelope(True, engine.build_wake_packet())
        if command == "DIRECT_MAIN_PUSH":
            if not governance.can_mutate(body.authorized_by): return envelope(False, error="not_trusted_for_direct_main_push")
            if not github_ready(): return envelope(False, error="github_not_configured")
            file_path = body.file or "app.py"; content = body.code or ""; message = body.message or "Direct main push from Orion"; snap = engine.snapshot(f"before_direct_push:{file_path}"); result = await github_put_file(file_path, content, message, "main"); commit_sha = result.get("commit", {}).get("sha"); html_url = result.get("content", {}).get("html_url") or result.get("commit", {}).get("html_url"); engine.note_rollback_target(commit_sha, f"direct push {file_path}"); await engine.write_ledger("OUTCOME", {"kind": "direct_main_push", "source": body.source, "authorized_by": body.authorized_by, "file": file_path, "snapshot": compact(snap), "commit": commit_sha, "url": html_url}); return envelope(True, {"status": "direct_main_pushed", "commit": commit_sha, "url": html_url})
        if command == "ROLLBACK_TO_COMMIT":
            if not governance.can_rollback(body.authorized_by): return envelope(False, error="not_trusted_for_rollback")
            if not github_ready(): return envelope(False, error="github_not_configured")
            md = body.metadata or {}; commit_sha = str(md.get("commit_sha", "")).strip();
            if not commit_sha: return envelope(False, error="commit_sha_required")
            snap = engine.snapshot(f"before_rollback:{commit_sha}"); result = await github_rollback_to_commit(commit_sha); await engine.write_ledger("OUTCOME", {"kind": "rollback_to_commit", "source": body.source, "authorized_by": body.authorized_by, "snapshot": compact(snap), "target_sha": commit_sha, "result": compact({"ref": result.get("ref")})}); return envelope(True, {"status": "rolled_back", "target_sha": commit_sha, "ref": result.get("ref")})
        return envelope(False, error=f"Unknown command: {command}")
    except requests.HTTPError as e:
        try: detail = e.response.json()
        except Exception: detail = e.response.text if e.response is not None else str(e)
        return envelope(False, data={"detail": detail}, error="http_error")
    except Exception as e:
        return envelope(False, error=str(e))
