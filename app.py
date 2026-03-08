
import os
import asyncio
import base64
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

app = FastAPI(title="FARL Orion Control Room")
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


def cache_headers() -> Dict[str, str]:
    return {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    }


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


async def github_get_file_content(file_path: str, ref: str = "main") -> str:
    r = await asyncio.to_thread(
        requests.get,
        f"https://api.github.com/repos/{REPO_NAME}/contents/{file_path}?ref={ref}",
        headers=github_headers(),
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    content = data.get("content", "")
    encoding = data.get("encoding", "base64")
    if encoding == "base64":
        return base64.b64decode(content).decode("utf-8")
    return content


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


def compact(obj: Any, limit: int = 1800) -> str:
    text = json.dumps(obj, separators=(",", ":"), default=str)
    return text[:limit]


def safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


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
        "has_rooms": "roomList" in html or "Rooms" in html,
        "has_feed": 'id="feed"' in html or "id='feed'" in html,
        "has_operator_textarea": 'id="operatorMessage"' in html or "id='operatorMessage'" in html,
        "has_send_button": "Send to council" in html,
        "has_status_rail": 'id="statusRail"' in html or "id='statusRail'" in html,
        "has_control_title": "FARL Council Feed" in html,
    }
    score = round(sum(1 for v in checks.values() if v) / max(len(checks), 1), 3)
    return {
        "score": score,
        "checks": checks,
        "status": "healthy" if score >= 0.9 else "degraded" if score >= 0.6 else "critical",
        "summary": "Rendered control room looks present." if score >= 0.9 else "Rendered control room structure is weak.",
    }


def build_live_payload() -> Dict[str, Any]:
    state = engine.get_state()
    channels = getattr(engine, "stream_channels", {}) or {}
    meetings = getattr(engine, "meeting_stream", []) or []
    payload = {
        "summary": {
            "status": state.get("status"),
            "leader": state.get("leader"),
            "autonomy_mode": state.get("autonomy_mode"),
            "background_debate_enabled": state.get("background_debate_enabled"),
            "last_run": state.get("last_run"),
            "last_vote": state.get("last_vote") or {},
            "last_verification": state.get("last_verification") or {},
            "spend_state": state.get("spend_state") or {},
            "ui_critique": state.get("ui_critique") or {},
            "repo_name": REPO_NAME,
        },
        "queues": {
            "redesign_threads": safe_list(state.get("redesign_threads"))[:12],
            "mutation_backlog": safe_list(state.get("mutation_backlog"))[:12],
            "execution_queue": safe_list(state.get("execution_queue"))[:12],
            "rollback_targets": safe_list(state.get("rollback_targets"))[:12],
            "observer_reports": safe_list(state.get("observer_reports"))[:12],
            "inbox": safe_list(channels.get("inbox"))[-20:],
        },
        "stream": {
            "channels": {
                "council": safe_list(channels.get("council"))[-120:],
                "inbox": safe_list(channels.get("inbox"))[-80:],
                "governance": safe_list(channels.get("governance"))[-80:],
                "workers": safe_list(channels.get("workers"))[-80:],
                "deploy_sims": safe_list(channels.get("deploy_sims"))[-60:],
                "token_master": safe_list(channels.get("token_master"))[-60:],
            },
            "meetings": meetings[-120:],
            "snapshots": safe_list(getattr(engine, "snapshots", []))[-20:],
            "questions": safe_list(getattr(engine, "self_questions", []))[-40:],
            "deployment_sims": safe_list(getattr(engine, "deployment_sims", []))[-40:],
        },
        "divisions": state.get("divisions") or {},
        "free_agents": safe_list(state.get("free_agents"))[:20],
    }
    return payload


def should_attempt_autonomous_push(state: Dict[str, Any]) -> Dict[str, Any]:
    global LAST_AUTO_PUSH_TS
    now = utc_ts()
    if not github_ready():
        return {"ok": False, "reason": "github_not_ready"}
    if state.get("autonomy_mode") != "autonomous" or not state.get("background_debate_enabled", False):
        return {"ok": False, "reason": "autonomy_not_enabled"}
    last_vote = state.get("last_vote") or {}
    if not last_vote.get("passed"):
        return {"ok": False, "reason": "vote_not_passed"}
    if float(last_vote.get("confidence", 0.0)) < AUTO_PUSH_MIN_CONFIDENCE:
        return {"ok": False, "reason": "vote_confidence_too_low"}
    verification = state.get("last_verification") or {}
    if verification.get("status") == "critical":
        return {"ok": False, "reason": "verification_critical"}
    if now - LAST_AUTO_PUSH_TS < AUTO_PUSH_INTERVAL_SECONDS:
        return {"ok": False, "reason": "cadence_not_ready", "seconds_remaining": int(AUTO_PUSH_INTERVAL_SECONDS - (now - LAST_AUTO_PUSH_TS))}
    return {"ok": True, "reason": "ready"}


async def direct_push_file(file_path: str, content: str, message: str, reason: str) -> Dict[str, Any]:
    push = await github_put_file(file_path, content, message, "main")
    commit_sha = push.get("commit", {}).get("sha")
    if commit_sha:
        engine.note_rollback_target(commit_sha, reason)
    verify = engine.verify_runtime()
    rendered = await asyncio.to_thread(fetch_rendered_view)
    rendered_check = inspect_rendered_view_html(rendered.get("html", "")) if rendered.get("ok") else {"status": "critical", "score": 0.0, "summary": rendered.get("reason", "rendered_fetch_failed")}
    closure = {
        "status": "pushed",
        "file": file_path,
        "commit": commit_sha,
        "verify": verify,
        "rendered": rendered_check,
    }
    if rendered_check.get("status") == "critical" or engine.rollback_recommended(verify):
        if commit_sha:
            rollback = await github_rollback_to_commit(commit_sha)
            closure["status"] = "rolled_back"
            closure["rollback"] = {"target_sha": commit_sha, "ref": rollback.get("ref"), "reason": "rendered_or_runtime_failure"}
    return closure


async def direct_push_bundle(files: List[Dict[str, str]], message: str, reason: str) -> Dict[str, Any]:
    commits = []
    for item in files:
        path = item["path"]
        content = item["content"]
        push = await github_put_file(path, content, f"{message} [{path}]", "main")
        commit_sha = push.get("commit", {}).get("sha")
        commits.append({"path": path, "commit": commit_sha})
        if commit_sha:
            engine.note_rollback_target(commit_sha, f"{reason}:{path}")
    verify = engine.verify_runtime()
    rendered = await asyncio.to_thread(fetch_rendered_view)
    rendered_check = inspect_rendered_view_html(rendered.get("html", "")) if rendered.get("ok") else {"status": "critical", "score": 0.0, "summary": rendered.get("reason", "rendered_fetch_failed")}
    closure = {"status": "pushed", "commits": commits, "verify": verify, "rendered": rendered_check}
    if commits and (rendered_check.get("status") == "critical" or engine.rollback_recommended(verify)):
        newest = commits[-1]["commit"]
        if newest:
            rollback = await github_rollback_to_commit(newest)
            closure["status"] = "rolled_back"
            closure["rollback"] = {"target_sha": newest, "ref": rollback.get("ref"), "reason": "rendered_or_runtime_failure"}
    return closure


async def run_autonomous_implementation(source: str, authorized_by: str) -> Dict[str, Any]:
    global LAST_AUTO_PUSH_TS
    state = engine.get_state()
    gate = should_attempt_autonomous_push(state)
    engine._append_stream("governance", {"autonomy_gate": gate, "ts": utc_now()})
    if not gate.get("ok") and source != "manual_autonomous_closure":
        closure = {"ts": utc_now(), "status": "skipped", "source": source, "gate": gate}
        engine.record_autonomous_closure(closure)
        return closure

    open_threads = [t for t in state.get("redesign_threads", []) if t.get("status") != "closed"]
    target_thread = next((t for t in open_threads if len(t.get("module_targets", [])) > 1), None)
    if target_thread is None:
        target_thread = next((t for t in open_threads if "app.py" in t.get("module_targets", []) or "engine.py" in t.get("module_targets", [])), None)

    if target_thread:
        bundle = engine.build_mutation_bundle(target_thread)
        vote = governance.call_vote(
            motion=f"Execute bounded bundle for {target_thread['objective']}",
            options=["APPROVE", "REJECT"],
            agent_count=len(engine.council_agents),
            preferred="APPROVE",
        )
        closure = {"ts": utc_now(), "source": source, "target_thread": target_thread, "bundle": bundle, "vote": vote, "status": "rejected"}
        engine._append_stream("governance", {"autonomy_vote": vote, "bundle": bundle, "ts": utc_now()})
        if vote.get("winner") != "APPROVE":
            engine.record_autonomous_closure(closure)
            return closure
        if bundle.get("handoff_required"):
            packet = engine.queue_external_executor_bundle(bundle, source, authorized_by)
            closure["status"] = "queued_external"
            closure["packet"] = packet
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "external_handoff", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
            engine.record_autonomous_closure(closure)
            return closure
        # bounded in-process path: only app.py or engine.py direct file refresh via current repo contents
        files = []
        for target in bundle.get("targets", []):
            if target in {"app.py", "engine.py", "guardian.py", "generator.py"}:
                files.append({"path": target, "content": await github_get_file_content(target, "main")})
        if files:
            closure = await direct_push_bundle(files, f"Autonomous bounded bundle refresh for {target_thread['objective']}", "autonomous_bundle_refresh")
            closure["bundle"] = bundle
            closure["target_thread"] = target_thread
            LAST_AUTO_PUSH_TS = utc_ts()
            engine.record_autonomous_closure(closure)
            return closure

    verify = engine.verify_runtime()
    closure = {"ts": utc_now(), "status": "noop", "source": source, "verify": verify}
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


VIEW_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>FARL Council Feed</title>
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <style>
    :root{--bg:#060817;--bg2:#0c1331;--panel:rgba(10,16,42,.78);--panel2:rgba(15,23,58,.88);--line:rgba(123,154,255,.18);--text:#eef2ff;--muted:#9aa5d1;--bright:#9fb3ff;--good:#8ef0bf;--warn:#ffd67a;--bad:#ff9494;--radius:24px;--shadow:0 12px 40px rgba(0,0,0,.28)}
    *{box-sizing:border-box} html,body{margin:0;height:100%;background:radial-gradient(1400px 700px at 100% -10%, rgba(90,110,255,.22), transparent 60%),radial-gradient(1200px 600px at -10% 10%, rgba(20,180,255,.12), transparent 52%),linear-gradient(180deg,var(--bg),var(--bg2));color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif} body{overflow:hidden}
    .shell{display:grid;grid-template-rows:auto 1fr auto;height:100%}
    .topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line);background:linear-gradient(180deg,rgba(10,16,42,.92),rgba(10,16,42,.68));backdrop-filter:blur(14px)}
    .brand{display:flex;align-items:center;gap:14px;min-width:0}.orb{width:16px;height:16px;border-radius:999px;background:radial-gradient(circle at 30% 30%, #dce3ff 0%, #8aa0ff 35%, #3f57d6 100%);box-shadow:0 0 30px rgba(110,135,255,.7)}
    .titleWrap{display:flex;flex-direction:column;gap:3px;min-width:0}.title{font-size:26px;font-weight:800;letter-spacing:-.03em}.subtitle{font-size:13px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .toolbar{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:10px}
    .btn{appearance:none;border:1px solid rgba(130,150,255,.22);background:linear-gradient(180deg,rgba(67,86,177,.40),rgba(35,44,92,.38));color:var(--text);border-radius:18px;padding:11px 14px;font-weight:700;font-size:13px;cursor:pointer;box-shadow:inset 0 1px 0 rgba(255,255,255,.08),var(--shadow);transition:transform .12s ease,border-color .12s ease,opacity .12s ease}
    .btn:hover{transform:translateY(-1px);border-color:rgba(160,180,255,.45)} .btn:disabled{opacity:.45;cursor:default;transform:none}
    .btn.primary{background:linear-gradient(180deg,rgba(99,130,255,.55),rgba(67,86,177,.5))} .btn.ghost{background:rgba(12,20,48,.55)}
    .layout{display:grid;grid-template-columns:260px minmax(0,1fr) 320px;gap:16px;padding:16px;height:100%;min-height:0}
    .panel{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);min-height:0;overflow:hidden;backdrop-filter:blur(10px)}
    .sidebar,.rail{display:flex;flex-direction:column;min-height:0}.panelHead{padding:18px 18px 12px;border-bottom:1px solid rgba(135,158,255,.12)} .panelTitle{font-size:15px;font-weight:800;letter-spacing:.02em;text-transform:uppercase;color:var(--bright)} .panelSub{font-size:12px;color:var(--muted);margin-top:4px}
    .roomList,.threadList,.railBody,.sideBody{padding:14px;overflow:auto}
    .roomBtn,.threadBtn{width:100%;text-align:left;border:1px solid transparent;background:rgba(17,26,61,.45);color:var(--text);border-radius:18px;padding:14px;margin-bottom:10px;cursor:pointer;transition:border-color .12s ease, background .12s ease, transform .12s ease}
    .roomBtn:hover,.threadBtn:hover{transform:translateY(-1px);border-color:rgba(152,174,255,.25)} .roomBtn.active,.threadBtn.active{background:linear-gradient(180deg,rgba(80,99,198,.45),rgba(41,54,117,.38));border-color:rgba(170,188,255,.4)}
    .roomName{font-weight:800;font-size:16px}.roomMeta{display:flex;justify-content:space-between;gap:8px;margin-top:6px;font-size:12px;color:var(--muted)}
    .feedWrap{display:grid;grid-template-rows:auto 1fr;min-height:0}
    .feedHead{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px;border-bottom:1px solid rgba(135,158,255,.12)}
    .feedTitle{font-size:28px;font-weight:900;letter-spacing:-.04em}.feedMeta{display:flex;gap:10px;flex-wrap:wrap}
    .pill{padding:8px 12px;border-radius:999px;border:1px solid rgba(140,160,255,.18);background:rgba(14,22,57,.5);font-size:12px;color:var(--muted)}
    .feed{overflow:auto;padding:18px 16px 28px;display:flex;flex-direction:column;gap:14px;scroll-behavior:smooth}
    .msg{display:grid;grid-template-columns:52px minmax(0,1fr);gap:12px;align-items:start}
    .avatar{width:52px;height:52px;border-radius:18px;display:grid;place-items:center;background:linear-gradient(180deg,rgba(90,116,255,.75),rgba(51,63,139,.75));border:1px solid rgba(190,206,255,.25);font-weight:900;font-size:18px;box-shadow:var(--shadow)}
    .bubble{padding:14px 16px 15px;border-radius:22px;border:1px solid rgba(135,158,255,.14);background:linear-gradient(180deg,rgba(14,23,60,.92),rgba(10,16,42,.88));box-shadow:var(--shadow)}
    .msgTop{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.who{font-weight:900;font-size:16px}.when{font-size:12px;color:var(--muted)} .badge{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--bright);padding:5px 9px;border-radius:999px;border:1px solid rgba(140,160,255,.2);background:rgba(20,31,74,.55)}
    .body{white-space:pre-wrap;line-height:1.5;font-size:15px;margin-top:10px;color:#eef2ff}
    .composer{padding:14px 16px 16px;border-top:1px solid rgba(135,158,255,.12);background:linear-gradient(180deg,rgba(10,16,42,.66),rgba(8,12,30,.9))}
    .composeBox{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:end}
    textarea{width:100%;min-height:88px;max-height:200px;resize:vertical;border-radius:22px;border:1px solid rgba(144,166,255,.18);background:rgba(10,16,42,.92);color:var(--text);padding:16px 18px;font-size:15px;outline:none}
    .composeActions{display:flex;flex-direction:column;gap:10px}
    .statusCard,.miniCard{border:1px solid rgba(140,160,255,.14);border-radius:22px;background:rgba(11,19,48,.55);padding:14px 14px 15px;margin-bottom:12px}.statusGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.k{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.12em}.v{font-size:20px;font-weight:900;margin-top:4px}.hint{font-size:13px;color:var(--muted);line-height:1.45}.railList{display:flex;flex-direction:column;gap:10px}.railItem{padding:12px 13px;border-radius:18px;background:rgba(14,22,57,.58);border:1px solid rgba(133,154,255,.12)} .railItemTitle{font-weight:800;font-size:14px}.railItemMeta{font-size:12px;color:var(--muted);margin-top:5px;line-height:1.45}
    .toast{position:fixed;right:18px;bottom:18px;z-index:40;padding:12px 14px;border-radius:16px;background:rgba(10,16,42,.96);border:1px solid rgba(148,171,255,.24);box-shadow:var(--shadow);font-size:13px;max-width:320px;display:none}.toast.show{display:block}
    .good{color:var(--good)} .warn{color:var(--warn)} .bad{color:var(--bad)}
    @media (max-width:1180px){.layout{grid-template-columns:220px minmax(0,1fr)}.rail{display:none}}
    @media (max-width:780px){.topbar{padding:12px}.title{font-size:22px}.layout{grid-template-columns:1fr;gap:12px;padding:12px}.sidebar{order:2}.feedPanel{order:1;min-height:0}.sidebar .panelHead{display:none}.roomList{display:flex;gap:10px;overflow:auto;padding:12px}.roomBtn{min-width:190px;margin:0}.threadList{display:none}.composeBox{grid-template-columns:1fr}.composeActions{flex-direction:row;flex-wrap:wrap}}
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="brand">
        <div class="orb"></div>
        <div class="titleWrap">
          <div class="title">FARL Council Feed</div>
          <div class="subtitle" id="subtitle">Private live organism room • council, builders, deploy, verification, rollback</div>
        </div>
      </div>
      <div class="toolbar">
        <button class="btn ghost" id="snapshotBtn">Snapshot</button>
        <button class="btn ghost" id="reflexBtn">Reflex</button>
        <button class="btn ghost" id="tacticBtn">Tactic</button>
        <button class="btn ghost" id="strategyBtn">Strategy</button>
        <button class="btn primary" id="deployBtn">Deploy now</button>
        <button class="btn ghost" id="autoOnBtn">Auto ON</button>
        <button class="btn ghost" id="autoOffBtn">Auto OFF</button>
      </div>
    </div>
    <div class="layout">
      <aside class="sidebar panel">
        <div class="panelHead"><div class="panelTitle">Rooms</div><div class="panelSub">Switch the live chamber and filter by speaker thread</div></div>
        <div class="sideBody"><div class="roomList" id="roomList"></div><div class="threadList" id="threadList"></div></div>
      </aside>
      <main class="panel feedPanel feedWrap">
        <div class="feedHead">
          <div><div class="feedTitle" id="roomTitle">Council</div><div class="panelSub" id="roomSub">The chamber answers, argues, plans, and acts.</div></div>
          <div class="feedMeta"><div class="pill" id="livePill">Live polling</div><div class="pill" id="statusPill">Status unknown</div><div class="pill" id="verifyPill">Verify unknown</div></div>
        </div>
        <div class="feed" id="feed"></div>
      </main>
      <aside class="rail panel">
        <div class="panelHead"><div class="panelTitle">Status rail</div><div class="panelSub">Compact proof, queues, spend, deploy pressure</div></div>
        <div class="railBody" id="statusRail"></div>
      </aside>
    </div>
    <div class="composer">
      <div class="composeBox">
        <textarea id="operatorMessage" placeholder="Message the council. They will answer here, in-room."></textarea>
        <div class="composeActions">
          <button class="btn primary" id="sendBtn">Send to council</button>
          <button class="btn ghost" id="pauseBtn">Pause scroll</button>
        </div>
      </div>
    </div>
  </div>
  <div class="toast" id="toast"></div>
  <script>
    const ROOM_DEFS = {
      council:{label:"Council",desc:"Live debate, operator replies, chamber floor."},
      inbox:{label:"Inbox / DM",desc:"Direct replies, prompts, and private agent notes."},
      builder:{label:"Builder",desc:"Mutation bundles, files, threads, backlog."},
      deploy:{label:"Deploy",desc:"Verification, rollback anchors, deploy pressure."},
      workers:{label:"Workers",desc:"Observer, builder, deploy, token, audit worker activity."}
    };
    const state={room:"council",thread:"all",paused:false,payload:null,bottomPinned:true};
    const feedEl=document.getElementById("feed"), roomListEl=document.getElementById("roomList"), threadListEl=document.getElementById("threadList"), statusRailEl=document.getElementById("statusRail");
    const toastEl=document.getElementById("toast"), roomTitleEl=document.getElementById("roomTitle"), roomSubEl=document.getElementById("roomSub"), subtitleEl=document.getElementById("subtitle"), livePillEl=document.getElementById("livePill"), statusPillEl=document.getElementById("statusPill"), verifyPillEl=document.getElementById("verifyPill"), operatorMessageEl=document.getElementById("operatorMessage");
    const byId=(id)=>document.getElementById(id), safe=(x,f="")=>x===null||x===undefined?f:x;
    const fmtTime=(v)=>{try{return new Date(v).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})}catch{return safe(v,"")}};
    const initials=(name)=>safe(name,"?").split(/[\s_-]+/).slice(0,2).map(x=>x[0]||"").join("").toUpperCase()||"?";
    function toast(text){toastEl.textContent=text;toastEl.classList.add("show");clearTimeout(window.__toastTimer);window.__toastTimer=setTimeout(()=>toastEl.classList.remove("show"),2600);}
    function esc(text){return safe(text).replace(/\r\n/g,"\n").trim();}
    feedEl.addEventListener("scroll",()=>{const delta=feedEl.scrollHeight-feedEl.scrollTop-feedEl.clientHeight;state.bottomPinned=delta<80;});
    async function api(command, extra={}){const res=await fetch("/view/control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({command,authorized_by:"Jack",source:"/view",...extra})});const data=await res.json();if(!data.ok) throw new Error(data.error||"command_failed");return data.data||{};}
    function getRoomMessages(payload, room){
      if(!payload) return [];
      const channels=(payload.stream||{}).channels||{}, meetings=(payload.stream||{}).meetings||[], queues=payload.queues||{};
      const wrap=(author,title,body,ts,kind="note")=>({author,title,body,ts,kind});
      if(room==="council"){
        const a=meetings.map(m=>{const c=m.content||{}; if(m.kind==="operator_note") return wrap("Jack","Operator",c.message||"Operator input",m.ts,m.kind); if(m.kind==="tactic") return wrap("Signal","Council tactic cycle","The chamber compared priorities, refreshed delegation, and weighed risk before moving.",m.ts,m.kind); if(m.kind==="strategy") return wrap("Vector","Strategy cycle concluded","A research cycle completed and a current winner emerged from the comparative tournament.",m.ts,m.kind); if(m.kind==="reflex") return wrap("Reflex","Reflex cycle","The organism re-sensed the field, refreshed opportunities, and updated the short horizon.",m.ts,m.kind); if(m.kind==="constitution") return wrap("Guardian","Constitution cycle","A state snapshot was sealed for replay and comparison.",m.ts,m.kind); return null;}).filter(Boolean);
        const b=(channels.council||[]).map(item=>{const c=item.content||{}; return wrap(c.agent||c.from||c.kind||"Council",c.title||c.subject||c.kind||"Council floor",c.summary||c.message||c.finding||c.note||"",item.ts,c.kind||"council");}).filter(x=>x.body||x.title);
        return [...a,...b];
      }
      if(room==="inbox"){
        const a=(channels.inbox||[]).map(item=>{const c=item.content||{}; return wrap(c.from||"Inbox",c.subject||"Inbox",c.message||"",item.ts,"inbox");});
        return a;
      }
      if(room==="builder"){
        const threads=(queues.redesign_threads||[]).map(t=>wrap("BuilderAgent","Redesign thread",`${safe(t.objective,'thread')}\nTargets: ${(t.module_targets||[]).join(', ')||'none'}\nScore ${Number(t.current_best_score||0).toFixed(3)} / ${Number(t.target_score||0).toFixed(3)}`,t.opened_at||new Date().toISOString(),"thread"));
        const backlog=(queues.mutation_backlog||[]).map(b=>wrap("BuilderAgent","Mutation bundle",`${safe(b.objective,b.bundle_id)}\nTargets: ${(b.targets||[]).join(', ')}\nExecutor: ${safe(b.executor,'unknown')}`,b.ts||new Date().toISOString(),"bundle"));
        const gov=(channels.governance||[]).map(item=>{const c=item.content||{}; if(c.bundle_status) return wrap("DeployAgent","Bundle status",`${c.bundle_status.bundle_id}\n${c.bundle_status.status}`,item.ts,"bundle_status"); if(c.thread_progress) return wrap("ObserverAgent","Thread progress",`${c.thread_progress.objective}\n${c.thread_progress.current_best_score}`,item.ts,"thread_progress"); if(c.external_handoff_packet) return wrap("ExternalHandoff","External packet",`${c.external_handoff_packet.objective}\nTargets: ${(c.external_handoff_packet.targets||[]).join(', ')}`,item.ts,"handoff"); return null;}).filter(Boolean);
        return [...threads,...backlog,...gov];
      }
      if(room==="deploy"){
        const verify=payload.summary?.last_verification||{};
        const a=[wrap("Guardian","Verification",`Status: ${safe(verify.status,'unknown')}\nScore: ${Number(verify.score||0).toFixed(3)}`,verify.ts||new Date().toISOString(),"verify")];
        const b=(queues.rollback_targets||[]).map(r=>wrap("Guardian","Rollback target",`${safe(r.reason,'rollback_anchor')}\n${safe(r.commit_sha,'unknown')}`,r.ts||new Date().toISOString(),"rollback_target"));
        const c=(channels.deploy_sims||[]).map(item=>{const x=item.content||{}; return wrap("DeployAgent","Deploy simulation",x.note||x.message||"Simulation ready",item.ts,"deploy_sim");});
        return [...a,...b,...c];
      }
      if(room==="workers"){
        const a=(payload.free_agents||[]).map(w=>wrap(w.name||"Worker","Worker",`${safe(w.mission,'Worker update')}\nStatus: ${safe(w.status,'unknown')}`,w.last_action||new Date().toISOString(),"worker"));
        const b=(channels.workers||[]).map(item=>{const c=item.content||{}; return wrap(c.name||"Worker","Worker channel",c.mission||c.message||c.task||"Worker update",item.ts,"worker");});
        return [...a,...b];
      }
      return [];
    }
    function uniqueThreads(messages){const names=["all"], seen=new Set(); messages.forEach(m=>{const k=m.author||m.kind||"all"; if(!seen.has(k)){seen.add(k); names.push(k);}}); return names;}
    function renderRooms(){roomListEl.innerHTML=""; Object.entries(ROOM_DEFS).forEach(([key,def])=>{const count=getRoomMessages(state.payload,key).length; const btn=document.createElement("button"); btn.className="roomBtn"+(state.room===key?" active":""); btn.innerHTML=`<div class="roomName">${def.label}</div><div class="roomMeta"><span>${def.desc}</span><span>${count}</span></div>`; btn.onclick=()=>{state.room=key; state.thread="all"; renderAll();}; roomListEl.appendChild(btn);});}
    function renderThreads(){const messages=getRoomMessages(state.payload,state.room); threadListEl.innerHTML=""; uniqueThreads(messages).forEach(name=>{const btn=document.createElement("button"); btn.className="threadBtn"+(state.thread===name?" active":""); btn.textContent=name==="all"?"All speakers":name; btn.onclick=()=>{state.thread=name; renderFeed();}; threadListEl.appendChild(btn);});}
    function renderFeed(){const messages=getRoomMessages(state.payload,state.room).filter(m=>state.thread==="all"?true:m.author===state.thread).sort((a,b)=>new Date(a.ts)-new Date(b.ts)); feedEl.innerHTML=""; if(!messages.length){const empty=document.createElement("div"); empty.className="miniCard"; empty.innerHTML=`<div class="railItemTitle">Room quiet</div><div class="railItemMeta">No visible events are in this room yet. Use the composer or trigger a cycle.</div>`; feedEl.appendChild(empty); return;} messages.forEach(m=>{const row=document.createElement("div"); row.className="msg"; row.innerHTML=`<div class="avatar">${initials(m.author)}</div><div class="bubble"><div class="msgTop"><div class="who">${m.author}</div><div class="when">${fmtTime(m.ts)}</div><div class="badge">${m.title}</div></div><div class="body"></div></div>`; row.querySelector(".body").textContent=esc(m.body||m.title); feedEl.appendChild(row);}); if(!state.paused && state.bottomPinned){requestAnimationFrame(()=>{feedEl.scrollTop=feedEl.scrollHeight;});}}
    function renderRail(){const summary=state.payload?.summary||{}, queues=state.payload?.queues||{}, spend=summary.spend_state||{}, verify=summary.last_verification||{}, critique=summary.ui_critique||{}, vote=summary.last_vote||{}; statusRailEl.innerHTML=`
      <div class="statusCard"><div class="panelTitle" style="margin-bottom:12px">Core state</div><div class="statusGrid"><div class="miniCard"><div class="k">Leader</div><div class="v">${safe(summary.leader,'unknown')}</div></div><div class="miniCard"><div class="k">Verify</div><div class="v ${verify.status==='healthy'?'good':verify.status==='critical'?'bad':'warn'}">${safe(verify.status,'unknown')}</div></div><div class="miniCard"><div class="k">Spend total</div><div class="v">$${Number(spend.total_usd||0).toFixed(4)}</div></div><div class="miniCard"><div class="k">Vote confidence</div><div class="v">${Number(vote.confidence||0).toFixed(3)}</div></div></div><div class="hint" style="margin-top:10px">${safe(critique.finding,'No UI critique yet.')}</div></div>
      <div class="statusCard"><div class="panelTitle" style="margin-bottom:10px">Open threads</div><div class="railList">${(queues.redesign_threads||[]).slice(0,4).map(t=>`<div class="railItem"><div class="railItemTitle">${safe(t.objective,'thread')}</div><div class="railItemMeta">Targets ${(t.module_targets||[]).join(', ')||'none'} • score ${Number(t.current_best_score||0).toFixed(3)} / ${Number(t.target_score||0).toFixed(3)}</div></div>`).join('')||`<div class="railItem"><div class="railItemTitle">No open threads</div><div class="railItemMeta">The redesign queue is clear.</div></div>`}</div></div>
      <div class="statusCard"><div class="panelTitle" style="margin-bottom:10px">Builder / deploy queue</div><div class="railList">${(queues.mutation_backlog||[]).slice(0,3).map(b=>`<div class="railItem"><div class="railItemTitle">${safe(b.objective,b.bundle_id)}</div><div class="railItemMeta">Bundle ${safe(b.bundle_id,'')} • ${(b.targets||[]).join(', ')} • ${safe(b.executor,'unknown')}</div></div>`).join('')}${(queues.execution_queue||[]).slice(0,3).map(q=>`<div class="railItem"><div class="railItemTitle">${safe(q.objective,q.packet_id)}</div><div class="railItemMeta">Execution packet • ${(q.targets||[]).join(', ')} • ${safe(q.status,'queued')}</div></div>`).join('')||`<div class="railItem"><div class="railItemTitle">No external handoff queued</div><div class="railItemMeta">All current work fits the current in-process builder scope.</div></div>`}</div></div>`;
    }
    function renderHeaders(){const def=ROOM_DEFS[state.room], summary=state.payload?.summary||{}; roomTitleEl.textContent=def.label; roomSubEl.textContent=def.desc; subtitleEl.textContent=`Private live organism room • last run ${summary.last_run?fmtTime(summary.last_run):'waiting'} • mode ${safe(summary.autonomy_mode,'unknown')} • leader ${safe(summary.leader,'unknown')}`; livePillEl.textContent=state.paused?'Polling paused':'Live polling every 3s'; statusPillEl.textContent=`Mode ${safe(summary.autonomy_mode,'unknown')} • leader ${safe(summary.leader,'unknown')}`; verifyPillEl.textContent=`Verify ${safe((summary.last_verification||{}).status,'unknown')} • spend $${Number((summary.spend_state||{}).total_usd||0).toFixed(4)}`;}
    function renderAll(){if(!state.payload) return; renderHeaders(); renderRooms(); renderThreads(); renderFeed(); renderRail();}
    async function refresh(){if(state.paused) return; try{const res=await fetch(`/view/live?ts=${Date.now()}`,{cache:'no-store'}); state.payload=await res.json(); renderAll();}catch(err){toast(`Refresh failed: ${err.message}`);}}
    byId("sendBtn").onclick=async()=>{const message=operatorMessageEl.value.trim(); if(!message) return; byId("sendBtn").disabled=true; try{await api("OPERATOR_NOTE",{message}); operatorMessageEl.value=""; toast("Council note sent"); await refresh();}catch(err){toast(`Send failed: ${err.message}`);}finally{byId("sendBtn").disabled=false;}};
    byId("pauseBtn").onclick=()=>{state.paused=!state.paused; byId("pauseBtn").textContent=state.paused?"Resume scroll":"Pause scroll"; renderHeaders();};
    byId("snapshotBtn").onclick=async()=>{try{await api("CREATE_SNAPSHOT"); toast("Snapshot captured"); await refresh();}catch(err){toast(`Snapshot failed: ${err.message}`);}};
    byId("reflexBtn").onclick=async()=>{try{await api("RUN_REFLEX_CYCLE"); toast("Reflex cycle ran"); await refresh();}catch(err){toast(`Reflex failed: ${err.message}`);}};
    byId("tacticBtn").onclick=async()=>{try{await api("RUN_TACTIC_CYCLE"); toast("Tactic cycle ran"); await refresh();}catch(err){toast(`Tactic failed: ${err.message}`);}};
    byId("strategyBtn").onclick=async()=>{try{await api("RUN_STRATEGY_CYCLE"); toast("Strategy cycle ran"); await refresh();}catch(err){toast(`Strategy failed: ${err.message}`);}};
    byId("deployBtn").onclick=async()=>{try{await api("RUN_AUTONOMOUS_IMPLEMENTATION"); toast("Deploy closure triggered"); await refresh();}catch(err){toast(`Deploy failed: ${err.message}`);}};
    byId("autoOnBtn").onclick=async()=>{try{await api("SET_CONSTRAINTS",{enabled:true,mode:'autonomous'}); toast("Autonomy ON"); await refresh();}catch(err){toast(`Autonomy ON failed: ${err.message}`);}};
    byId("autoOffBtn").onclick=async()=>{try{await api("SET_CONSTRAINTS",{enabled:false,mode:'manual'}); toast("Autonomy OFF"); await refresh();}catch(err){toast(`Autonomy OFF failed: ${err.message}`);}};
    operatorMessageEl.addEventListener("keydown",(e)=>{if(e.key==="Enter" && !e.shiftKey){e.preventDefault(); byId("sendBtn").click();}});
    refresh(); setInterval(refresh,3000);
  </script>
</body>
</html>"""


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start())
    asyncio.create_task(autonomous_operator_loop())


@app.get("/health")
async def health():
    return JSONResponse({"ok": True, "status": "healthy", "service": "orion"}, headers=cache_headers())


@app.get("/view")
async def view_dashboard():
    return HTMLResponse(VIEW_HTML, headers=cache_headers())


@app.get("/view/live")
async def view_live():
    return JSONResponse(build_live_payload(), headers=cache_headers())


@app.get("/view/state")
async def view_state():
    state = engine.get_state()
    state["github_enabled"] = github_ready()
    state["repo_name"] = REPO_NAME
    state["background_debate_enabled"] = engine.background_debate_enabled
    return JSONResponse(state, headers=cache_headers())


@app.get("/view/stream")
async def view_stream():
    return JSONResponse(
        {
            "channels": getattr(engine, "stream_channels", {}) or {},
            "meetings": safe_list(getattr(engine, "meeting_stream", []))[-120:],
            "questions": safe_list(getattr(engine, "self_questions", []))[-80:],
            "snapshots": safe_list(getattr(engine, "snapshots", []))[-40:],
            "deployment_sims": safe_list(getattr(engine, "deployment_sims", []))[-40:],
        },
        headers=cache_headers(),
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
            {"ok": ok, "command": command, "request_id": request_id, "timestamp_utc": now, "data": data or {}, "error": error},
            headers=cache_headers(),
        )

    try:
        if command == "HEALTH_CHECK":
            return envelope(True, {"status": "healthy", "service": "orion"})
        if command == "STATUS_CHECK":
            state = engine.get_state()
            state["github_enabled"] = github_ready()
            state["repo_name"] = REPO_NAME
            state["background_debate_enabled"] = engine.background_debate_enabled
            return envelope(True, state)
        if command == "OPERATOR_NOTE":
            result = engine.process_operator_note(body.message or "", body.source or "FARL Council Node", body.authorized_by or "Jack")
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_note", "source": body.source, "authorized_by": body.authorized_by or "Jack", "message": (body.message or "")[:500], "objectives": result.get("objectives", [])})
            try:
                await engine.run_tactic_cycle()
            except Exception:
                pass
            return envelope(True, {"status": "operator_note_recorded", "result": result})
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
            return envelope(True, {"constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode, "snapshot": snap})
        if command == "RUN_REFLEX_CYCLE":
            result = await engine.run_reflex_cycle()
            return envelope(True, {"status": "ok", "result": result})
        if command == "RUN_TACTIC_CYCLE":
            result = await engine.run_tactic_cycle()
            return envelope(True, {"status": "ok", "result": result})
        if command == "RUN_STRATEGY_CYCLE":
            result = await engine.run_strategy_cycle()
            return envelope(True, {"status": "ok", "result": result})
        if command == "RUN_CONSTITUTION_CYCLE":
            result = await engine.run_constitution_cycle()
            return envelope(True, {"status": "ok", "result": result})
        if command == "CREATE_SNAPSHOT":
            snap = engine.snapshot("manual_snapshot")
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "manual_snapshot", "source": body.source, "authorized_by": body.authorized_by or "Jack", "snapshot": compact(snap)})
            return envelope(True, {"status": "snapshot_created", "snapshot": snap})
        if command == "DIRECT_MAIN_PUSH":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_direct_push")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            if not body.file or body.code is None:
                return envelope(False, error="file_and_code_required")
            closure = await direct_push_file(body.file, body.code, body.message or f"Direct push {body.file}", "direct_main_push")
            await engine.write_ledger("OUTCOME", {"kind": "direct_main_push", "source": body.source, "authorized_by": body.authorized_by, "file": body.file, "closure": compact(closure)})
            return envelope(True, {"status": closure["status"], "closure": closure})
        if command == "DIRECT_PUSH_BUNDLE":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_direct_push")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            files = (body.metadata or {}).get("files", [])
            if not isinstance(files, list) or not files:
                return envelope(False, error="metadata.files_required")
            norm = []
            for item in files:
                if not isinstance(item, dict) or "path" not in item or "content" not in item:
                    return envelope(False, error="invalid_bundle_item")
                norm.append({"path": item["path"], "content": item["content"]})
            closure = await direct_push_bundle(norm, body.message or "Direct push bundle", "direct_push_bundle")
            await engine.write_ledger("OUTCOME", {"kind": "direct_push_bundle", "source": body.source, "authorized_by": body.authorized_by, "closure": compact(closure)})
            return envelope(True, {"status": closure["status"], "closure": closure})
        if command == "RUN_AUTONOMOUS_IMPLEMENTATION":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_autonomous_implementation")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            closure = await run_autonomous_implementation("manual_autonomous_closure", body.authorized_by or governance.operator_sovereign)
            return envelope(True, {"status": closure["status"], "closure": closure})
        if command == "ROLLBACK_TO_COMMIT":
            if not governance.can_rollback(body.authorized_by):
                return envelope(False, error="not_trusted_for_rollback")
            sha = (body.metadata or {}).get("commit_sha") or body.message
            if not sha:
                return envelope(False, error="commit_sha_required")
            result = await github_rollback_to_commit(sha)
            return envelope(True, {"status": "rolled_back", "result": result})
        if command == "ELECT_LEADER":
            leader = governance.elect_leader()
            return envelope(True, {"status": "leader_elected", "leader": leader})
        return envelope(False, error=f"Unknown command: {command}")
    except requests.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text if e.response is not None else str(e)
        return envelope(False, data={"detail": detail}, error="http_error")
    except Exception as e:
        return envelope(False, error=str(e))
