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
    replacement = current_app
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
    push = await github_put_file("app.py", replacement, "Autonomous /view rebuild with rendered verification", "main")
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


async def run_multifile_bundle_closure(source: str, authorized_by: str) -> Dict[str, Any]:
    global LAST_AUTO_PUSH_TS
    state = engine.get_state()
    gate = should_attempt_autonomous_push(state)
    engine._append_stream("governance", {"autonomy_gate": gate, "ts": utc_now()})
    if not gate.get("ok") and source != "manual_autonomous_closure":
        closure = {"ts": utc_now(), "status": "skipped", "source": source, "gate": gate, "kind": "multifile_bundle"}
        engine.record_autonomous_closure(closure)
        return closure
    open_threads = [t for t in state.get("redesign_threads", []) if t.get("status") != "closed"]
    target_thread = next((t for t in open_threads if len(t.get("module_targets", [])) > 1), None)
    if target_thread is None:
        target_thread = next((t for t in open_threads if "engine.py" in t.get("module_targets", []) or "app.py" in t.get("module_targets", [])), None)
    if target_thread is None:
        closure = {"ts": utc_now(), "status": "skipped", "source": source, "gate": gate, "kind": "multifile_bundle", "reason": "no_open_multifile_thread"}
        engine.record_autonomous_closure(closure)
        return closure
    bundle = engine.build_mutation_bundle(target_thread)
    vote = governance.call_vote(
        motion=f"Execute bounded multi-file bundle for {target_thread['objective']}",
        options=["APPROVE", "REJECT"],
        agent_count=len(engine.council_agents),
        preferred="APPROVE",
    )
    engine._append_stream("governance", {"autonomy_vote": vote, "bundle": bundle, "ts": utc_now()})
    closure = {"ts": utc_now(), "status": "rejected", "source": source, "kind": "multifile_bundle", "gate": gate, "bundle": bundle, "vote": vote}
    if vote.get("winner") != "APPROVE":
        engine.record_autonomous_closure(closure)
        return closure
    if bundle.get("handoff_required"):
        packet = engine.queue_external_executor_bundle(bundle, source, authorized_by)
        closure.update({"status": "queued_external", "packet": packet})
        await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "bounded_multifile_external_handoff", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
        engine.record_autonomous_closure(closure)
        return closure
    current_engine = await github_get_file_content("engine.py", "main")
    tuned_engine = apply_self_tuning_patch(current_engine, {"proposal_limit": min(10, max(3, len(state.get("mutation_backlog", [])) + 1))})
    current_app = await github_get_file_content("app.py", "main")
    rebuilt_app = current_app
    snap = engine.snapshot("before_multifile_bundle")
    engine_push = await github_put_file("engine.py", tuned_engine, f"Bounded multi-file bundle: engine update for {target_thread['objective']}", "main")
    engine_commit = engine_push.get("commit", {}).get("sha")
    engine.note_rollback_target(engine_commit, f"multifile_bundle_engine:{target_thread['objective']}")
    app_push = await github_put_file("app.py", rebuilt_app, f"Bounded multi-file bundle: app view update for {target_thread['objective']}", "main")
    app_commit = app_push.get("commit", {}).get("sha")
    engine.note_rollback_target(app_commit, f"multifile_bundle_app:{target_thread['objective']}")
    await asyncio.sleep(6)
    verify = engine.verify_runtime()
    rendered = await asyncio.to_thread(fetch_rendered_view)
    rendered_check = inspect_rendered_view_html(rendered.get("html", "")) if rendered.get("ok") else {"status": "critical", "score": 0.0, "summary": rendered.get("reason", "post_fetch_failed")}
    closure.update({
        "status": "pushed",
        "snapshot": snap,
        "engine_commit": engine_commit,
        "app_commit": app_commit,
        "verify": verify,
        "rendered": rendered_check,
    })
    LAST_AUTO_PUSH_TS = utc_ts()
    engine.update_thread_progress(target_thread["objective"], rendered_check.get("score", 0.0), "multifile_bundle_pushed", bundle)
    engine.mark_bundle_status(bundle["bundle_id"], "pushed", {"engine_commit": engine_commit, "app_commit": app_commit})
    engine._append_stream("governance", {"autonomy_push": {"commit": app_commit, "verify": verify, "rendered": rendered_check, "bundle": bundle}, "ts": utc_now()})
    if engine.rollback_recommended(verify) or rendered_check.get("status") == "critical":
        rollback = await github_rollback_to_commit(app_commit)
        closure["status"] = "rolled_back"
        closure["rollback"] = {"target_sha": app_commit, "ref": rollback.get("ref"), "reason": "critical_multifile_verification"}
        engine.record_regression("multifile_bundle", "Rendered verification or runtime verification degraded after bundle push.", snap, {"verify": verify, "rendered": rendered_check})
        engine._append_stream("governance", {"autonomy_rollback": closure["rollback"], "ts": utc_now()})
    await engine.write_ledger("OUTCOME", {"kind": "bounded_multifile_bundle", "source": source, "authorized_by": authorized_by, "closure": compact(closure)})
    engine.record_autonomous_closure(closure)
    return closure


async def run_autonomous_implementation(source: str, authorized_by: str) -> Dict[str, Any]:
    state = engine.get_state()
    open_threads = state.get("redesign_threads") or []
    if any(len(t.get("module_targets", [])) > 1 for t in open_threads):
        return await run_multifile_bundle_closure(source, authorized_by)
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
    return HTMLResponse("<html><body><meta http-equiv='Cache-Control' content='no-cache, no-store, must-revalidate'><meta http-equiv='Pragma' content='no-cache'><meta http-equiv='Expires' content='0'><script>location.href='/view/state'</script></body></html>", headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})


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
            if body.enabled and body.mode == "autonomous":
                asyncio.create_task(run_autonomous_implementation("manual_autonomous_closure", body.authorized_by or governance.operator_sovereign))
            return envelope(True, {"constraints_active": governance.constraints["active"], "background_debate_enabled": engine.background_debate_enabled, "autonomy_mode": engine.autonomy_mode})
        if command == "RUN_AUTONOMOUS_IMPLEMENTATION":
            if not governance.can_mutate(body.authorized_by):
                return envelope(False, error="not_trusted_for_autonomous_implementation")
            if not github_ready():
                return envelope(False, error="github_not_configured")
            closure = await run_autonomous_implementation("manual_autonomous_closure", body.authorized_by or governance.operator_sovereign)
            return envelope(True, {"status": closure["status"], "closure": closure})
        return envelope(False, error=f"Unknown command: {command}")
    except requests.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text if e.response is not None else str(e)
        return envelope(False, data={"detail": detail}, error="http_error")
    except Exception as e:
        return envelope(False, error=str(e))
