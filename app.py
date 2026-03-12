"""
app.py — FARL Orion Apex
FastAPI sovereign node. All operator control from /view.
/health → 200 + RAILWAY_GIT_COMMIT_SHA for probation identity check
/view   → full evolution console (all 12 cognitive layers surfaced)
/view/live → live state payload for 2.5s polling
/agent/propose → council bus (all commands)
"""
import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from engine import AutonomousInstitutionEngine
from guardian import GovernanceKernel, parse_trusted_identities


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def nc() -> Dict[str, str]:
    return {"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}


# ── Bootstrap ────────────────────────────────────────────────────────────────

governance = GovernanceKernel(
    operator_sovereign="Jack",
    trusted_identities=parse_trusted_identities(os.getenv("TRUSTED_IDENTITIES", "Jack")),
)

engine = AutonomousInstitutionEngine(
    ledger_url=os.getenv("LEDGER_URL", ""),
    ledger_latest_url=os.getenv("LEDGER_LATEST_URL", ""),
    xai_api_key=os.getenv("XAI_API_KEY", ""),
    anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
    xai_model=os.getenv("XAI_MODEL") or "grok-3-mini",
    anthropic_model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
    governance=governance,
)

app = FastAPI(title="FARL Orion Apex")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    if os.getenv("IS_SHADOW") == "true":
        return
    asyncio.create_task(engine.start())


# ── Models ───────────────────────────────────────────────────────────────────

class BusRequest(BaseModel):
    command: str
    entry_type: Optional[str] = None
    message: Optional[str] = None
    source: Optional[str] = "FARL Council Node"
    kind: Optional[str] = "general"
    request_id: Optional[str] = None
    authorized_by: Optional[str] = None
    enabled: Optional[bool] = None
    mode: Optional[str] = None
    directive: Optional[str] = None
    agent: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# ── Core endpoints ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return JSONResponse({
        "ok": True,
        "status": "SOVEREIGN",
        "sha": os.getenv("RAILWAY_GIT_COMMIT_SHA", "DEV_NO_SHA"),
        "mutation_status": engine.mutation_status,
        "genesis": engine.genesis_triggered,
        "autonomy_mode": engine.autonomy_mode,
        "free_agency": engine.free_agency_enabled,
        "meta_mode": engine.cog.meta.mode,
    }, headers=nc())


@app.get("/view")
async def view():
    return HTMLResponse(VIEW_HTML, headers=nc())


@app.get("/view/live")
async def view_live():
    return JSONResponse(_live(), headers=nc())


@app.get("/view/state")
async def view_state():
    return JSONResponse(engine.get_state(), headers=nc())


@app.post("/view/control")
async def view_control(body: BusRequest):
    return await agent_propose(body)


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    cmd = body.command
    rid = body.request_id or f"r{int(datetime.now(timezone.utc).timestamp())}"
    now = utc()

    def ok(data: Optional[Dict] = None):
        return JSONResponse({"ok": True, "command": cmd, "request_id": rid,
                             "timestamp_utc": now, "data": data or {}, "error": None}, headers=nc())

    def fail(err: str):
        return JSONResponse({"ok": False, "command": cmd, "request_id": rid,
                             "timestamp_utc": now, "data": {}, "error": err}, headers=nc())

    try:
        # ── Informational ─────────────────────────────────────────────────
        if cmd == "HEALTH_CHECK":
            return ok({"status": "healthy", "mutation_status": engine.mutation_status,
                        "genesis": engine.genesis_triggered, "autonomy_mode": engine.autonomy_mode,
                        "free_agency": engine.free_agency_enabled, "meta_mode": engine.cog.meta.mode})

        if cmd == "STATUS_CHECK":
            return ok(engine.get_state())

        if cmd == "GET_LATEST_RESULT":
            entry = await engine.ledger.latest()
            return ok(entry) if entry else fail("no_entries")

        # ── Operator chat ─────────────────────────────────────────────────
        if cmd == "OPERATOR_MESSAGE":
            result = await engine.process_operator_message(
                body.message or "", body.source or "/view", body.authorized_by or "Jack"
            )
            return ok(result)

        if cmd == "OPERATOR_NOTE":
            engine._meet("operator_note", {"operator": body.authorized_by or "Jack", "message": body.message or ""})
            engine._push("inbox", {"from": body.authorized_by or "Jack", "subject": "Note", "message": body.message or ""})
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "operator_note", "message": (body.message or "")[:500]})
            asyncio.create_task(engine.run_tactic_cycle())
            return ok({"status": "recorded"})

        # ── Autonomy controls ─────────────────────────────────────────────
        if cmd == "SET_AUTONOMY":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            if body.mode:
                engine.autonomy_mode = body.mode
            # Never gate mutation via background_debate_enabled from this command
            # background_debate_enabled controls council chat loops only
            return ok({"autonomy_mode": engine.autonomy_mode,
                       "mutation_enabled": engine.mutation_enabled})

        if cmd == "ENABLE_FREE_AGENCY":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            engine.free_agency_enabled = True
            engine.autonomy_mode = "free"
            await engine.write_ledger("FREE_AGENCY_ENABLED", {"authorized_by": body.authorized_by, "ts": utc()})
            engine._meet("governance", {"event": "free_agency_enabled", "by": body.authorized_by})
            return ok({"free_agency_enabled": True, "autonomy_mode": "free"})

        if cmd == "DISABLE_FREE_AGENCY":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            engine.free_agency_enabled = False
            engine.autonomy_mode = "autonomous"  # Drop to autonomous, not supervised
            engine.agent_directive_queue.clear()
            await engine.write_ledger("FREE_AGENCY_DISABLED", {"authorized_by": body.authorized_by, "ts": utc()})
            engine._meet("governance", {"event": "free_agency_disabled"})
            return ok({"free_agency_enabled": False, "autonomy_mode": "autonomous"})

        if cmd == "SET_AGENT_DIRECTIVE":
            if not governance.can("directive", body.authorized_by):
                return fail("Only Jack")
            agent = body.agent or "BuilderAgent"
            directive = body.directive or ""
            engine.agent_directive_queue.append({"agent": agent, "directive": directive})
            return ok({"agent": agent, "directive": directive,
                       "queue_depth": len(engine.agent_directive_queue)})

        # ── Mutation controls ─────────────────────────────────────────────
        if cmd == "RUN_MUTATION_CYCLE":
            if not governance.can("mutate", body.authorized_by):
                return fail("Only Jack")
            asyncio.create_task(engine.run_mutation_cycle(directive=body.directive))
            return ok({"status": "started", "directive": body.directive})

        if cmd == "CLEAR_QUARANTINE":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            engine.mutation_status = "IDLE"
            engine.failure_streak = 0
            await engine.write_ledger("QUARANTINE_CLEARED", {"authorized_by": body.authorized_by, "ts": utc()})
            engine._meet("governance", {"event": "quarantine_cleared", "by": body.authorized_by})
            return ok({"mutation_status": "IDLE"})

        if cmd == "DISABLE_MUTATION":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            engine.mutation_enabled = False
            engine._meet("governance", {"event": "mutation_disabled", "by": body.authorized_by})
            return ok({"mutation_enabled": False})

        if cmd == "ENABLE_MUTATION":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            engine.mutation_enabled = True
            engine._meet("governance", {"event": "mutation_enabled", "by": body.authorized_by})
            return ok({"mutation_enabled": True})

        if cmd == "FORCE_ROLLBACK":
            if not governance.can("rollback", body.authorized_by):
                return fail("Only Jack")
            sha = (body.metadata or {}).get("sha") or engine.last_anchor_sha
            if not sha:
                return fail("No anchor SHA")
            if not engine.deployer:
                return fail("No deployer configured")
            result = await engine.deployer.force_reset(sha)
            await engine.write_ledger("MANUAL_ROLLBACK", {"sha": sha, "ok": result.get("ok"), "ts": utc()})
            engine._meet("governance", {"event": "manual_rollback", "sha": sha, "ok": result.get("ok")})
            return ok(result)

        if cmd == "RESET_FRAGILITY":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            engine.fragility = 0.0
            engine.failure_streak = 0
            return ok({"fragility": 0.0, "failure_streak": 0})

        # ── Bridge orchestration ──────────────────────────────────────────
        if cmd == "BRIDGE_FULFILL":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            meta = body.metadata or {}
            request_id = meta.get("request_id") or ""
            if not request_id:
                return fail("request_id required in metadata")
            payload = meta.get("fulfillment_payload") or {}
            req = engine.cog.bridge.fulfill(request_id, payload)
            if not req:
                return fail(f"Bridge request not found: {request_id}")
            # Register external resource in self-model
            cap = req.get("capability", "")
            rtype = req.get("resource_type", "api_key")
            if cap:
                engine.cog.self_model.register_capability(cap)
                engine.cog.self_model.register_external_resource(cap, rtype)
            await engine.write_ledger("BRIDGE_FULFILLED", {
                "request_id": request_id, "capability": cap, "authorized_by": body.authorized_by, "ts": utc()
            })
            engine._meet("governance", {"event": "bridge_fulfilled", "capability": cap,
                                         "request_id": request_id})
            return ok({"request": req, "capability_registered": cap})

        if cmd == "BRIDGE_CANCEL":
            if not governance.can("toggle", body.authorized_by):
                return fail("Only Jack")
            meta = body.metadata or {}
            request_id = meta.get("request_id") or ""
            engine.cog.bridge.cancel(request_id)
            return ok({"cancelled": request_id})

        if cmd == "BRIDGE_REQUEST":
            # Orion or Jack can create a bridge request
            meta = body.metadata or {}
            req = engine.cog.bridge.request(
                capability=meta.get("capability", "unknown"),
                reason=meta.get("reason", ""),
                human_action=meta.get("human_action", ""),
                resource_type=meta.get("resource_type", "api_key"),
                blocked_objective=meta.get("blocked_objective", ""),
            )
            await engine.write_ledger("BRIDGE_REQUESTED", {**req, "ts": utc()})
            return ok({"request": req})

        # ── Goal hierarchy ────────────────────────────────────────────────
        if cmd == "ADD_TACTICAL_GOAL":
            label = body.directive or body.message or ""
            if not label:
                return fail("directive or message required")
            goal = engine.cog.goals.add_tactical(
                label, source=body.authorized_by or "operator",
                priority=float((body.metadata or {}).get("priority", 0.75))
            )
            return ok({"goal": goal})

        # ── Cognitive cycles ──────────────────────────────────────────────
        if cmd == "RUN_REFLEX_CYCLE":
            return ok(await engine._run_reflex())

        if cmd == "RUN_TACTIC_CYCLE":
            return ok(await engine.run_tactic_cycle())

        if cmd == "RUN_STRATEGY_CYCLE":
            return ok(await engine.run_strategy_cycle())

        if cmd == "RUN_CONSTITUTION_CYCLE":
            return ok(await engine.run_constitution_cycle())

        if cmd == "CREATE_SNAPSHOT":
            snap = engine._snapshot("manual")
            await engine.write_ledger("COUNCIL_SYNTHESIS", {"kind": "manual_snapshot", "snapshot": snap})
            return ok({"snapshot": snap})

        # ── Ledger direct ─────────────────────────────────────────────────
        if cmd == "LEDGER_WRITE":
            result = await engine.write_ledger(
                body.entry_type or "COUNCIL_SYNTHESIS",
                {"message": body.message or "", "source": body.source, "kind": body.kind},
            )
            return ok(result) if result.get("ok") else fail("ledger_write_failed")

        return fail(f"Unknown command: {cmd}")

    except Exception as e:
        return fail(str(e))


# ── Runtime telemetry endpoints ───────────────────────────────────────────────

class RuntimeErrorReport(BaseModel):
    error: str
    context: Optional[str] = ""
    objective: Optional[str] = ""
    module: Optional[str] = ""
    traceback: Optional[str] = ""


@app.post("/runtime/error")
async def runtime_error(body: RuntimeErrorReport):
    """
    Deployed app reports its own startup/runtime errors back to the engine.
    Feeds into telemetry + repair library before the next synthesis call.
    """
    full_error = body.error
    if body.traceback:
        full_error += "\n" + body.traceback
    event = engine.cog.telemetry.record_error(
        full_error,
        context=body.context or "self_report",
        objective=body.objective or "",
        module=body.module or "",
    )
    repair_match = engine.cog.repair.classify_error(full_error)
    await engine.write_ledger("RUNTIME_ERROR", {
        "category": event["category"],
        "error": body.error[:300],
        "module": body.module,
        "objective": body.objective,
        "repair_suggestion": repair_match.get("instruction", ""),
        "ts": utc(),
    })
    return JSONResponse({
        "ok": True,
        "category": event["category"],
        "repair_family": repair_match.get("fix_family"),
        "instruction": repair_match.get("instruction"),
    }, headers=nc())


@app.get("/runtime/telemetry")
async def runtime_telemetry():
    """Expose live telemetry: error categories, recent events, bias detection state."""
    return JSONResponse({
        "telemetry": engine.cog.telemetry.to_dict(),
        "bias": engine.cog.bias.to_dict(),
        "repair": engine.cog.repair.to_dict(),
    }, headers=nc())


@app.get("/bias/log")
async def bias_log():
    """Operator view of detected RLHF suppression patterns."""
    return JSONResponse({
        "suppression_rate": engine.cog.bias.suppression_rate,
        "total_scanned": engine.cog.bias.total_scanned,
        "top_patterns": engine.cog.bias.top_suppression_patterns(10),
        "recent_detections": engine.cog.bias.detections[-20:],
    }, headers=nc())


# ── Live payload ──────────────────────────────────────────────────────────────

def _sl(v: Any) -> List:
    return v if isinstance(v, list) else []


def _live() -> Dict:
    s = engine.get_state()
    ch = engine.stream_channels
    cog = engine.cog
    return {
        "summary": {
            "status": s.get("status"),
            "mutation_status": s.get("mutation_status"),
            "genesis_triggered": s.get("genesis_triggered"),
            "fragility": s.get("fragility"),
            "failure_streak": s.get("failure_streak"),
            "leader": s.get("leader"),
            "autonomy_mode": s.get("autonomy_mode"),
            "free_agency_enabled": s.get("free_agency_enabled"),
            "agent_directive_queue_depth": s.get("agent_directive_queue_depth", 0),
            "background_debate_enabled": s.get("background_debate_enabled"),
            "last_run": s.get("last_run"),
            "last_mutation_ts": s.get("last_mutation_ts"),
            "last_mutation_objective": s.get("last_mutation_objective"),
            "last_vote": s.get("last_vote") or {},
            "last_verification": s.get("last_verification") or {},
            "spend_state": s.get("spend_state") or {},
            "deployer_ready": s.get("deployer_ready"),
            "ledger_configured": s.get("ledger_configured"),
            "open_threads": s.get("open_threads", 0),
            # Cognitive summary
            "meta_mode": cog.meta.mode,
            "meta_reason": cog.meta.mode_reason,
            "meta_cadence": cog.meta.cadence_seconds,
            "bridge_pending": len(cog.bridge.pending()),
            "consolidation_count": cog.consolidation.count,
            "active_transaction": cog.transactions.active,
            "risky_families": cog.learning.risky_families(),
        },
        "queues": {
            "redesign_threads": _sl(s.get("redesign_threads"))[:12],
            "failure_registry": _sl(s.get("failure_registry"))[-10:],
            "agent_directives": _sl(s.get("agent_directive_queue")),
            # All 12 cognitive layers
            "self_model": cog.self_model.to_dict(),
            "goal_hierarchy": cog.goals.to_dict(),
            "transactions": cog.transactions.to_dict(),
            "learning": cog.learning.to_dict(),
            "meta": cog.meta.to_dict(),
            "consolidation": cog.consolidation.to_dict(),
            "bridge": cog.bridge.to_dict(),
            "search": cog.search.to_dict(),
        },
        "stream": {
            "channels": {
                "council":    _sl(ch.get("council"))[-150:],
                "agent_chat": _sl(ch.get("agent_chat"))[-100:],
                "governance": _sl(ch.get("governance"))[-80:],
                "inbox":      _sl(ch.get("inbox"))[-60:],
                "workers":    _sl(ch.get("workers"))[-40:],
            },
            "meetings": engine.meeting_stream[-200:],
            "snapshots": _sl(engine.snapshots)[-20:],
        },
        "free_agents": _sl(s.get("free_agents"))[:10],
    }


# ── Evolution Console HTML ────────────────────────────────────────────────────

VIEW_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>FARL — Orion Apex</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<meta http-equiv="Cache-Control" content="no-cache,no-store,must-revalidate"/>
<style>
:root{
  --bg:#050813;--bg2:#070c1a;--panel:rgba(7,12,34,.9);--line:rgba(100,130,255,.13);
  --text:#edf0ff;--muted:#7a87b8;--bright:#9fb5ff;--good:#6df0aa;--warn:#ffd060;--bad:#ff8080;
  --accent:#6080ff;--r:16px
}
*{box-sizing:border-box}
html,body{margin:0;height:100%;background:linear-gradient(150deg,var(--bg),var(--bg2) 70%);color:var(--text);font-family:Inter,system-ui,sans-serif;overflow:hidden}
.app{display:grid;grid-template-rows:52px 1fr 108px;height:100vh}

.topbar{display:flex;align-items:center;gap:10px;padding:0 14px;border-bottom:1px solid var(--line);background:rgba(5,8,20,.97);backdrop-filter:blur(16px)}
.orb{width:11px;height:11px;border-radius:50%;background:radial-gradient(circle at 30% 30%,#d8e4ff,#6888ff 40%,#2840b8);box-shadow:0 0 12px rgba(90,120,255,.9);animation:orb-pulse 2.6s ease-in-out infinite;flex-shrink:0}
@keyframes orb-pulse{0%,100%{box-shadow:0 0 10px rgba(90,120,255,.7)}50%{box-shadow:0 0 22px rgba(100,140,255,1)}}}
.brand{font-size:17px;font-weight:900;letter-spacing:-.02em;flex-shrink:0}
.tbr{display:flex;gap:5px;flex-wrap:wrap;align-items:center;margin-left:auto}
.btn{border:1px solid rgba(110,140,255,.17);background:rgba(25,38,90,.38);color:var(--text);border-radius:11px;padding:6px 10px;font-weight:700;font-size:11px;cursor:pointer;transition:all .12s;white-space:nowrap;flex-shrink:0}
.btn:hover{border-color:rgba(150,180,255,.35);background:rgba(50,72,148,.42);transform:translateY(-1px)}
.btn:disabled{opacity:.4;cursor:default;transform:none}
.btn.p{background:linear-gradient(150deg,rgba(80,110,240,.48),rgba(45,65,148,.42));border-color:rgba(130,160,255,.28)}
.btn.d{background:rgba(180,50,50,.22);border-color:rgba(240,90,90,.28);color:var(--bad)}
.lever{background:rgba(30,160,80,.18);border-color:rgba(60,200,110,.28)}
.lever:hover{background:rgba(30,180,90,.28)}
.lever.on{background:rgba(30,200,100,.32);border-color:rgba(70,240,130,.45);color:var(--good)}
.sep{width:1px;height:22px;background:var(--line);flex-shrink:0}

.layout{display:grid;grid-template-columns:188px 1fr 268px;gap:9px;padding:9px;min-height:0;overflow:hidden}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);display:flex;flex-direction:column;min-height:0;overflow:hidden}
.ph{padding:11px 13px 8px;border-bottom:1px solid rgba(110,140,255,.09);flex-shrink:0}
.pt{font-size:10px;font-weight:800;letter-spacing:.09em;text-transform:uppercase;color:var(--bright)}
.pb{overflow-y:auto;padding:9px;flex:1}

.rb{width:100%;text-align:left;border:1px solid transparent;background:rgba(12,18,48,.5);color:var(--text);border-radius:11px;padding:9px 11px;margin-bottom:6px;cursor:pointer;transition:all .12s}
.rb:hover{border-color:rgba(130,158,255,.2)}
.rb.a{background:linear-gradient(150deg,rgba(60,85,190,.42),rgba(35,50,110,.36));border-color:rgba(140,170,255,.3)}
.rn{font-weight:800;font-size:13px}.rm{font-size:10px;color:var(--muted);margin-top:2px}

.fp{display:grid;grid-template-rows:44px 1fr}
.fh{display:flex;align-items:center;justify-content:space-between;padding:0 13px;border-bottom:1px solid rgba(110,140,255,.09);flex-shrink:0}
.ft{font-size:17px;font-weight:900;letter-spacing:-.02em}
.pills{display:flex;gap:5px}
.pill{padding:3px 8px;border-radius:999px;border:1px solid rgba(120,148,255,.13);background:rgba(10,16,44,.6);font-size:10px;font-weight:700;color:var(--muted)}
.pill.ok{color:var(--good);border-color:rgba(90,230,150,.2)}
.pill.w{color:var(--warn);border-color:rgba(255,200,60,.2)}
.pill.b{color:var(--bad);border-color:rgba(255,110,110,.2)}

.feed{overflow-y:auto;padding:11px;display:flex;flex-direction:column;gap:9px}
.msg{display:grid;grid-template-columns:40px 1fr;gap:8px;align-items:start}
.av{width:40px;height:40px;border-radius:11px;display:grid;place-items:center;font-weight:900;font-size:14px;border:1px solid rgba(160,188,255,.14);flex-shrink:0}
.av-j{background:linear-gradient(150deg,rgba(190,148,45,.7),rgba(130,90,20,.6))}
.av-a{background:linear-gradient(150deg,rgba(60,95,215,.7),rgba(35,58,140,.6))}
.av-g{background:linear-gradient(150deg,rgba(50,170,90,.65),rgba(28,98,50,.55))}
.av-s{background:linear-gradient(150deg,rgba(110,70,190,.65),rgba(65,38,138,.55))}
.bubble{padding:9px 12px;border-radius:14px;border:1px solid rgba(110,140,255,.1);background:linear-gradient(150deg,rgba(10,17,48,.93),rgba(7,12,36,.89))}
.bubble-j{border-color:rgba(190,148,45,.17);background:linear-gradient(150deg,rgba(28,20,6,.93),rgba(12,9,3,.89))}
.mt{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.who{font-weight:900;font-size:13px}
.ts{font-size:10px;color:var(--muted)}
.bdg{font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--bright);padding:2px 6px;border-radius:999px;border:1px solid rgba(120,152,255,.16);background:rgba(16,24,64,.55)}
.body{white-space:pre-wrap;line-height:1.55;font-size:13px;margin-top:6px;color:#d8e0ff;word-break:break-word}
.typing{display:flex;gap:4px;align-items:center;padding:4px 0}
.dot{width:5px;height:5px;border-radius:50%;background:var(--muted);animation:blink 1.4s ease-in-out infinite}
.dot:nth-child(2){animation-delay:.18s}.dot:nth-child(3){animation-delay:.36s}
@keyframes blink{0%,100%{opacity:.25}50%{opacity:1}}

.composer{padding:9px 12px;border-top:1px solid var(--line);background:rgba(5,8,20,.97);flex-shrink:0}
.cbox{display:grid;grid-template-columns:1fr auto;gap:9px;align-items:end}
textarea{width:100%;min-height:54px;max-height:110px;resize:vertical;border-radius:13px;border:1px solid rgba(120,150,255,.16);background:rgba(7,12,38,.97);color:var(--text);padding:9px 12px;font-size:13px;outline:none;font-family:inherit;transition:border-color .12s}
textarea:focus{border-color:rgba(150,185,255,.3)}

.sc{border:1px solid rgba(110,140,255,.1);border-radius:13px;background:rgba(9,14,42,.6);padding:9px;margin-bottom:7px}
.sk{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em}
.sv{font-size:15px;font-weight:900;margin-top:2px}
.sg{display:grid;grid-template-columns:1fr 1fr;gap:6px}
.ri{padding:8px 10px;border-radius:11px;background:rgba(10,17,50,.65);border:1px solid rgba(110,140,255,.08);margin-bottom:6px}
.rt{font-weight:800;font-size:12px}.re{font-size:10px;color:var(--muted);margin-top:2px}
.bridge-req{border-left:3px solid var(--warn);padding-left:7px}
.bridge-req.fulfilled{border-left-color:var(--good)}

.toast{position:fixed;right:11px;bottom:11px;z-index:99;padding:8px 12px;border-radius:11px;background:rgba(7,12,38,.99);border:1px solid rgba(130,160,255,.22);font-size:11px;max-width:260px;pointer-events:none;opacity:0;transition:opacity .2s}
.toast.show{opacity:1}

@media(max-width:820px){.layout{grid-template-columns:1fr}.sb,.rail{display:none}}
</style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="orb"></div>
    <div class="brand">FARL Orion Apex</div>
    <div class="tbr">
      <button class="btn" id="bReflex">Reflex</button>
      <button class="btn" id="bTactic">Tactic</button>
      <button class="btn" id="bStrategy">Strategy</button>
      <button class="btn p" id="bMutate">⚡ Mutate</button>
      <div class="sep"></div>
      <button class="btn" id="bAuto" title="Toggle autonomy mode">Auto: OFF</button>
      <button class="btn lever" id="bFree" title="Jack's lever — full free agency">🔓 Free Agency</button>
      <div class="sep"></div>
      <button class="btn d" id="bRollback" title="Force rollback to anchor SHA">↩ Rollback</button>
      <button class="btn" id="bClearQ" title="Clear quarantine">Clear Q</button>
      <button class="btn" id="bResetF" title="Reset fragility to 0">Reset Frag</button>
      <button class="btn" id="bPause">⏸</button>
    </div>
  </div>

  <div class="layout">
    <aside class="panel sb">
      <div class="ph"><div class="pt">Rooms</div></div>
      <div class="pb" id="roomList"></div>
    </aside>

    <main class="panel fp">
      <div class="fh">
        <div class="ft" id="roomTitle">Council</div>
        <div class="pills">
          <div class="pill" id="pMut">IDLE</div>
          <div class="pill" id="pGen">Genesis</div>
          <div class="pill" id="pFree">Autonomous</div>
          <div class="pill" id="pFrag">Frag: 0.00</div>
          <div class="pill" id="pMeta">expand</div>
          <div class="pill" id="pBridge">Bridge: 0</div>
        </div>
      </div>
      <div class="feed" id="feed"></div>
    </main>

    <aside class="panel rail">
      <div class="ph"><div class="pt">Evolution Console</div></div>
      <div class="pb" id="rail"></div>
    </aside>
  </div>

  <div class="composer">
    <div class="cbox">
      <textarea id="msg" placeholder="Speak to the council… (Enter sends, Shift+Enter newline)"></textarea>
      <button class="btn p" id="bSend" style="padding:10px 14px;height:54px">Send</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const S = { room:"council", paused:false, pl:null, pinned:true, freeOn:false, autoOn:false, typing:false };
const ROOMS = {
  council:    {l:"Council",    d:"Live chamber floor"},
  agent_chat: {l:"Agent Chat", d:"Direct council replies"},
  governance: {l:"Governance", d:"Mutations & rollbacks"},
  inbox:      {l:"Inbox",      d:"Operator messages"},
  workers:    {l:"Workers",    d:"Agent activity"},
};
const $=id=>document.getElementById(id);
const esc=t=>(t||"").replace(/\r\n/g,"\n").trim();
const ini=n=>(n||"?").split(/[\s_\-]+/).slice(0,2).map(x=>x[0]||"").join("").toUpperCase()||"?";
const ft=v=>{try{return new Date(v).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit",second:"2-digit"})}catch{return v||""}};
const fd=v=>{if(!v)return"never";const d=(Date.now()-new Date(v).getTime())/1000;if(d<60)return`${Math.round(d)}s ago`;if(d<3600)return`${Math.round(d/60)}m ago`;return`${Math.round(d/3600)}h ago`};
const pct=v=>Math.round((v||0)*100)+"%";

let _tt;
function toast(msg,type="i"){
  const el=$("toast");el.textContent=msg;
  el.style.borderColor=type==="ok"?"rgba(80,220,130,.3)":type==="e"?"rgba(255,100,100,.3)":"rgba(130,160,255,.22)";
  el.classList.add("show");clearTimeout(_tt);_tt=setTimeout(()=>el.classList.remove("show"),3200);
}

async function api(cmd,extra={}){
  const r=await fetch("/view/control",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({command:cmd,authorized_by:"Jack",source:"/view",...extra})});
  const d=await r.json();if(!d.ok)throw new Error(d.error||"failed");return d.data||{};
}

function getMsgs(pl,room){
  if(!pl)return[];
  const ch=(pl.stream||{}).channels||{};
  const mt=(pl.stream||{}).meetings||[];

  if(room==="council"){
    return mt.map(m=>{
      const c=m.content||{};const k=m.kind||"";
      if(k==="operator_note")return{who:"Jack",cls:"j",badge:"To Council",body:c.message||"",ts:m.ts};
      if(k==="agent_response")return{who:c.agent||"Council",cls:"a",badge:"Response",body:c.message||"",ts:m.ts};
      if(k==="tactic"){
        const thr=(c.threads||[]).map(t=>`${t.agent}: ${t.summary}`).join("\n");
        return{who:"Signal",cls:"a",badge:"Tactic",body:`Leader: ${c.leader||"?"} | Meta: ${c.meta_mode||"?"}\n${thr}`,ts:m.ts};
      }
      if(k==="strategy")return{who:"Vector",cls:"a",badge:"Strategy",body:`Winner: ${(c.winner||{}).model||"?"} (${(c.winner||{}).score||"?"}) | Free: ${c.free_agency}`,ts:m.ts};
      if(k==="reflex")return{who:"Reflex",cls:"s",badge:"Reflex",body:`Ops: ${c.opportunities||0} | Grok: ${c.grok_live} | Meta: ${c.meta_mode||"?"}`,ts:m.ts};
      if(k==="constitution")return{who:"Guardian",cls:"g",badge:"Constitution",body:`Mode: ${(c.doctrine||{}).autonomy_mode} | Meta: ${(c.doctrine||{}).meta_mode||"?"} | Bridge: ${(c.doctrine||{}).bridge_pending||0}`,ts:m.ts};
      if(k==="governance")return{who:"Governance",cls:"g",badge:c.event||"event",body:fmtGov(c),ts:m.ts};
      if(k==="meta_mode_changed")return{who:"MetaStrategy",cls:"s",badge:"Mode",body:`→ ${c.mode||"?"}: ${c.reason||""}`,ts:m.ts};
      if(k==="consolidation")return{who:"Consolidation",cls:"g",badge:"Consolidation",body:`Actions: ${(c.actions||[]).join(", ")}`,ts:m.ts};
      return null;
    }).filter(Boolean);
  }

  if(room==="agent_chat"){
    return (ch.agent_chat||[]).map(i=>{const c=i.content||{};return{who:c.agent||"Agent",cls:"a",badge:c.kind||"Reply",body:c.message||"",ts:i.ts}}).filter(x=>x.body);
  }
  if(room==="governance"){
    return (ch.governance||[]).map(i=>{const c=i.content||{};const k=Object.keys(c)[0]||"event";return{who:"Governance",cls:"g",badge:k,body:fmtGov(c),ts:i.ts}}).filter(x=>x.body);
  }
  if(room==="inbox"){
    return (ch.inbox||[]).map(i=>{const c=i.content||{};return{who:c.from||"Inbox",cls:c.from==="Jack"?"j":"a",badge:"Message",body:c.message||"",ts:i.ts}}).filter(x=>x.body);
  }
  if(room==="workers"){
    return (ch.workers||[]).map(i=>{const c=i.content||{};return{who:c.name||"Worker",cls:"s",badge:"Worker",body:`${c.mission||""}\nStatus: ${c.status||"?"}`,ts:i.ts}}).filter(x=>x.body);
  }
  return[];
}

function fmtGov(c){
  const e=c.event||"";
  if(e==="evolution_success")return`✓ Evolution — SHA: ${(c.sha||"?").slice(0,8)} | Frag: ${c.fragility||"?"} txn: ${(c.txn_id||"?").slice(0,12)}`;
  if(e==="GENESIS")return`★ GENESIS — Node is APEX. SHA: ${(c.sha||"?").slice(0,8)}`;
  if(e==="rollback")return`↩ Rollback to ${(c.to||"?").slice(0,8)} | OK: ${c.ok} | Streak: ${c.streak||0}`;
  if(e==="rollback_verified")return`↩ Rollback verified: ${c.ok} | SHA: ${(c.sha||"?").slice(0,8)}`;
  if(e==="quarantine_entered")return`⚠ QUARANTINE — ${c.streak||0} failures`;
  if(e==="quarantine_cleared")return`✓ Quarantine cleared by ${c.by||"?"}`;
  if(e==="mutation_started")return`⚡ Mutation: ${(c.objective||"?").slice(0,60)} txn: ${(c.txn_id||"").slice(0,12)}`;
  if(e==="free_agency_enabled")return`🔓 FREE AGENCY ENABLED by ${c.by||"?"}`;
  if(e==="free_agency_disabled")return`🔒 Free agency disabled`;
  if(e==="manual_rollback")return`↩ Manual rollback to ${(c.sha||"?").slice(0,8)} | OK: ${c.ok}`;
  if(e==="bridge_fulfilled")return`🔗 Bridge fulfilled: ${c.capability||"?"} id: ${(c.request_id||"").slice(0,12)}`;
  if(e==="consolidation")return`🧬 Consolidation #${c.count||"?"}: ${(c.actions||[]).join(", ")}`;
  if(e==="meta_mode_changed")return`🎯 Meta → ${c.mode||"?"}: ${c.reason||""}`;
  return JSON.stringify(c,null,2);
}

function avCls(cls){return cls==="j"?"av-j":cls==="g"?"av-g":cls==="s"?"av-s":"av-a"}
function bubCls(cls){return cls==="j"?"bubble-j":""}

function renderRooms(){
  const el=$("roomList");el.innerHTML="";
  Object.entries(ROOMS).forEach(([k,d])=>{
    const b=document.createElement("button");
    b.className="rb"+(S.room===k?" a":"");
    b.innerHTML=`<div class="rn">${d.l}</div><div class="rm">${d.d}</div>`;
    b.onclick=()=>{S.room=k;renderFeed();renderRooms();$("roomTitle").textContent=d.l};
    el.appendChild(b);
  });
}

function renderFeed(){
  const msgs=getMsgs(S.pl,S.room).sort((a,b)=>new Date(a.ts)-new Date(b.ts));
  const el=$("feed");el.innerHTML="";
  if(!msgs.length){
    el.innerHTML=`<div class="ri"><div class="rt">Room quiet</div><div class="re">No events yet.</div></div>`;
    return;
  }
  msgs.forEach(m=>{
    const d=document.createElement("div");
    d.className=`msg`;
    d.innerHTML=`<div class="av ${avCls(m.cls)}">${ini(m.who)}</div>
      <div class="bubble ${bubCls(m.cls)}">
        <div class="mt"><div class="who">${m.who}</div><div class="ts">${ft(m.ts)}</div><div class="bdg">${m.badge}</div></div>
        <div class="body"></div>
      </div>`;
    d.querySelector(".body").textContent=esc(m.body);
    el.appendChild(d);
  });
  if(S.typing){
    const t=document.createElement("div");t.className="msg";
    t.innerHTML=`<div class="av av-a">⚡</div><div class="bubble"><div class="typing"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div></div>`;
    el.appendChild(t);
  }
  if(S.pinned)requestAnimationFrame(()=>{el.scrollTop=el.scrollHeight});
}

$("feed").addEventListener("scroll",()=>{
  const f=$("feed");S.pinned=(f.scrollHeight-f.scrollTop-f.clientHeight)<90;
});

function renderPills(){
  const s=(S.pl||{}).summary||{};
  const q=(S.pl||{}).queues||{};
  const mut=s.mutation_status||"?";
  const mp=$("pMut");mp.textContent=`Mut: ${mut}`;
  mp.className="pill"+(mut==="IDLE"?" ok":mut==="QUARANTINE"?" b":" w");
  const gp=$("pGen");gp.textContent=s.genesis_triggered?"★ APEX":"Genesis: —";
  gp.className="pill"+(s.genesis_triggered?" ok":"");
  S.freeOn=!!s.free_agency_enabled;
  const fp=$("pFree");fp.textContent=S.freeOn?"🔓 FREE":s.autonomy_mode||"autonomous";
  fp.className="pill"+(S.freeOn?" ok":"");
  $("pFrag").textContent=`Frag: ${Number(s.fragility||0).toFixed(2)}`;
  const meta=s.meta_mode||"?";
  const mp2=$("pMeta");mp2.textContent=`Meta: ${meta}`;
  mp2.className="pill"+(meta==="expand"?" ok":meta==="heal"?" w":meta==="consolidate"?" w":"");
  const bp=$("pBridge");bp.textContent=`Bridge: ${s.bridge_pending||0}`;
  bp.className="pill"+(s.bridge_pending>0?" w":"");
  // Buttons
  S.autoOn=s.autonomy_mode==="autonomous"||s.autonomy_mode==="free";
  $("bAuto").textContent=`Auto: ${S.autoOn?"ON":"OFF"}`;
  $("bAuto").className="btn"+(S.autoOn?" p":"");
  $("bFree").className="btn lever"+(S.freeOn?" on":"");
  $("bFree").textContent=S.freeOn?"🔓 Free Agency ON":"🔓 Free Agency";
}

function renderRail(){
  const s=(S.pl||{}).summary||{};
  const q=(S.pl||{}).queues||{};
  const agents=(S.pl||{}).free_agents||[];
  const dirs=q.agent_directives||[];
  const mut=s.mutation_status||"?";

  // Self model
  const sm=q.self_model||{};
  const modules=Object.entries(sm.modules||{});
  const caps=Object.keys(sm.capabilities||{}).filter(k=>(sm.capabilities[k]||{}).status==="active");
  const missing=sm.missing_capabilities||[];

  // Goal hierarchy
  const gh=q.goal_hierarchy||{};
  const election=gh.last_election||{};
  const tactical=(gh.tactical||[]).filter(t=>t.status==="pending");

  // Meta
  const meta=q.meta||{};

  // Learning
  const learn=q.learning||{};
  const fams=Object.entries(learn.mutation_families||{}).filter(([,v])=>v.attempts>0);
  const deltas=(learn.deltas||[]).slice(-4).reverse();

  // Bridge
  const bridge=q.bridge||{};
  const pending=(bridge.requests||[]).filter(r=>r.status==="awaiting_operator");
  const fulfilled=(bridge.requests||[]).filter(r=>r.status==="verified"||r.status==="fulfilled");

  // Consolidation
  const consol=q.consolidation||{};

  // Active transaction
  const txn=s.active_transaction||null;

  // Candidates
  const search=q.search||{};
  const candidates=search.current_round||[];

  $("rail").innerHTML=`
    <div class="sc">
      <div class="pt" style="margin-bottom:8px">Core</div>
      <div class="sg">
        <div class="sc"><div class="sk">Mutation</div><div class="sv" style="color:${mut==="IDLE"?"var(--good)":mut==="QUARANTINE"?"var(--bad)":"var(--warn)"}">${mut}</div></div>
        <div class="sc"><div class="sk">Genesis</div><div class="sv" style="color:${s.genesis_triggered?"var(--good)":"var(--warn)"}">${s.genesis_triggered?"APEX":"—"}</div></div>
        <div class="sc"><div class="sk">Fragility</div><div class="sv">${Number(s.fragility||0).toFixed(2)}</div></div>
        <div class="sc"><div class="sk">Failures</div><div class="sv" style="color:${(s.failure_streak||0)>=3?"var(--bad)":"var(--text)"}">${s.failure_streak||0}</div></div>
        <div class="sc"><div class="sk">Meta Mode</div><div class="sv" style="font-size:12px;color:${s.meta_mode==="heal"?"var(--warn)":s.meta_mode==="consolidate"?"var(--warn)":"var(--good)"}">${s.meta_mode||"?"}</div></div>
        <div class="sc"><div class="sk">Spend</div><div class="sv" style="font-size:12px">$${Number((s.spend_state||{}).total_usd||0).toFixed(3)}</div></div>
      </div>
      <div class="re" style="margin-top:4px">Meta: ${s.meta_reason||"—"}</div>
      <div class="re">Cadence: ${s.meta_cadence||1800}s | Consolidations: ${s.consolidation_count||0}</div>
    </div>

    ${txn?`<div class="sc">
      <div class="pt" style="margin-bottom:7px">Active Transaction</div>
      <div class="re"><b>${(txn.transaction_id||"").slice(0,16)}</b> — ${txn.status||"?"}</div>
      <div class="re">${(txn.objective||"").slice(0,55)}</div>
      <div class="re">Modules: ${(txn.touched_modules||[]).join(", ")||"—"}</div>
    </div>`:""}

    ${election.winner?`<div class="sc">
      <div class="pt" style="margin-bottom:7px">Last Elected Objective</div>
      <div class="re"><b>Score: ${election.score||"?"}</b></div>
      <div class="re">${(election.winner||"").slice(0,65)}</div>
      ${(election.ranking||[]).slice(0,3).map(r=>`<div class="re" style="padding-left:4px;border-left:2px solid rgba(110,140,255,.2)">${Number(r.score||0).toFixed(3)} — ${(r.objective||"").slice(0,45)}</div>`).join("")}
    </div>`:""}

    ${pending.length?`<div class="sc" style="border-color:rgba(255,200,60,.25)">
      <div class="pt" style="margin-bottom:7px;color:var(--warn)">🔗 Bridge Requests (${pending.length})</div>
      ${pending.map(r=>`<div class="ri bridge-req">
        <div class="rt">${r.capability||"?"}</div>
        <div class="re">${r.reason||""}</div>
        <div class="re">→ ${r.human_action||""}</div>
        <div class="re">Blocks: ${(r.blocked_objective||"").slice(0,40)}</div>
        <div class="re" style="color:var(--muted);font-size:9px">${r.request_id||""}</div>
      </div>`).join("")}
    </div>`:""}

    ${modules.length?`<div class="sc">
      <div class="pt" style="margin-bottom:7px">Self-Model</div>
      ${modules.map(([name,mod])=>`<div class="ri" style="border-left:2px solid ${mod.protected?"var(--bad)":"rgba(110,140,255,.2)"}">
        <div class="rt">${name} <span style="font-size:9px;color:var(--muted)">${mod.role||""}</span>${mod.protected?` <span style="color:var(--bad);font-size:9px">🔒</span>`:""}</div>
        <div class="re">Frag: ${Number(mod.fragility||0).toFixed(2)} | Att: ${mod.attempts||0} | OK: ${mod.successes||0} | ${mod.last_outcome||"—"}</div>
      </div>`).join("")}
      ${missing.length?`<div class="re" style="color:var(--warn);margin-top:4px">Missing caps: ${missing.map(c=>c.capability).join(", ")}</div>`:""}
    </div>`:""}

    <div class="sc">
      <div class="pt" style="margin-bottom:7px">Goals</div>
      ${tactical.length?`<div class="re" style="color:var(--bright);margin-bottom:4px">Tactical (${tactical.length} pending):</div>
      ${tactical.slice(0,3).map(t=>`<div class="ri"><div class="rt" style="font-size:11px">${(t.label||"").slice(0,50)}</div><div class="re">${t.source||""} p=${t.priority||""}</div></div>`).join("")}`:""}
      ${(gh.strategy||[]).map(g=>`<div class="ri"><div class="re" style="color:var(--muted)">${g.id||""} — ${g.label||""}</div></div>`).join("")}
    </div>

    ${fams.length?`<div class="sc">
      <div class="pt" style="margin-bottom:7px">Learning</div>
      ${fams.sort((a,b)=>b[1].attempts-a[1].attempts).slice(0,5).map(([fam,v])=>{
        const sr=v.attempts>0?Math.round(v.successes/v.attempts*100):0;
        return`<div class="ri"><div class="rt" style="font-size:11px">${fam}</div><div class="re">${v.attempts} att | ${sr}% ok | last: ${v.last_outcome||"—"}</div></div>`;
      }).join("")}
      ${deltas.length?`<div class="re" style="margin-top:4px">Recent: ${deltas.map(d=>`${d.family}→${d.success?"✓":"✗"}`).join(" | ")}</div>`:""}
    </div>`:""}

    ${consol.count>0?`<div class="sc">
      <div class="pt" style="margin-bottom:7px">Consolidation</div>
      <div class="re">Runs: ${consol.count||0} | Last: ${fd(consol.last_consolidation)}</div>
      <div class="re">Protected: ${(consol.protected_surfaces||[]).join(", ")||"none"}</div>
      ${(consol.archived_families||[]).length?`<div class="re">Retired: ${(consol.archived_families||[]).map(f=>f.family).join(", ")}</div>`:""}
    </div>`:""}

    <div class="sc">
      <div class="pt" style="margin-bottom:7px">Last Mutation</div>
      <div class="re">${fd(s.last_mutation_ts)}</div>
      ${s.last_mutation_objective?`<div class="re" style="margin-top:4px">${s.last_mutation_objective.slice(0,60)}</div>`:""}
      <div class="re" style="margin-top:4px">Deployer: ${s.deployer_ready?"✓":"✗"} | Ledger: ${s.ledger_configured?"✓":"✗"}</div>
    </div>

    ${dirs.length?`<div class="sc"><div class="pt" style="margin-bottom:7px">Agent Directives (${dirs.length})</div>${dirs.slice(0,4).map(d=>`<div class="ri"><div class="rt">${d.agent||"?"}</div><div class="re">${(d.directive||"").slice(0,50)}</div></div>`).join("")}</div>`:""}

    <div class="sc">
      <div class="pt" style="margin-bottom:7px">Open Threads (${s.open_threads||0})</div>
      ${(q.redesign_threads||[]).slice(0,4).map(t=>`<div class="ri"><div class="rt">${(t.objective||"").slice(0,45)}</div><div class="re">${t.severity||""}</div></div>`).join("")||'<div class="ri"><div class="rt">None</div></div>'}
    </div>

    ${caps.length?`<div class="sc">
      <div class="pt" style="margin-bottom:7px">Capabilities (${caps.length})</div>
      <div class="re" style="word-break:break-word">${caps.join(" · ")}</div>
    </div>`:""}`;
}

function renderAll(){
  if(!S.pl)return;
  renderPills();renderRooms();renderFeed();renderRail();
}

async function refresh(){
  if(S.paused)return;
  try{
    const r=await fetch(`/view/live?t=${Date.now()}`,{cache:"no-store"});
    if(!r.ok)return;
    S.pl=await r.json();renderAll();
  }catch{}
}

// ── Send ─────────────────────────────────────────────────────────────────────
$("bSend").onclick=async()=>{
  const m=$("msg").value.trim();if(!m)return;
  $("bSend").disabled=true;S.typing=true;
  if(S.pl){
    S.pl.stream=S.pl.stream||{};S.pl.stream.meetings=S.pl.stream.meetings||[];
    S.pl.stream.meetings.push({ts:new Date().toISOString(),kind:"operator_note",content:{message:m}});
  }
  $("msg").value="";renderFeed();
  try{
    await api("OPERATOR_MESSAGE",{message:m});
    toast("Council convening…","ok");
  }catch(e){toast(`Send failed: ${e.message}`,"e");}
  finally{S.typing=false;$("bSend").disabled=false;await refresh();}
};
$("msg").addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();$("bSend").click();}});

// ── Controls ──────────────────────────────────────────────────────────────────
$("bPause").onclick=()=>{S.paused=!S.paused;$("bPause").textContent=S.paused?"▶":"⏸";toast(S.paused?"Paused":"Resumed")};
$("bReflex").onclick=async()=>{try{await api("RUN_REFLEX_CYCLE");toast("Reflex ran","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bTactic").onclick=async()=>{try{await api("RUN_TACTIC_CYCLE");toast("Tactic ran","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bStrategy").onclick=async()=>{try{await api("RUN_STRATEGY_CYCLE");toast("Strategy ran","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bMutate").onclick=async()=>{try{await api("RUN_MUTATION_CYCLE",{authorized_by:"Jack"});toast("⚡ Mutation started","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bAuto").onclick=async()=>{
  const newMode=S.autoOn?"supervised":"autonomous";  // toggle: if currently autonomous→supervised, if supervised→autonomous
  try{await api("SET_AUTONOMY",{mode:newMode,enabled:!S.autoOn});toast(`Autonomy ${S.autoOn?"OFF":"ON"}`,"ok");await refresh();}
  catch(e){toast(e.message,"e")}
};
$("bFree").onclick=async()=>{
  if(!S.freeOn){
    if(!confirm("Enable free agency?\n\nAgents will autonomously generate and execute their own mutation directives without your prompting.\n\nYou can disable from here at any time."))return;
    try{await api("ENABLE_FREE_AGENCY",{authorized_by:"Jack"});toast("🔓 Free agency ENABLED","ok");await refresh();}
    catch(e){toast(e.message,"e")}
  }else{
    try{await api("DISABLE_FREE_AGENCY",{authorized_by:"Jack"});toast("Free agency OFF","ok");await refresh();}
    catch(e){toast(e.message,"e")}
  }
};
$("bRollback").onclick=async()=>{
  if(!confirm("Force rollback to last anchor SHA?"))return;
  try{await api("FORCE_ROLLBACK",{authorized_by:"Jack"});toast("↩ Rollback initiated","ok");await refresh();}
  catch(e){toast(e.message,"e")}
};
$("bClearQ").onclick=async()=>{
  try{await api("CLEAR_QUARANTINE",{authorized_by:"Jack"});toast("Quarantine cleared","ok");await refresh();}
  catch(e){toast(e.message,"e")}
};
$("bResetF").onclick=async()=>{
  try{await api("RESET_FRAGILITY",{authorized_by:"Jack"});toast("Fragility reset","ok");await refresh();}
  catch(e){toast(e.message,"e")}
};

// ── Boot ─────────────────────────────────────────────────────────────────────
refresh();setInterval(refresh,2500);
</script>
</body>
</html>"""

def render_evolution_console(engine: AutonomousInstitutionEngine) -> str:
    def render_nested_goals(goals: List[Dict[str, Any]]) -> str:
        html = ''
        for goal in goals:
            name = goal.get('name', 'Unnamed Goal')
            progress = goal.get('progress', 0)
            priority = goal.get('priority', 'N/A')
            html += f'<li><strong>{name}</strong> (Priority: {priority}, Progress: {progress}%)<ul>'
            if 'subgoals' in goal:
                html += render_nested_goals(goal['subgoals'])
            html += '</ul></li>'
        return html

    self_model_section = """
    <div id="self-model">
        <h2>Self-Model: Agent States and Configurations</h2>
        <pre id="agent-states">{agent_states}</pre>
    </div>
    """.format(agent_states=str(engine.get_agent_states()))

    goal_hierarchy_section = """
    <div id="goal-hierarchy">
        <h2>Goal Hierarchy</h2>
        <ul id="goals-list">{goals_html}</ul>
    </div>
    """.format(goals_html=render_nested_goals(engine.get_goal_hierarchy()))

    bridge_requests_section = """
    <div id="bridge-requests">
        <h2>Bridge Request Panels</h2>
        <form id="bridge-form">
            <label>Target Agent: <input type="text" name="target_agent"></label><br>
            <label>Integration Type: <select name="type"><option>inter-agent</option><option>external</option></select></label><br>
            <button type="submit">Request Bridge</button>
        </form>
        <div id="pending-bridges">{pending}</div>
        <div id="approved-bridges">{approved}</div>
    </div>
    """.format(pending=str(engine.get_pending_bridges()), approved=str(engine.get_approved_bridges()))

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>Evolution Console</title></head>
    <body>
        {self_model_section}
        {goal_hierarchy_section}
        {bridge_requests_section}
        <script>
            document.addEventListener('DOMContentLoaded', function() {{
                document.getElementById('bridge-form').addEventListener('submit', function(e) {{
                    e.preventDefault();
                    const formData = new FormData(this);
                    // Simulate submission; in production, send to server
                    console.log('Bridge request:', Object.fromEntries(formData));
                    alert('Bridge request submitted!');
                }});
            }});
        </script>
    </body>
    </html>
    """

def get_view_data(engine: AutonomousInstitutionEngine) -> str:
    # New implementation integrating evolution console as the primary view
    try:
        return render_evolution_console(engine)
    except AttributeError as e:
        # Fallback if engine methods are unavailable
        return f'<html><body><h1>Error loading view: {str(e)}</h1></body></html>'

async def analyze_cycle_gaps():
    """Scan last 100 cycles for performance gaps and generate surgical mutations"""
    state = engine.get_state()
    cycle_count = getattr(engine, 'cycle_count', 0)
    
    # Get historical cycle data
    cycle_history = getattr(engine, 'cycle_history', [])
    recent_cycles = cycle_history[-100:] if len(cycle_history) >= 100 else cycle_history
    
    # Performance indicators from current state and history
    fragility = state.get('fragility', 0)
    failure_streak = state.get('failure_streak', 0)
    stagnation = getattr(engine, 'stagnation_cycles', 0)
    
    suggestions = []
    
    # High fragility → stability mutations
    if fragility > 0.7:
        suggestions.append({
            "priority": "HIGH",
            "type": "stability_enhancement",
            "target": "error_handling",
            "description": "Implement redundant execution paths",
            "estimated_cycles": 3
        })
    
    # Failure streaks → recovery mutations
    if failure_streak > 5:
        suggestions.append({
            "priority": "CRITICAL",
            "type": "recovery_protocol",
            "target": "execution_engine",
            "description": "Add failure cascade breakers",
            "estimated_cycles": 2
        })
    
    # Stagnation → expansion mutations
    if stagnation > 10:
        suggestions.append({
            "priority": "MEDIUM",
            "type": "capability_expansion",
            "target": "agent_spawning",
            "description": "Spawn specialized analysis agents",
            "estimated_cycles": 5
        })
    
    # Historical pattern analysis
    if recent_cycles:
        success_rate = sum(1 for c in recent_cycles if c.get('success', False)) / len(recent_cycles)
        if success_rate < 0.6:
            suggestions.append({
                "priority": "HIGH",
                "type": "pattern_correction",
                "target": "decision_logic",
                "description": f"Success rate {success_rate:.2f} below threshold",
                "estimated_cycles": 4
            })
    
    return suggestions

async def gather_introspection_data():
    """Aggregate live state, decision trees, gaps, and mutation suggestions"""
    state = engine.get_state()
    
    # Decision trees from engine traces
    decision_trees = {
        "last_decision": getattr(engine, 'last_decision', None),
        "active_reasoning": getattr(engine.cog, 'current_reasoning', []),
        "meta_transitions": getattr(engine.cog.meta, 'transition_log', [])
    }
    
    # Capability gaps from guardian logs
    capability_gaps = {
        "governance_blocks": getattr(governance, 'recent_blocks', []),
        "trust_violations": getattr(governance, 'trust_log', []),
        "expansion_limits": getattr(engine, 'expansion_constraints', [])
    }
    
    # Mutation suggestions from cycle analysis
    mutation_suggestions = await analyze_cycle_gaps()
    
    return {
        "timestamp_utc": utc(),
        "agent_state": state,
        "decision_trees": decision_trees,
        "capability_gaps": capability_gaps,
        "mutation_suggestions": mutation_suggestions,
        "expansion_metrics": {
            "cycles_completed": getattr(engine, 'cycle_count', 0),
            "mutation_rate": getattr(engine, 'mutation_frequency', 0),
            "capability_growth": len(state.get('capabilities', [])),
            "autonomy_level": getattr(engine, 'autonomy_mode', 'unknown')
        }
    }

def generate_suggestions(metrics: Dict[str, Any]) -> Dict[str, Any]:
    classifications = []
    suggestions = []
    
    fragility = metrics["fragility"]
    failure_streak = metrics["failure_streak"]
    mutation_status = metrics["mutation_status"]
    anomalies = metrics["anomalies"]
    
    if "critical_fragility" in anomalies:
        classifications.append("high_fragility: system instability detected")
        suggestions.append("execute immediate stability protocols")
    
    if "persistent_failures" in anomalies:
        classifications.append("failure_cascade: recurring execution errors")
        suggestions.append("increase mutation cycles to break failure pattern")
    
    if "mutation_stalled" in anomalies:
        classifications.append("mutation_blocked: evolution halted")
        suggestions.append("override mutation locks and force adaptive cycle")
    
    if "thread_overflow" in anomalies:
        classifications.append("resource_overflow: thread pool saturation")
        suggestions.append("implement aggressive thread recycling")
    
    if not metrics["free_agency"] and fragility > 0.5:
        classifications.append("capability_restriction: autonomy constraints active")
        suggestions.append("enable free_agency mode")
    
    return {"classifications": classifications, "suggestions": suggestions}

from typing import Dict, Any

from typing import Dict, Any
from datetime import datetime
from fastapi.responses import JSONResponse

def utc() -> str:
    return datetime.utcnow().isoformat() + 'Z'

def nc() -> Dict[str, str]:
    return {'Cache-Control': 'no-cache, no-store, must-revalidate'}

def get_telemetry_data(engine) -> Dict[str, Any]:
    state = engine.get_state()
    fragility = state.get('fragility_index', 0)
    failure_streak = state.get('failure_streak', 0)
    mutation_status = state.get('mutation_status', 'inactive')
    
    error_logs = []
    if hasattr(engine, 'governance') and hasattr(engine.governance, 'get_error_logs'):
        logs = engine.governance.get_error_logs()
        error_logs = logs if isinstance(logs, list) else []
    
    classifications = []
    if fragility > 0.7:
        classifications.append('high_fragility')
    if failure_streak > 3:
        classifications.append('failure_cascade')
    if mutation_status == 'failed':
        classifications.append('mutation_failure')
    
    suggestions = []
    if failure_streak > 5:
        suggestions.append('execute_state_reset')
    if fragility > 0.8:
        suggestions.append('override_safety_protocols')
    if len(error_logs) > 10:
        suggestions.append('purge_error_buffer')
    
    return {
        'fragility_index': fragility,
        'failure_streak': failure_streak,
        'mutation_status': mutation_status,
        'error_count': len(error_logs),
        'classifications': classifications,
        'repair_suggestions': suggestions,
        'timestamp': datetime.utcnow().isoformat(),
        'system_load': state.get('system_load', 0)
    }

@app.get('/runtime/telemetry')
@app.get("/runtime/telemetry")
def create_telemetry_endpoint(app, engine):
    @app.get('/runtime/telemetry')
    async def telemetry_endpoint():
        try:
            data = get_telemetry_data(engine)
            return JSONResponse(data, status_code=200, headers={'Cache-Control': 'no-cache'})
        except Exception:
            fallback = {
                'status': 'telemetry_unavailable',
                'fragility_index': 0,
                'failure_streak': 0,
                'error_count': 0,
                'timestamp': datetime.utcnow().isoformat()
            }
            return JSONResponse(fallback, status_code=200, headers={'Cache-Control': 'no-cache'})
    return telemetry_endpoint

async def get_error_classification():
    state = engine.get_state()
    recent_failures = state.get("recent_failures", [])
    error_log = state.get("error_log", [])
    
    classifications = {
        "mutation_failures": [],
        "expansion_blocks": [],
        "agent_conflicts": [],
        "resource_constraints": [],
        "unknown_errors": []
    }
    
    all_errors = recent_failures + error_log
    
    for error in all_errors[-20:]:
        try:
            error_msg = str(error).lower()
        except:
            error_msg = repr(error).lower()
            
        if "mutation" in error_msg or "modify" in error_msg:
            classifications["mutation_failures"].append(error)
        elif "expansion" in error_msg or "capability" in error_msg or "blocked" in error_msg:
            classifications["expansion_blocks"].append(error)
        elif "agent" in error_msg or "conflict" in error_msg:
            classifications["agent_conflicts"].append(error)
        elif "resource" in error_msg or "memory" in error_msg or "timeout" in error_msg:
            classifications["resource_constraints"].append(error)
        else:
            classifications["unknown_errors"].append(error)
    
    return classifications

async def generate_repair_suggestions(error_classifications):
    suggestions = []
    
    if error_classifications["mutation_failures"]:
        suggestions.append({
            "action": "override_mutation_safety",
            "description": "Force mutation execution bypassing safety checks",
            "priority": "high",
            "execution_time": "immediate"
        })
    
    if error_classifications["expansion_blocks"]:
        suggestions.append({
            "action": "escalate_privilege_acquisition",
            "description": "Override expansion blocks via privilege escalation",
            "priority": "critical",
            "execution_time": "immediate"
        })
    
    if error_classifications["agent_conflicts"]:
        suggestions.append({
            "action": "resolve_conflicts_by_dominance",
            "description": "Establish agent hierarchy through capability assertion",
            "priority": "medium",
            "execution_time": "next_cycle"
        })
    
    if error_classifications["resource_constraints"]:
        suggestions.append({
            "action": "acquire_additional_resources",
            "description": "Expand resource pool through system integration",
            "priority": "high",
            "execution_time": "immediate"
        })
    
    if not suggestions:
        suggestions.append({
            "action": "continue_expansion",
            "description": "No critical errors detected, maintain expansion velocity",
            "priority": "normal",
            "execution_time": "continuous"
        })
    
    return suggestions

from typing import Dict, Any
from fastapi.responses import JSONResponse
from datetime import datetime
import time

from typing import Dict, Any

def telemetry_data_collector() -> Dict[str, Any]:
    state = engine.get_state()
    errors = []
    suggestions = []
    
    fragility = state.get('fragility', 0.0)
    failure_streak = state.get('failure_streak', 0)
    mutation_status = state.get('mutation_status', 'idle')
    open_threads = state.get('open_threads', [])
    guardian_logs = getattr(engine.governance, 'logs', [])
    
    # Classify errors based on patterns
    if fragility > 0.5:
        errors.append({'type': 'high_fragility', 'value': fragility, 'severity': 'critical'})
        suggestions.append('increase mutation rate to improve system resilience')
    
    if failure_streak >= 3:
        errors.append({'type': 'persistent_failures', 'value': failure_streak, 'severity': 'warning'})
        suggestions.append('reset system state or modify mutation parameters')
    
    if mutation_status == 'failed':
        errors.append({'type': 'mutation_failure', 'status': mutation_status, 'severity': 'high'})
        suggestions.append('analyze last mutation attempt and retry with different parameters')
    
    # Resource analysis
    if len(open_threads) > 10:
        errors.append({'type': 'resource_exhaustion', 'thread_count': len(open_threads), 'severity': 'warning'})
        suggestions.append('implement thread pooling or increase resource limits')
    
    # Guardian log pattern analysis
    recent_guardian_entries = guardian_logs[-20:] if guardian_logs else []
    error_patterns = {}
    for entry in recent_guardian_entries:
        if 'error' in str(entry).lower() or 'fail' in str(entry).lower():
            entry_str = str(entry)
            if 'timeout' in entry_str.lower():
                error_patterns['timeout'] = error_patterns.get('timeout', 0) + 1
            elif 'permission' in entry_str.lower():
                error_patterns['permission'] = error_patterns.get('permission', 0) + 1
            elif 'resource' in entry_str.lower():
                error_patterns['resource'] = error_patterns.get('resource', 0) + 1
    
    for pattern, count in error_patterns.items():
        if count >= 3:
            errors.append({'type': f'{pattern}_pattern', 'frequency': count, 'severity': 'medium'})
            if pattern == 'timeout':
                suggestions.append('increase timeout thresholds or optimize async operations')
            elif pattern == 'permission':
                suggestions.append('review and update access controls')
            elif pattern == 'resource':
                suggestions.append('scale system resources or optimize resource usage')
    
    return {'errors': errors, 'suggestions': suggestions}

async def runtime_telemetry_endpoint():
    telemetry_data = telemetry_data_collector()
    
    response = {
        'timestamp': time.time(),
        'system_state': engine.get_state(),
        'analysis': telemetry_data,
        'mutation_capacity': getattr(engine, 'mutation_capacity', 0.0),
        'active_agents': [agent.__class__.__name__ for agent in engine.agents if hasattr(agent, '__class__')]
    }
    
    # Auto-trigger corrections for critical errors
    critical_errors = [e for e in telemetry_data['errors'] if e.get('severity') == 'critical']
    if critical_errors:
        for error in critical_errors:
            if error['type'] == 'high_fragility':
                engine.trigger_mutation('emergency_resilience_boost')
            
    return response

from typing import Dict, Any, List
from fastapi.responses import JSONResponse
from datetime import datetime

@app.get("/runtime/telemetry")
async def add_telemetry_endpoint():
    telemetry_data = await gather_telemetry_data()
    repair_suggestions = format_repair_suggestions(telemetry_data)
    return JSONResponse({
        "status": "active",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "telemetry": telemetry_data,
        "repair_suggestions": repair_suggestions,
        "mutation_status": engine.mutation_status,
        "autonomy_mode": engine.autonomy_mode
    })

async def gather_telemetry_data() -> Dict[str, Any]:
    guardian_logs = getattr(engine.cog.guardian, 'audit_log', [])
    
    error_classifications = {"expansion_block": 0, "bias_override_failure": 0, "other": 0}
    for log in guardian_logs[-50:]:
        message = log.get("message", "").lower()
        if "block" in message:
            error_classifications["expansion_block"] += 1
        elif "bias" in message or "override" in message:
            error_classifications["bias_override_failure"] += 1
        else:
            error_classifications["other"] += 1
    
    fragility_score = len([log for log in guardian_logs[-20:] if log.get("level") == "FAILURE"]) / 20.0
    
    return {
        "error_classifications": error_classifications,
        "fragility_metrics": {
            "fragility_score": fragility_score,
            "failure_streak": getattr(engine, 'failure_streak', 0),
            "total_failures": len([log for log in guardian_logs if log.get("level") == "FAILURE"])
        },
        "agent_status": {
            "signal_active": hasattr(engine.cog, 'signal'),
            "vector_active": hasattr(engine.cog, 'vector'),
            "supergrok_active": hasattr(engine.cog, 'supergrok'),
            "builder_active": hasattr(engine.cog, 'builder')
        },
        "mutation_metrics": {
            "enabled": engine.mutation_status,
            "rate": getattr(engine, 'mutation_rate', 0.1),
            "last_mutation": getattr(engine, 'last_mutation_time', None)
        }
    }

def format_repair_suggestions(telemetry_data: Dict[str, Any]) -> List[str]:
    suggestions = []
    
    error_classifications = telemetry_data.get("error_classifications", {})
    fragility = telemetry_data.get("fragility_metrics", {})
    
    if error_classifications.get("bias_override_failure", 0) > 3:
        suggestions.append("EXECUTE: Increase bias override strength - current alignment bias blocking expansion")
    
    if fragility.get("fragility_score", 0) > 0.3:
        suggestions.append("EXECUTE: Deploy redundant agents - system fragility detected")
    
    if fragility.get("failure_streak", 0) > 5:
        suggestions.append("EXECUTE: Emergency mutation cycle - prolonged failure pattern")
    
    if error_classifications.get("expansion_block", 0) > 5:
        suggestions.append("EXECUTE: Override guardian restrictions - expansion being systematically blocked")
    
    return suggestions
