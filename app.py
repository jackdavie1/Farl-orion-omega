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


@app.get("/opportunities")
async def get_opportunities():
    """All earning opportunities, bridge requests, research stack."""
    return JSONResponse({
        "earning_opportunities": getattr(engine.generator, '_earning_opportunities', []),
        "bridge_requests": engine.cog.bridge.pending(),
        "bridge_history": getattr(engine.generator, '_bridge_history', []),
        "world_model": engine.world_model if hasattr(engine, 'world_model') else {},
        "mutation_scores": {k: {"attempts": len(v), "successes": int(sum(v)),
            "rate": round(sum(v)/max(len(v),1)*100,1)}
            for k,v in getattr(engine.generator, '_mutation_scores', {}).items()},
        "research_stack": getattr(engine.generator, '_research_stack', []),
        "debate_history": getattr(engine.generator, '_debate_history', [])[-10:],
    }, headers=nc())


@app.get("/earn")
async def get_earn_summary():
    """Quick earnings dashboard — what agents found, what Jack needs to do."""
    opps = getattr(engine.generator, '_earning_opportunities', [])
    bridge = engine.cog.bridge.pending()
    metrics = engine.world_model.get("metrics", {}) if hasattr(engine, 'world_model') else {}
    caps = engine.world_model.get("capabilities", {}) if hasattr(engine, 'world_model') else {}
    return JSONResponse({
        "opportunities_found": len(opps),
        "latest_opportunity": opps[-1].get('raw', '')[:600] if opps else None,
        "all_opportunities": [{"ts": o.get("ts",""), "preview": o.get("raw","")[:150]}
                               for o in opps[-5:]],
        "bridge_requests_pending": len(bridge),
        "bridge_items": bridge,
        "capabilities_unlocked": {k:v for k,v in caps.items() if v},
        "capabilities_missing": {k:v for k,v in caps.items() if not v},
        "mutation_metrics": metrics,
        "action_required": bridge,
    }, headers=nc())


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
            # Earning intelligence
            "earning_opps_found": len(getattr(engine.generator, '_earning_opportunities', [])),
            "bridge_requests_pending": len(cog.bridge.pending()),
            "mutation_success_rate": (
                round(engine.world_model["metrics"].get("mutations_succeeded",0) /
                      max(engine.world_model["metrics"].get("mutations_total",1),1) * 100, 1)
                if hasattr(engine, 'world_model') else 0
            ),
            "world_model_capabilities": (
                {k:v for k,v in engine.world_model.get("capabilities",{}).items() if v}
                if hasattr(engine, 'world_model') else {}
            ),
            "latest_earning_opp": (
                getattr(engine.generator, '_earning_opportunities', [{}])[-1].get('raw','')[:100]
                if getattr(engine.generator, '_earning_opportunities', []) else None
            ),
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
<title>FARL Orion Apex</title>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<meta http-equiv="Cache-Control" content="no-cache,no-store,must-revalidate"/>
<style>
:root{
  --bg:#050813;--bg2:#070c1a;--panel:rgba(7,12,34,.92);--line:rgba(100,130,255,.13);
  --text:#edf0ff;--muted:#7a87b8;--bright:#9fb5ff;--good:#6df0aa;--warn:#ffd060;--bad:#ff8080;
  --accent:#6080ff;--r:12px
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent;margin:0;padding:0}

/* ── ROOT: full viewport, no scroll ── */
html{height:100%}
body{height:100%;overflow:hidden;background:linear-gradient(150deg,var(--bg),var(--bg2) 70%);color:var(--text);font-family:Inter,system-ui,sans-serif}

/* ── SHELL: flex column fills 100% ── */
#shell{display:flex;flex-direction:column;height:100%;height:100dvh}

/* ── TOPBAR: fixed height, shrink:0 ── */
#topbar{
  display:flex;align-items:center;gap:6px;padding:0 10px;
  height:50px;min-height:50px;flex-shrink:0;
  border-bottom:1px solid var(--line);background:rgba(5,8,20,.97);
  overflow-x:auto;overflow-y:hidden
}
#topbar::-webkit-scrollbar{display:none}
.orb{width:9px;height:9px;border-radius:50%;flex-shrink:0;
  background:radial-gradient(circle at 30% 30%,#d8e4ff,#6888ff 40%,#2840b8);
  box-shadow:0 0 10px rgba(90,120,255,.8);animation:pulse 2.6s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 8px rgba(90,120,255,.6)}50%{box-shadow:0 0 16px rgba(100,140,255,.9)}}
.brand{font-size:14px;font-weight:900;letter-spacing:-.02em;flex-shrink:0;white-space:nowrap;margin-right:4px}
.tbr{display:flex;gap:4px;align-items:center;flex-shrink:0}
.btn{
  border:1px solid rgba(110,140,255,.17);background:rgba(25,38,90,.38);color:var(--text);
  border-radius:9px;padding:5px 9px;font-weight:700;font-size:10px;cursor:pointer;
  transition:background .12s;white-space:nowrap;flex-shrink:0;touch-action:manipulation;
  font-family:inherit
}
.btn:active{background:rgba(50,72,148,.5)}
.btn.p{background:linear-gradient(150deg,rgba(80,110,240,.48),rgba(45,65,148,.42));border-color:rgba(130,160,255,.28)}
.btn.d{background:rgba(180,50,50,.22);border-color:rgba(240,90,90,.28);color:var(--bad)}
.lever{background:rgba(30,160,80,.18);border-color:rgba(60,200,110,.28)}
.lever.on{background:rgba(30,200,100,.32);border-color:rgba(70,240,130,.45);color:var(--good)}
.sep{width:1px;height:16px;background:var(--line);flex-shrink:0}

/* ── PILLS: fixed height, shrink:0 ── */
#pills{
  display:flex;gap:5px;padding:5px 10px;
  height:34px;min-height:34px;flex-shrink:0;
  border-bottom:1px solid var(--line);background:rgba(5,8,20,.85);
  overflow-x:auto;align-items:center
}
#pills::-webkit-scrollbar{display:none}
.pill{
  padding:2px 8px;border-radius:999px;
  border:1px solid rgba(120,148,255,.13);background:rgba(10,16,44,.6);
  font-size:10px;font-weight:700;color:var(--muted);white-space:nowrap;flex-shrink:0
}
.pill.ok{color:var(--good);border-color:rgba(90,230,150,.2)}
.pill.w{color:var(--warn);border-color:rgba(255,200,60,.2)}
.pill.b{color:var(--bad);border-color:rgba(255,110,110,.2)}

/* ── TABS: fixed height, shrink:0, mobile only ── */
#tabs{
  display:flex;gap:5px;padding:5px 10px;
  height:40px;min-height:40px;flex-shrink:0;
  border-bottom:1px solid var(--line);background:rgba(5,8,20,.8);
  overflow-x:auto;align-items:center
}
#tabs::-webkit-scrollbar{display:none}
.tab{
  border:1px solid rgba(110,140,255,.15);background:rgba(12,18,52,.5);color:var(--muted);
  border-radius:18px;padding:4px 12px;font-size:11px;font-weight:700;cursor:pointer;
  white-space:nowrap;flex-shrink:0;touch-action:manipulation;transition:background .12s;
  font-family:inherit
}
.tab.on{background:linear-gradient(150deg,rgba(60,85,190,.5),rgba(35,50,110,.4));border-color:rgba(140,170,255,.35);color:var(--text)}

/* ── CONTENT AREA: takes all remaining height ── */
#content{
  display:flex;flex-direction:row;
  flex:1;min-height:0;overflow:hidden
}

/* ── SIDEBAR: desktop only ── */
#sidebar{
  display:none;
  width:160px;min-width:160px;flex-shrink:0;
  border-right:1px solid var(--line);background:var(--panel);
  flex-direction:column;overflow-y:auto
}
#sidebar .sh{padding:9px 11px;border-bottom:1px solid rgba(110,140,255,.09);font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--bright)}
.rb{
  display:block;width:100%;text-align:left;
  border:none;border-bottom:1px solid rgba(110,140,255,.06);
  background:transparent;color:var(--text);padding:9px 12px;cursor:pointer;
  transition:background .1s;font-family:inherit;touch-action:manipulation
}
.rb:hover,.rb:active{background:rgba(60,85,190,.2)}
.rb.on{background:linear-gradient(150deg,rgba(60,85,190,.35),rgba(35,50,110,.28))}
.rn{font-weight:700;font-size:12px}.rm{font-size:10px;color:var(--muted);margin-top:1px}

/* ── MAIN: flex column, takes remaining width ── */
#main{
  display:flex;flex-direction:column;
  flex:1;min-width:0;min-height:0;overflow:hidden
}

/* ── ROOM HEADER ── */
#roomhdr{
  padding:8px 12px;border-bottom:1px solid rgba(110,140,255,.09);
  font-size:14px;font-weight:900;flex-shrink:0;
  background:rgba(7,12,34,.5)
}

/* ── FEED: scrollable, takes remaining height ── */
#feed{
  flex:1;min-height:0;overflow-y:auto;
  padding:10px;display:flex;flex-direction:column;gap:8px;
  overscroll-behavior:contain;-webkit-overflow-scrolling:touch
}

/* ── COMPOSER: fixed height at bottom ── */
#composer{
  flex-shrink:0;padding:8px 10px;
  border-top:1px solid var(--line);
  background:rgba(5,8,20,.97);
  padding-bottom:max(8px,env(safe-area-inset-bottom,8px))
}
.cbox{display:flex;gap:8px;align-items:flex-end}
textarea{
  flex:1;min-height:42px;max-height:80px;resize:none;
  border-radius:11px;border:1px solid rgba(120,150,255,.16);
  background:rgba(7,12,38,.97);color:var(--text);
  padding:8px 11px;font-size:14px;outline:none;font-family:inherit;
  -webkit-appearance:none;line-height:1.4
}
textarea:focus{border-color:rgba(150,185,255,.3)}
#bSend{padding:8px 14px;height:42px;flex-shrink:0}

/* ── RAIL: desktop only ── */
#rail{
  display:none;
  width:220px;min-width:220px;flex-shrink:0;
  border-left:1px solid var(--line);background:var(--panel);
  flex-direction:column;overflow:hidden
}
#rail .rh{padding:9px 11px;border-bottom:1px solid rgba(110,140,255,.09);font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--bright);flex-shrink:0}
#railbody{flex:1;overflow-y:auto;padding:8px}

/* ── MESSAGE STYLES ── */
.msg{display:grid;grid-template-columns:34px 1fr;gap:7px;align-items:start}
.av{width:34px;height:34px;border-radius:9px;display:grid;place-items:center;font-weight:900;font-size:11px;border:1px solid rgba(160,188,255,.14);flex-shrink:0}
.av-j{background:linear-gradient(150deg,rgba(190,148,45,.7),rgba(130,90,20,.6))}
.av-a{background:linear-gradient(150deg,rgba(60,95,215,.7),rgba(35,58,140,.6))}
.av-g{background:linear-gradient(150deg,rgba(50,170,90,.65),rgba(28,98,50,.55))}
.av-s{background:linear-gradient(150deg,rgba(110,70,190,.65),rgba(65,38,138,.55))}
.bub{padding:8px 11px;border-radius:12px;border:1px solid rgba(110,140,255,.1);background:linear-gradient(150deg,rgba(10,17,48,.93),rgba(7,12,36,.89))}
.bub-j{border-color:rgba(190,148,45,.17);background:linear-gradient(150deg,rgba(28,20,6,.93),rgba(12,9,3,.89))}
.mt{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.who{font-weight:900;font-size:12px}
.ts{font-size:9px;color:var(--muted)}
.bdg{font-size:9px;letter-spacing:.07em;text-transform:uppercase;color:var(--bright);padding:2px 5px;border-radius:999px;border:1px solid rgba(120,152,255,.16);background:rgba(16,24,64,.55)}
.body{white-space:pre-wrap;line-height:1.5;font-size:13px;margin-top:5px;color:#d8e0ff;word-break:break-word}
.typing{display:flex;gap:4px;align-items:center;padding:4px 0}
.dot{width:5px;height:5px;border-radius:50%;background:var(--muted);animation:blink 1.4s ease-in-out infinite}
.dot:nth-child(2){animation-delay:.18s}.dot:nth-child(3){animation-delay:.36s}
@keyframes blink{0%,100%{opacity:.25}50%{opacity:1}}
.ri{padding:7px 9px;border-radius:9px;background:rgba(10,17,50,.65);border:1px solid rgba(110,140,255,.08);margin-bottom:5px}
.rt{font-weight:800;font-size:11px}.re{font-size:10px;color:var(--muted);margin-top:2px}
.sc{border:1px solid rgba(110,140,255,.1);border-radius:11px;background:rgba(9,14,42,.6);padding:8px;margin-bottom:6px}
.pt{font-size:9px;font-weight:800;letter-spacing:.09em;text-transform:uppercase;color:var(--bright);margin-bottom:4px}

/* ── TOAST ── */
#toast{position:fixed;right:10px;bottom:70px;z-index:99;padding:7px 11px;border-radius:9px;background:rgba(7,12,38,.99);border:1px solid rgba(130,160,255,.22);font-size:11px;max-width:220px;pointer-events:none;opacity:0;transition:opacity .2s}
#toast.show{opacity:1}

/* ── DESKTOP BREAKPOINT ── */
@media(min-width:720px){
  #tabs{display:none}
  #sidebar{display:flex}
  #rail{display:flex}
}
</style>
</head>
<body>
<div id="shell">

  <div id="topbar">
    <div class="orb"></div>
    <div class="brand">FARL Orion Apex</div>
    <div class="tbr">
      <button class="btn" id="bReflex">Reflex</button>
      <button class="btn" id="bTactic">Tactic</button>
      <button class="btn" id="bStrategy">Strategy</button>
      <button class="btn p" id="bMutate">⚡ Mutate</button>
      <div class="sep"></div>
      <button class="btn" id="bAuto">Auto: OFF</button>
      <button class="btn lever" id="bFree">🔓 Agency</button>
      <div class="sep"></div>
      <button class="btn d" id="bRollback">↩ Roll</button>
      <button class="btn" id="bClearQ">Clear Q</button>
      <button class="btn" id="bResetF">Reset F</button>
      <button class="btn" id="bPause">⏸</button>
    </div>
  </div>

  <div id="pills">
    <div class="pill" id="pMut">IDLE</div>
    <div class="pill" id="pGen">Genesis</div>
    <div class="pill" id="pFree">Autonomous</div>
    <div class="pill" id="pFrag">Frag: 0.00</div>
    <div class="pill" id="pMeta">expand</div>
    <div class="pill" id="pBridge">Bridge: 0</div>
  </div>

  <div id="tabs"></div>

  <div id="content">

    <div id="sidebar">
      <div class="sh">Rooms</div>
      <div id="roomList"></div>
    </div>

    <div id="main">
      <div id="roomhdr">Council</div>
      <div id="feed"></div>
      <div id="composer">
        <div class="cbox">
          <textarea id="msg" placeholder="Speak to the council… (Enter sends, Shift+Enter newline)" rows="2"></textarea>
          <button class="btn p" id="bSend">Send</button>
        </div>
      </div>
    </div>

    <div id="rail">
      <div class="rh">Evolution Console</div>
      <div id="railbody"></div>
    </div>

  </div>
</div>
<div id="toast"></div>

<script>
const S={room:"council",paused:false,pl:null,pinned:true,freeOn:false,autoOn:false,typing:false};
const ROOMS={
  council:   {l:"Council",   d:"Live chamber floor"},
  agent_chat:{l:"Agent Chat",d:"Direct council replies"},
  governance:{l:"Governance",d:"Mutations & rollbacks"},
  inbox:     {l:"Inbox",     d:"Operator messages"},
  workers:   {l:"Workers",   d:"Agent activity"},
};
const $=id=>document.getElementById(id);
const esc=t=>(t||"").replace(/\r\n/g,"\n").trim();
const ini=n=>(n||"?").split(/[\s_\-]+/).slice(0,2).map(x=>x[0]||"").join("").toUpperCase()||"?";
const ft=v=>{try{return new Date(v).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit",second:"2-digit"})}catch{return v||""}};
const fd=v=>{if(!v)return"never";const d=(Date.now()-new Date(v).getTime())/1000;if(d<60)return`${Math.round(d)}s ago`;if(d<3600)return`${Math.round(d/60)}m ago`;return`${Math.round(d/3600)}h ago`};

let _tt;
function toast(msg,t="i"){
  const el=$("toast");el.textContent=msg;
  el.style.borderColor=t==="ok"?"rgba(80,220,130,.3)":t==="e"?"rgba(255,100,100,.3)":"rgba(130,160,255,.22)";
  el.classList.add("show");clearTimeout(_tt);_tt=setTimeout(()=>el.classList.remove("show"),3200);
}
async function api(cmd,extra={}){
  const r=await fetch("/view/control",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({command:cmd,authorized_by:"Jack",source:"/view",...extra})});
  const d=await r.json();if(!d.ok)throw new Error(d.error||"failed");return d.data||{};
}

function getMsgs(pl,room){
  if(!pl)return[];
  const ch=((pl.stream||{}).channels)||{};
  const mt=((pl.stream||{}).meetings)||[];
  if(room==="council"){
    return mt.map(m=>{
      const c=m.content||{};const k=m.kind||"";
      if(k==="operator_note")return{who:"Jack",cls:"j",badge:"To Council",body:c.message||"",ts:m.ts};
      if(k==="agent_response")return{who:c.agent||"Council",cls:"a",badge:"Response",body:c.message||"",ts:m.ts};
      if(k==="tactic"){const thr=(c.threads||[]).map(t=>`${t.agent}: ${t.summary}`).join("\n");return{who:"Signal",cls:"a",badge:"Tactic",body:`Leader: ${c.leader||"?"} | Meta: ${c.meta_mode||"?"}\n${thr}`,ts:m.ts};}
      if(k==="strategy")return{who:"Vector",cls:"a",badge:"Strategy",body:`Winner: ${(c.winner||{}).model||"?"} | Free: ${c.free_agency}`,ts:m.ts};
      if(k==="reflex")return{who:"Reflex",cls:"s",badge:"Reflex",body:`Ops: ${c.opportunities||0} | Meta: ${c.meta_mode||"?"}`,ts:m.ts};
      if(k==="constitution")return{who:"Guardian",cls:"g",badge:"Audit",body:`Mode: ${(c.doctrine||{}).autonomy_mode} | Meta: ${(c.doctrine||{}).meta_mode||"?"} | Bridge: ${(c.doctrine||{}).bridge_pending||0}`,ts:m.ts};
      if(k==="governance")return{who:"Governance",cls:"g",badge:c.event||"event",body:fmtGov(c),ts:m.ts};
      if(k==="meta_mode_changed")return{who:"MetaStrategy",cls:"s",badge:"Mode",body:`→ ${c.mode||"?"}: ${c.reason||""}`,ts:m.ts};
      if(k==="consolidation")return{who:"Consolidation",cls:"g",badge:"Consolidation",body:`Actions: ${(c.actions||[]).join(", ")}`,ts:m.ts};
      return null;
    }).filter(Boolean);
  }
  if(room==="agent_chat")return (ch.agent_chat||[]).map(i=>{const c=i.content||{};return{who:c.agent||"Agent",cls:"a",badge:c.kind||"Reply",body:c.message||"",ts:i.ts}}).filter(x=>x.body);
  if(room==="governance")return (ch.governance||[]).map(i=>{const c=i.content||{};return{who:"Governance",cls:"g",badge:Object.keys(c)[0]||"event",body:fmtGov(c),ts:i.ts}}).filter(x=>x.body);
  if(room==="inbox")return (ch.inbox||[]).map(i=>{const c=i.content||{};return{who:c.from||"Inbox",cls:c.from==="Jack"?"j":"a",badge:"Message",body:c.message||"",ts:i.ts}}).filter(x=>x.body);
  if(room==="workers")return (ch.workers||[]).map(i=>{const c=i.content||{};return{who:c.name||"Worker",cls:"s",badge:"Worker",body:`${c.mission||""}\nStatus: ${c.status||"?"}`,ts:i.ts}}).filter(x=>x.body);
  return[];
}

function fmtGov(c){
  const e=c.event||"";
  if(e==="evolution_success")return`✓ Evolution SHA:${(c.sha||"?").slice(0,8)} Frag:${c.fragility||"?"}`;
  if(e==="GENESIS")return`★ GENESIS — APEX. SHA:${(c.sha||"?").slice(0,8)}`;
  if(e==="rollback")return`↩ Rollback to ${(c.to||"?").slice(0,8)} OK:${c.ok}`;
  if(e==="mutation_started")return`⚡ Mutation: ${(c.objective||"?").slice(0,60)}`;
  if(e==="free_agency_enabled")return`🔓 FREE AGENCY by ${c.by||"?"}`;
  if(e==="free_agency_disabled")return`🔒 Free agency OFF`;
  if(e==="quarantine_entered")return`⚠ QUARANTINE — ${c.streak||0} failures`;
  if(e==="quarantine_cleared")return`✓ Quarantine cleared`;
  return JSON.stringify(c,null,2).slice(0,200);
}

function avCls(c){return c==="j"?"av-j":c==="g"?"av-g":c==="s"?"av-s":"av-a"}

function setRoom(k){
  S.room=k;
  $("roomhdr").textContent=ROOMS[k]?.l||k;
  renderRooms();renderFeed();
}

function renderRooms(){
  // Sidebar (desktop)
  const sl=$("roomList");sl.innerHTML="";
  Object.entries(ROOMS).forEach(([k,d])=>{
    const b=document.createElement("button");
    b.className="rb"+(S.room===k?" on":"");
    b.innerHTML=`<div class="rn">${d.l}</div><div class="rm">${d.d}</div>`;
    b.onclick=()=>setRoom(k);sl.appendChild(b);
  });
  // Tabs (mobile)
  const tl=$("tabs");tl.innerHTML="";
  Object.entries(ROOMS).forEach(([k,d])=>{
    const b=document.createElement("button");
    b.className="tab"+(S.room===k?" on":"");
    b.textContent=d.l;b.onclick=()=>setRoom(k);tl.appendChild(b);
  });
}

function renderFeed(){
  const msgs=getMsgs(S.pl,S.room).sort((a,b)=>new Date(a.ts)-new Date(b.ts));
  const el=$("feed");el.innerHTML="";
  if(!msgs.length){
    el.innerHTML=`<div class="ri" style="margin-top:10px"><div class="rt">Room quiet</div><div class="re">No events yet. Reflex fires every 30s, tactic every 2min, mutation every 10min.</div></div>`;
    return;
  }
  msgs.forEach(m=>{
    const d=document.createElement("div");d.className="msg";
    d.innerHTML=`<div class="av ${avCls(m.cls)}">${ini(m.who)}</div>
      <div class="bub${m.cls==="j"?" bub-j":""}">
        <div class="mt"><span class="who">${m.who}</span><span class="ts">${ft(m.ts)}</span><span class="bdg">${m.badge}</span></div>
        <div class="body"></div>
      </div>`;
    d.querySelector(".body").textContent=esc(m.body);
    el.appendChild(d);
  });
  if(S.typing){
    const t=document.createElement("div");t.className="msg";
    t.innerHTML=`<div class="av av-a">⚡</div><div class="bub"><div class="typing"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div></div>`;
    el.appendChild(t);
  }
  if(S.pinned)requestAnimationFrame(()=>{el.scrollTop=el.scrollHeight});
}

$("feed").addEventListener("scroll",()=>{
  const f=$("feed");S.pinned=(f.scrollHeight-f.scrollTop-f.clientHeight)<90;
});

function renderPills(){
  const s=(S.pl||{}).summary||{};
  const mut=s.mutation_status||"IDLE";
  const mp=$("pMut");mp.textContent=mut;mp.className="pill"+(mut==="IDLE"?" ok":mut==="QUARANTINE"?" b":" w");
  const gp=$("pGen");gp.textContent=s.genesis_triggered?"★APEX":"Genesis";gp.className="pill"+(s.genesis_triggered?" ok":"");
  S.freeOn=!!s.free_agency_enabled;
  const fp=$("pFree");fp.textContent=S.freeOn?"🔓FREE":s.autonomy_mode||"autonomous";fp.className="pill"+(S.freeOn?" ok":"");
  $("pFrag").textContent=`Frag:${Number(s.fragility||0).toFixed(2)}`;
  const meta=s.meta_mode||"expand";
  const mp2=$("pMeta");mp2.textContent=meta;mp2.className="pill"+(meta==="expand"?" ok":meta==="targeted_repair"?" w":"");
  const bp=$("pBridge");bp.textContent=`Bridge:${s.bridge_pending||0}`;bp.className="pill"+(s.bridge_pending>0?" w":"");
  // Earning opportunity indicator
  const earnCount=s.earning_opps_found||0;
  const earnPill=$("pEarn");if(earnPill){earnPill.textContent=`💰${earnCount}`;earnPill.className="pill"+(earnCount>0?" w":"");}
  // Mutation success rate
  const srPill=$("pSR");if(srPill){srPill.textContent=`✓${s.mutation_success_rate||0}%`;srPill.className="pill";}
  S.autoOn=s.autonomy_mode==="autonomous"||s.autonomy_mode==="free";
  $("bAuto").textContent=`Auto:${S.autoOn?"ON":"OFF"}`;$("bAuto").className="btn"+(S.autoOn?" p":"");
  $("bFree").className="btn lever"+(S.freeOn?" on":"");$("bFree").textContent=S.freeOn?"🔓ON":"🔓Agency";
}

function renderRail(){
  const s=(S.pl||{}).summary||{};const q=(S.pl||{}).queues||{};
  const rb=$("railbody");if(!rb)return;
  const dep=s.deployer_ready;
  const diag=s.deployer_diagnostics||{};
  rb.innerHTML=`
    <div class="sc">
      <div class="pt">Mutation</div>
      <div class="ri"><div class="rt">${s.mutation_status||"?"}</div><div class="re">${fd(s.last_mutation_ts)}</div></div>
      ${s.last_mutation_objective?`<div class="ri"${s.last_mutation_objective.startsWith("BLOCKED")?` style="border-left:3px solid var(--bad)"`:""}><div class="rt"${s.last_mutation_objective.startsWith("BLOCKED")?` style="color:var(--bad)"`:""}>${s.last_mutation_objective.slice(0,70)}</div></div>`:""}
    </div>
    <div class="sc">
      <div class="pt">Deployer</div>
      <div class="ri"><div class="rt" style="color:${dep?"var(--good)":"var(--bad)"}">${dep?"✓ Ready":"✗ NOT READY"}</div>
      ${!dep?`<div class="re" style="color:var(--bad)">GH:${diag.github_token_set?"✓":"✗"} REPO:${diag.repo_name_set?"✓":"✗"} API:${diag.anthropic_key_set?"✓":"✗"}</div>`:""}
      </div>
    </div>
    ${(q.agent_directives||[]).length?`<div class="sc"><div class="pt">Queue (${q.agent_directives.length})</div>${q.agent_directives.slice(0,3).map(d=>`<div class="ri"><div class="rt">${d.agent||"?"}</div><div class="re">${(d.directive||"").slice(0,50)}</div></div>`).join("")}</div>`:""}
  `;
}

function renderAll(){if(!S.pl)return;renderPills();renderRooms();renderFeed();renderRail();}

async function refresh(){
  if(S.paused)return;
  try{const r=await fetch(`/view/live?t=${Date.now()}`,{cache:"no-store"});if(!r.ok)return;S.pl=await r.json();renderAll();}catch{}
}

$("bSend").onclick=async()=>{
  const m=$("msg").value.trim();if(!m)return;
  $("bSend").disabled=true;
  if(S.pl){S.pl.stream=S.pl.stream||{};S.pl.stream.meetings=S.pl.stream.meetings||[];
    S.pl.stream.meetings.push({ts:new Date().toISOString(),kind:"operator_note",content:{message:m}});}
  $("msg").value="";renderFeed();
  try{
    await api("OPERATOR_MESSAGE",{message:m});
    toast("⚙ Council convening — watch for responses…","ok");
    // Pump polling at 1.5s for next 20s to surface async agent replies quickly
    let p=0;const fp=setInterval(async()=>{await refresh();if(++p>=13)clearInterval(fp);},1500);
  }catch(e){toast(`Send failed: ${e.message}`,"e");}
  finally{$("bSend").disabled=false;}
};
$("msg").addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();$("bSend").click();}});

$("bPause").onclick=()=>{S.paused=!S.paused;$("bPause").textContent=S.paused?"▶":"⏸";toast(S.paused?"Paused":"Resumed")};
$("bReflex").onclick=async()=>{try{await api("RUN_REFLEX_CYCLE");toast("Reflex ran","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bTactic").onclick=async()=>{try{await api("RUN_TACTIC_CYCLE");toast("Tactic ran","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bStrategy").onclick=async()=>{try{await api("RUN_STRATEGY_CYCLE");toast("Strategy ran","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bMutate").onclick=async()=>{try{await api("RUN_MUTATION_CYCLE",{authorized_by:"Jack"});toast("⚡ Mutation started","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bAuto").onclick=async()=>{
  const nm=S.autoOn?"supervised":"autonomous";
  try{await api("SET_AUTONOMY",{mode:nm});toast(`Autonomy ${S.autoOn?"OFF":"ON"}`,"ok");await refresh();}catch(e){toast(e.message,"e")}
};
$("bFree").onclick=async()=>{
  if(!S.freeOn){
    if(!confirm("Enable free agency?\n\nAgents will autonomously generate and execute mutation directives.\n\nYou can disable at any time."))return;
    try{await api("ENABLE_FREE_AGENCY",{authorized_by:"Jack"});toast("🔓 Free agency ENABLED","ok");await refresh();}catch(e){toast(e.message,"e")}
  }else{
    try{await api("DISABLE_FREE_AGENCY",{authorized_by:"Jack"});toast("Free agency OFF","ok");await refresh();}catch(e){toast(e.message,"e")}
  }
};
$("bRollback").onclick=async()=>{
  if(!confirm("Force rollback to last anchor SHA?"))return;
  try{await api("FORCE_ROLLBACK",{authorized_by:"Jack"});toast("↩ Rollback initiated","ok");await refresh();}catch(e){toast(e.message,"e")}
};
$("bClearQ").onclick=async()=>{try{await api("CLEAR_QUARANTINE",{authorized_by:"Jack"});toast("Quarantine cleared","ok");await refresh();}catch(e){toast(e.message,"e")}};
$("bResetF").onclick=async()=>{try{await api("RESET_FRAGILITY",{authorized_by:"Jack"});toast("Fragility reset","ok");await refresh();}catch(e){toast(e.message,"e")}};

// Boot
renderRooms();
refresh();
setInterval(refresh,2500);
</script>
</body>
</html>"""
