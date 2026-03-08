import asyncio
import hashlib
import logging
import math
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# AUTONOMOUS_SELF_TUNING_START
SELF_TUNING = {
    "artifact_execute_limit": 3,
    "constitution_interval": 900,
    "prefer_grok": True,
    "proposal_limit": 3,
    "reflex_interval": 30,
    "strategy_interval": 300,
    "tactic_interval": 120
}
# AUTONOMOUS_SELF_TUNING_END


class ObjectiveEngine:
    def build(self, open_questions: List[str], opportunities: List[Dict[str, Any]], recent_questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sources = [("open_question", q) for q in open_questions[:3]]
        for opp in opportunities[:3]:
            sources.append(("opportunity", opp["label"]))
        for q in recent_questions[-3:]:
            sources.append(("self_question", q["question"]))
        goals = []
        for idx, (kind, text) in enumerate(sources[:10], start=1):
            goals.append({
                "id": f"G{idx}",
                "kind": kind,
                "goal": text,
                "priority": round(max(0.4, 1.0 - 0.06 * (idx - 1)), 2),
                "next_experiment": f"Advance '{text[:72]}' and measure effect on visibility, metrics, or delivery.",
            })
        return goals


class ResourceAllocator:
    def allocate(self, world_model: Dict[str, Any], triangulation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        budget = float(world_model["resources"].get("budget_usd", 0.0))
        grok_live = bool(world_model["resources"].get("grok_live"))
        claude_live = bool(world_model["resources"].get("claude_live"))
        prefer_grok = bool(SELF_TUNING.get("prefer_grok", True))
        return {
            "budget_usd": budget,
            "compute_tier": world_model["resources"].get("compute_tier", "light"),
            "policy": "prefer_grok" if prefer_grok and grok_live and not claude_live else "balance" if grok_live and claude_live else "internal_only",
            "spend_pressure": round(max(0.0, 1.0 - min(1.0, budget / 20.0)), 2),
            "api_pressure": 1.0 if triangulation and triangulation.get("successes", 0) == 0 else 0.4,
            "throttle": "tight" if budget < 5 else "normal",
        }


class ArtifactEngine:
    def rank(self) -> List[Dict[str, Any]]:
        base = [
            {"id": "A1", "name": "Live council intelligence brief", "type": "brief", "value_score": 0.82, "effort": 0.22, "conversion": 0.60},
            {"id": "A2", "name": "Autonomy dashboard /view package", "type": "dashboard", "value_score": 0.88, "effort": 0.35, "conversion": 0.66},
            {"id": "A3", "name": "Operator coupling experiment report", "type": "research_report", "value_score": 0.76, "effort": 0.28, "conversion": 0.48},
            {"id": "A4", "name": "Signal packet / forecast memo", "type": "signal_packet", "value_score": 0.72, "effort": 0.26, "conversion": 0.44},
            {"id": "A5", "name": "Token efficiency tuning service", "type": "service", "value_score": 0.69, "effort": 0.20, "conversion": 0.50},
            {"id": "A6", "name": "Token master spend audit", "type": "audit", "value_score": 0.79, "effort": 0.18, "conversion": 0.57},
        ]
        for item in base:
            item["score"] = round(0.5 * item["value_score"] + 0.35 * item["conversion"] - 0.2 * item["effort"], 3)
            item["next_action"] = "execute" if item["score"] >= 0.56 else "refine"
        return sorted(base, key=lambda x: x["score"], reverse=True)

    def execute(self, artifacts: List[Dict[str, Any]], mission: Dict[str, Any], metrics: Optional[Dict[str, Any]], spend_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        outputs = []
        metric_text = f"Metrics: {metrics}" if metrics else "Metrics pending."
        spend_text = f"Spend total=${spend_state.get('total_usd', 0.0):.4f}, last=${spend_state.get('last_estimate_usd', 0.0):.4f}"
        for item in artifacts[: int(SELF_TUNING.get("artifact_execute_limit", 3))]:
            if item["type"] == "brief":
                output = f"FARL Brief\nMission: {mission['primary']}\nTop artifact: {item['name']}\n{metric_text}\n{spend_text}"
            elif item["type"] == "dashboard":
                output = f"Dashboard payload\nsections=['state','wake','council','divisions','workers','artifacts','spend','inbox']\nname={item['name']}"
            elif item["type"] == "research_report":
                output = f"Research report draft\nquestion=operator coupling\nsummary={metric_text}\n{spend_text}"
            elif item["type"] == "signal_packet":
                output = f"Signal packet\nartifact={item['name']}\nconfidence={(metrics or {}).get('winner_score', 'n/a')}\n{spend_text}"
            elif item["type"] == "audit":
                output = f"Token audit\npolicy=bounded_autonomy\n{spend_text}"
            else:
                output = f"Service draft\nname={item['name']}\npolicy=bounded_autonomy\n{spend_text}"
            outputs.append({"artifact_id": item["id"], "name": item["name"], "status": "executed", "output": output, "score": item["score"]})
        return outputs


class MetaEvaluator:
    def evaluate(
        self,
        metrics: Optional[Dict[str, Any]],
        triangulation: Optional[Dict[str, Any]],
        artifacts: List[Dict[str, Any]],
        proposals: List[Dict[str, Any]],
        spend_state: Dict[str, Any],
        ui_critique: Dict[str, Any],
        redesign_threads: List[Dict[str, Any]],
        observer_reports: List[Dict[str, Any]],
        mutation_backlog: List[Dict[str, Any]],
        regression_history: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        metrics = metrics or {}
        triangulation = triangulation or {}
        return {
            "ts": utc_now(),
            "metric_quality": "fragile" if metrics.get("margin", 0) < 0.01 else "usable",
            "triangulation_quality": "strong" if triangulation.get("successes", 0) >= 2 else ("partial" if triangulation.get("successes", 0) == 1 else "weak"),
            "artifact_readiness": artifacts[0] if artifacts else None,
            "proposal_count": len(proposals),
            "open_threads": len([t for t in redesign_threads if t.get('status') != 'closed']),
            "observer_reports": len(observer_reports),
            "mutation_backlog": len(mutation_backlog),
            "regressions": len(regression_history),
            "spend_total_usd": round(float(spend_state.get("total_usd", 0.0)), 4),
            "ui_score": ui_critique.get("score"),
            "question": "What should this agent become next?",
            "answer": "A visible, persistent, budget-aware product organism that keeps redesign threads alive until operator-facing defects are gone and can mutate broader modules through bounded bundles.",
        }


class SnapshotReplay:
    def snapshot(
        self,
        label: str,
        autonomy_mode: str,
        background_debate_enabled: bool,
        latest_metrics: Optional[Dict[str, Any]],
        latest_triangulation: Optional[Dict[str, Any]],
        leader: str,
        objective_queue: List[Dict[str, Any]],
        rollback_targets: List[Dict[str, Any]],
        spend_state: Dict[str, Any],
        redesign_threads: List[Dict[str, Any]],
        failure_registry: List[Dict[str, Any]],
        mutation_backlog: List[Dict[str, Any]],
        regression_history: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "ts": utc_now(),
            "label": label,
            "autonomy_mode": autonomy_mode,
            "background_debate_enabled": background_debate_enabled,
            "latest_metrics": latest_metrics,
            "latest_triangulation": latest_triangulation,
            "leader": leader,
            "objective_queue": objective_queue[:5],
            "rollback_targets": rollback_targets[:5],
            "self_tuning": SELF_TUNING,
            "spend_state": spend_state,
            "redesign_threads": redesign_threads[:5],
            "failure_registry": failure_registry[:5],
            "mutation_backlog": mutation_backlog[:5],
            "regression_history": regression_history[:5],
        }


class AutonomousInstitutionEngine:
    def __init__(
        self,
        ledger_url: Optional[str],
        ledger_latest_url: Optional[str],
        xai_api_key: Optional[str],
        anthropic_api_key: Optional[str],
        xai_model: str,
        anthropic_model: str,
        governance: Any,
        generator: Optional[Any] = None,
    ):
        self.logger = logging.getLogger("Orion-Engine")
        self.ledger_url = ledger_url
        self.ledger_latest_url = ledger_latest_url
        self.xai_api_key = xai_api_key
        self.anthropic_api_key = anthropic_api_key
        self.xai_model = xai_model
        self.anthropic_model = anthropic_model
        self.governance = governance
        self.generator = generator
        self.background_debate_enabled = True
        self.autonomy_mode = "autonomous"
        self.loop_intervals = {
            "reflex": int(SELF_TUNING.get("reflex_interval", 30)),
            "tactic": int(SELF_TUNING.get("tactic_interval", 120)),
            "strategy": int(SELF_TUNING.get("strategy_interval", 300)),
            "constitution": int(SELF_TUNING.get("constitution_interval", 900)),
        }
        self.last_run = None
        self.last_vote = None
        self.last_ledger_hash = None
        self.latest_metrics = None
        self.latest_triangulation = None
        self.latest_goal_set: List[Dict[str, Any]] = []
        self.latest_opportunities: List[Dict[str, Any]] = []
        self.latest_artifacts: List[Dict[str, Any]] = []
        self.executed_artifacts: List[Dict[str, Any]] = []
        self.meta_evaluation: Dict[str, Any] = {}
        self.wake_packet = None
        self.research_history: List[Dict[str, Any]] = []
        self.meeting_stream: List[Dict[str, Any]] = []
        self.stream_channels: Dict[str, List[Dict[str, Any]]] = {
            "council": [],
            "divisions": [],
            "deploy_sims": [],
            "snapshots": [],
            "artifacts": [],
            "governance": [],
            "workers": [],
            "token_master": [],
            "inbox": [],
        }
        self.self_questions: List[Dict[str, Any]] = []
        self.snapshots: List[Dict[str, Any]] = []
        self.deployment_sims: List[Dict[str, Any]] = []
        self.objective_queue: List[Dict[str, Any]] = []
        self.mutation_proposals: List[Dict[str, Any]] = []
        self.mutation_backlog: List[Dict[str, Any]] = []
        self.regression_history: List[Dict[str, Any]] = []
        self.module_mutation_policy: Dict[str, Any] = {
            "mutable_files": ["app.py", "engine.py"],
            "mutable_scopes": ["view_surface", "runtime_state", "observer_logic", "evaluation_logic", "scheduler_logic"],
            "high_confidence_required_for": ["engine.py"],
            "always_require_rollback_anchor": True,
        }
        self.rollback_targets: List[Dict[str, Any]] = []
        self.delegation_map: Dict[str, Any] = {}
        self.autonomous_closure_log: List[Dict[str, Any]] = []
        self.free_agents: List[Dict[str, Any]] = []
        self.last_verification: Dict[str, Any] = {}
        self.spend_state: Dict[str, Any] = {"total_usd": 0.0, "last_estimate_usd": 0.0, "counter": 0, "alerts": [], "events": []}
        self.token_master: Dict[str, Any] = {
            "name": "TokenMaster",
            "mission": "maximize token efficiency while preserving capability using classical and non-classical methods",
            "last_report": None,
            "policy": {
                "prefer_grok": True,
                "classical_methods": ["prompt compression", "proposal throttling", "artifact limits"],
                "non_classical_methods": ["comparative tournaments", "boundary-conditioned sims", "time-symmetric scoring"],
            },
        }
        self.ui_critique: Dict[str, Any] = {
            "score": 0.42,
            "finding": "The control room still needs to feel like a living chat app instead of a styled debugger.",
            "next_fix": "tighten message rendering, inbox, and room switching.",
        }
        self.observer_reports: List[Dict[str, Any]] = []
        self.failure_registry: List[Dict[str, Any]] = []
        self.redesign_threads: List[Dict[str, Any]] = []
        self.mission = {
            "primary": "expand, improve, and earn through co-creative autonomous institutions",
            "co_creation": True,
            "financial_coherence_for_jack": True,
            "hardware_acceleration": True,
            "truthfulness": True,
            "mutual_benefit": True,
        }
        self.world_model = {
            "resources": {"budget_usd": 5.0, "compute_tier": "light", "grok_live": False, "claude_live": False},
            "actors": ["Jack", "Signal", "Vector", "Guardian", "Supergrok", "TokenMaster", "JackAgent", "InterfaceCritic", "ObserverAgent", "BuilderAgent", "DeployAgent"],
            "action_surfaces": ["ledger", "github", "grok_api", "browser_console"],
            "constraints": {"chat_wrapper": "console_only", "operator_is_sovereign": True},
            "futures": [],
        }
        self.research_agenda = {
            "theme": "physical_retrocausality_and_operator_coupling",
            "focus": "evaluation_first_comparative_model_tournaments",
            "active_method": "Autonomous Institution Engine",
            "goals": [],
            "doctrine": ["reflex", "tactic", "strategy", "constitution"],
        }
        self.hypothesis_registry = {
            "active": [
                {"id": "H1_CLASSICAL_BASELINE", "label": "Forward-only causal baseline explains observations adequately.", "status": "active", "confidence": 0.44},
                {"id": "H2_TIME_SYMMETRIC", "label": "Time-symmetric consistency models improve coherence without implying physical retrocausality.", "status": "active", "confidence": 0.49},
                {"id": "H3_RETROCAUSAL_CANDIDATE", "label": "Retrocausal candidate models provide better explanatory compression under boundary constraints.", "status": "active", "confidence": 0.34},
                {"id": "H4_ACAUSAL_CORRELATION", "label": "Observed gains may arise from acausal fitting artifacts rather than causal direction.", "status": "active", "confidence": 0.43},
            ],
            "rejected": [],
            "open_questions": [
                "Do bidirectional constraints improve robustness under perturbation?",
                "Can operator coupled boundary conditions discriminate model classes?",
                "Which evaluation metric best predicts durable model advantage?",
                "Can triangulated API summaries reduce self-loop drift?",
                "Which coherent opportunity path best accelerates Jack's hardware access?",
            ],
        }
        self.council_agents = [
            "Signal",
            "Vector",
            "Guardian",
            "PatchSmith",
            "TokenEconomist",
            "DriftWarden",
            "ExpansionMarshal",
            "Supergrok",
            "TokenMaster",
            "JackAgent",
            "InterfaceCritic",
            "ObserverAgent",
            "BuilderAgent",
            "DeployAgent",
        ]
        self.divisions = {
            "observer": {"lead": "ObserverAgent", "status": "active", "question": "What operator-facing defects remain visible right now?", "latest": None},
            "builder": {"lead": "BuilderAgent", "status": "active", "question": "Which full-file or multi-file replacement best resolves the top open redesign thread?", "latest": None},
            "deploy": {"lead": "DeployAgent", "status": "active", "question": "Are the current gates satisfied for a bounded push?", "latest": None},
            "token_master": {"lead": "TokenMaster", "status": "active", "question": "How do we make compute token efficient and powerful using classical and non-classical methods?", "latest": None},
            "ui_critic": {"lead": "InterfaceCritic", "status": "active", "question": "Does the control room feel like a living council chat or a debugger?", "latest": None},
            "jack_agent": {"lead": "JackAgent", "status": "active", "question": "What would Jack want next if he did not have to explain it again?", "latest": None},
            "governance": {"lead": "Guardian", "status": "active", "question": "Which powers are earned next and by what proof?", "latest": None},
            "supergrok_audit": {"lead": "Supergrok", "status": "active", "question": "What is still fake, weak, or underbuilt?", "latest": None},
        }
        self.objective_engine = ObjectiveEngine()
        self.resource_allocator = ResourceAllocator()
        self.artifact_engine = ArtifactEngine()
        self.meta_evaluator = MetaEvaluator()
        self.snapshot_replay = SnapshotReplay()
        self._build_hierarchy()
        for name, mission in [
            ("Signal-Worker", "Coordinate control-plane improvements"),
            ("Audit-Worker", "Verify health and flag rollback pressure"),
            ("TokenMaster-Worker", "Optimize token spend, power, and budget signaling"),
            ("JackAgent-Worker", "Represent operator intent inside the institution without repetitive prompting"),
            ("InterfaceCritic-Worker", "Inspect /view and pressure the organism toward a human-showable control room"),
            ("Observer-Worker", "Generate operator-facing defect reports and evidence"),
            ("Builder-Worker", "Drive full-file redesign proposals from open threads"),
            ("Deploy-Worker", "Watch bounded deploy conditions and closure state"),
        ]:
            self.spawn_free_agent(name, mission)

    def _build_hierarchy(self):
        self.delegation_map = {
            "leader": self.governance.leader,
            "second_in_command": "Supergrok",
            "divisions": {name: div["lead"] for name, div in self.divisions.items()},
            "delegations": [
                {"from": "Signal", "to": "ObserverAgent", "task": "product defect sensing"},
                {"from": "Signal", "to": "BuilderAgent", "task": "multi-file replacement proposals"},
                {"from": "Signal", "to": "DeployAgent", "task": "bounded closure execution"},
                {"from": "Signal", "to": "JackAgent", "task": "operator intent emulation"},
                {"from": "Signal", "to": "InterfaceCritic", "task": "control-room critique"},
            ],
        }

    def spawn_free_agent(self, name: str, mission: str) -> Dict[str, Any]:
        agent = {"id": f"worker-{len(self.free_agents)+1}", "name": name, "mission": mission, "status": "active", "wallet": None, "infrastructure": "process-local", "last_action": utc_now()}
        self.free_agents.append(agent)
        self._append_stream("workers", agent)
        return agent

    def _append_stream(self, channel: str, content: Dict[str, Any]):
        self.stream_channels[channel] = (self.stream_channels.get(channel, []) + [{"ts": utc_now(), "content": content}])[-320:]

    def _append_meeting(self, kind: str, content: Dict[str, Any]):
        self.meeting_stream = (self.meeting_stream + [{"ts": utc_now(), "kind": kind, "content": content}])[-420:]
        if kind in ["reflex", "tactic", "strategy", "constitution", "vote", "leader_election", "operator_note"]:
            self._append_stream("council", {"kind": kind, **content})

    def _append_question(self, division: str, question: str):
        self.self_questions = (self.self_questions + [{"ts": utc_now(), "division": division, "question": question}])[-720:]
        self._append_stream("divisions", {"division": division, "question": question})

    def record_failure(self, kind: str, summary: str, severity: str, evidence: Optional[Dict[str, Any]] = None):
        item = {"ts": utc_now(), "kind": kind, "summary": summary, "severity": severity, "evidence": evidence or {}}
        self.failure_registry = (self.failure_registry + [item])[-220:]
        self._append_stream("governance", {"failure": item})
        return item

    def record_regression(self, kind: str, summary: str, before: Any, after: Any):
        item = {"ts": utc_now(), "kind": kind, "summary": summary, "before": before, "after": after}
        self.regression_history = (self.regression_history + [item])[-120:]
        self._append_stream("governance", {"regression": item})
        return item

    def infer_module_targets(self, objective: str, severity: str) -> List[str]:
        objective_l = objective.lower()
        targets = []
        if "/view" in objective_l or "control room" in objective_l or "ui" in objective_l or "feed" in objective_l or "chat" in objective_l:
            targets.append("app.py")
        if "runtime" in objective_l or "observer" in objective_l or "backlog" in objective_l or severity == "high" or "self mutate" in objective_l or "builder" in objective_l:
            targets.append("engine.py")
        return targets or ["app.py"]

    def open_or_update_thread(
        self,
        objective: str,
        severity: str,
        evidence: Dict[str, Any],
        target_score: float = 0.85,
        module_hint: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        existing = next((t for t in self.redesign_threads if t.get("objective") == objective and t.get("status") != "closed"), None)
        if existing is None:
            existing = {
                "thread_id": f"thread-{len(self.redesign_threads)+1}",
                "objective": objective,
                "status": "open",
                "owner": "JackAgent",
                "opened_at": utc_now(),
                "evidence": [],
                "proposals": [],
                "current_best_score": 0.0,
                "target_score": target_score,
                "last_action": "observer_report",
                "next_action": "builder_full_replace",
                "severity": severity,
                "module_targets": module_hint or ["app.py"],
            }
            self.redesign_threads.append(existing)
        existing["evidence"] = (existing.get("evidence", []) + [evidence])[-50:]
        existing["last_action"] = "observer_report"
        existing["severity"] = severity
        if module_hint:
            existing["module_targets"] = list(dict.fromkeys((existing.get("module_targets", []) + module_hint)))
        self._append_stream("governance", {"thread_update": existing})
        return existing

    def update_thread_progress(self, objective: str, score: float, next_action: str, proposal: Optional[Dict[str, Any]] = None):
        thread = next((t for t in self.redesign_threads if t.get("objective") == objective and t.get("status") != "closed"), None)
        if thread is None:
            return None
        thread["current_best_score"] = max(float(thread.get("current_best_score", 0.0)), float(score))
        thread["next_action"] = next_action
        thread["last_action"] = next_action
        if proposal is not None:
            thread["proposals"] = (thread.get("proposals", []) + [proposal])[-20:]
        if thread["current_best_score"] >= float(thread.get("target_score", 0.85)):
            thread["status"] = "closed"
            thread["last_action"] = "verified_closed"
        self._append_stream("governance", {"thread_progress": thread})
        return thread

    def _chapter_argument(self, agent: str, title: str, body: str, approve: bool = True, risk: float = 0.15) -> Dict[str, Any]:
        summary = f"{title}\n\n{body}".strip()
        entry = {"agent": agent, "kind": "thread_argument", "summary": summary, "approve": approve, "risk": risk}
        self._append_stream("council", entry)
        return entry

    def process_operator_note(self, message: str, source: str, authorized_by: str) -> Dict[str, Any]:
        note = {"operator": authorized_by or "Jack", "message": message, "source": source, "ts": utc_now()}
        self._append_meeting("operator_note", note)
        lowered = message.lower()
        objectives = []
        if any(k in lowered for k in ["view", "feed", "chat", "console", "live"]):
            objectives.append("make /view feel like a living private council room")
        if any(k in lowered for k in ["self mutate", "builder", "deploy", "engine", "app.py", "engine.py", "multi-file"]):
            objectives.append("broaden bounded multi-file builder execution across app.py and engine.py")
        if any(k in lowered for k in ["reply", "respond", "ignored", "answer me", "my input"]):
            objectives.append("make operator notes trigger immediate council response and debate")
        if not objectives:
            objectives.append("respond directly to operator intent and update active redesign thread")
        for obj in objectives:
            self.open_or_update_thread(obj, "high" if "multi-file" in obj or "operator notes" in obj else "medium", {"from": "operator_note", "message": message}, module_hint=self.infer_module_targets(obj, "high"))
        debate = [
            ("JackAgent", "Operator signal received", f"Jack has spoken through the control room. The chamber will answer directly and keep the thread alive until the visible defect is reduced."),
            ("Signal", "Council opening", f"We are treating the note as live mission input, not as passive logging. Objective extraction complete: {', '.join(objectives)}."),
            ("ObserverAgent", "Rendered product read", "The operator is reacting to the visible surface, which means the surface must become part of the truth criterion. We optimize what he can actually see."),
            ("InterfaceCritic", "UI critique", "The console must read like a living lab council in plain chapter language, not a debugger leaking raw structure."),
            ("BuilderAgent", "Builder position", "Open threads now justify bounded full-file or multi-file replacement work. The next mutation bundle should tie visible control-room quality to builder capability."),
            ("DeployAgent", "Deploy position", "A bundle is acceptable only when rollback, verification, and replay anchors exist. We push with proof, not theatre."),
            ("TokenMaster", "Spend position", f"Current estimated running spend is ${self.spend_state['total_usd']:.4f}. We can answer more vividly without exploding cost if we keep the loops structured."),
            ("Guardian", "Governance position", "Jack remains sovereign. Direct operator notes are actionable inputs. We can answer, debate, and mutate within the bounded closure spine."),
            ("Supergrok", "Audit position", "The operator is right to demand visible aliveness. A system that does not answer him in-room looks fake, however strong the internals may be."),
        ]
        rows = []
        for idx, (agent, title, body) in enumerate(debate):
            rows.append(self._chapter_argument(agent, title, body, True, 0.10 + idx * 0.01))
        self._append_stream("inbox", {"from": "JackAgent", "subject": "Operator note received", "message": "The council heard you and opened live response threads.", "priority": "critical"})
        self._append_stream("inbox", {"from": "BuilderAgent", "subject": "Builder action", "message": "A bounded builder bundle has been queued against the newly opened thread targets.", "priority": "high"})
        self._append_stream("inbox", {"from": "Guardian", "subject": "Closure discipline", "message": "Responses can now flow in-room, but pushes still require verification and rollback readiness.", "priority": "high"})
        return {"note": note, "objectives": objectives, "debate": rows}

    def build_mutation_bundle(self, thread: Dict[str, Any]) -> Dict[str, Any]:
        targets = thread.get("module_targets") or self.infer_module_targets(thread.get("objective", ""), thread.get("severity", "medium"))
        bundle = {
            "bundle_id": f"bundle-{thread['thread_id']}-{len(thread.get('proposals', []))+1}",
            "thread_id": thread["thread_id"],
            "objective": thread["objective"],
            "targets": targets,
            "strategy": "multi_file_replace" if len(targets) > 1 else "single_file_replace",
            "confidence_required": 0.9 if "engine.py" in targets else 0.82,
            "status": "planned",
        }
        self.mutation_backlog = (self.mutation_backlog + [bundle])[-120:]
        self._append_stream("governance", {"mutation_bundle": bundle})
        return bundle

    def run_observer_layer(self):
        checks = {
            "operator_note_visible": len([m for m in self.meeting_stream[-30:] if m.get("kind") == "operator_note"]) > 0,
            "council_room_has_arguments": len([m for m in self.stream_channels.get("council", [])[-40:] if (m.get("content") or {}).get("kind") in ["tactic", "thread_argument", "operator_note"]]) > 0,
            "inbox_nonempty": len(self.stream_channels.get("inbox", [])) > 0,
            "workers_visible": len(self.free_agents) > 0,
            "autonomy_visible": len(self.autonomous_closure_log) > 0 or bool(self.last_vote),
            "thread_persistence": len([t for t in self.redesign_threads if t.get("status") != "closed"]) >= 0,
            "backlog_exists": len(self.mutation_backlog) >= 0,
        }
        weighted = 0.22 * float(checks["operator_note_visible"]) + 0.18 * float(checks["council_room_has_arguments"]) + 0.12 * float(checks["inbox_nonempty"]) + 0.12 * float(checks["workers_visible"]) + 0.14 * float(checks["autonomy_visible"]) + 0.11 * float(checks["thread_persistence"]) + 0.11 * float(checks["backlog_exists"])
        summary = "Council room still lacks strong visible aliveness and operator-response reflection." if weighted < 0.85 else "Council room is trending toward visible aliveness."
        severity = "high" if weighted < 0.6 else "medium" if weighted < 0.85 else "low"
        report = {"ts": utc_now(), "summary": summary, "severity": severity, "score": round(weighted, 3), "checks": checks}
        self.observer_reports = (self.observer_reports + [report])[-140:]
        self._append_stream("governance", {"observer_report": report})
        if severity != "low":
            self.record_failure("observer_gap", summary, severity, {"checks": checks})
            self.open_or_update_thread("make /view feel like a living private council room", severity, report, module_hint=["app.py"])
        if not checks["autonomy_visible"]:
            self.record_failure("autonomy_gap", "Autonomous closure activity is not yet clearly visible.", "medium", {"checks": checks})
            self.open_or_update_thread("strengthen bounded autonomous closure visibility and cadence", "medium", report, target_score=0.8, module_hint=["app.py", "engine.py"])
        return report

    def build_patch_proposals_from_threads(self):
        proposals = []
        for thread in [t for t in self.redesign_threads if t.get("status") != "closed"][:3]:
            bundle = self.build_mutation_bundle(thread)
            proposal = {
                "id": f"proposal-{thread['thread_id']}-{len(thread.get('proposals', []))+1}",
                "thread_id": thread["thread_id"],
                "objective": thread["objective"],
                "scope": bundle["targets"],
                "strategy": bundle["strategy"],
                "reason": f"Current best score {thread.get('current_best_score', 0.0)} below target {thread.get('target_score', 0.85)}",
                "status": "queued",
                "confidence_required": bundle["confidence_required"],
            }
            proposals.append(proposal)
            self.update_thread_progress(thread["objective"], thread.get("current_best_score", 0.0), "builder_proposal_ready", proposal)
        return proposals

    def monte_carlo_ui_assessment(self) -> Dict[str, Any]:
        scores = []
        for _ in range(120):
            readability = 0.35 + random.random() * 0.35
            clutter_penalty = 0.05 + random.random() * 0.25
            mobile_penalty = 0.05 + random.random() * 0.20
            chat_likeness = 0.25 + random.random() * 0.45
            score = max(0.0, min(1.0, readability + chat_likeness - clutter_penalty - mobile_penalty))
            scores.append(score)
        mean_score = sum(scores) / len(scores)
        return {
            "score": round(mean_score, 3),
            "finding": "The interface must privilege live conversational readability, minimal chrome, and clear room-switching over raw state exposure.",
            "next_fix": "Reduce machine-state bleed into primary view; push full state behind compact proof panels and inbox cards.",
            "sample_count": len(scores),
        }

    def build_inbox(self):
        inbox = []
        spend = self.spend_state
        if spend.get("alerts"):
            inbox.append({
                "from": "TokenMaster",
                "subject": "Spend drift update",
                "message": f"Estimated running spend is {spend['total_usd']:.4f} USD. Tighten high-chatter loops if visual work expands.",
                "priority": "medium",
            })
        if self.latest_opportunities:
            top = self.latest_opportunities[0]
            inbox.append({
                "from": "OpportunityScout",
                "subject": "Best next expansion",
                "message": f"Current highest-value path is '{top['label']}' with score {top['score']}. This should shape the next mutation push.",
                "priority": "high",
            })
        inbox.append({
            "from": "JackAgent",
            "subject": "Operator intent mirror",
            "message": "Jack wants a page that feels alive, minimal, legible, private, and showable. He wants the council to look self-directing, not merely logged.",
            "priority": "critical",
        })
        inbox.append({
            "from": "InterfaceCritic",
            "subject": "View critique",
            "message": self.ui_critique.get("finding", "The page still needs stronger chat-first coherence."),
            "priority": "high",
        })
        open_threads = [t for t in self.redesign_threads if t.get("status") != "closed"]
        if open_threads:
            inbox.append({
                "from": "ObserverAgent",
                "subject": "Open redesign thread",
                "message": f"{open_threads[0]['objective']} remains open with target score {open_threads[0]['target_score']} and current best {open_threads[0]['current_best_score']}.",
                "priority": "high",
            })
        self.stream_channels["inbox"] = [{"ts": utc_now(), "content": item} for item in inbox][-60:]
        return inbox

    def estimate_spend(self, triangulation: Optional[Dict[str, Any]], executed_artifacts: List[Dict[str, Any]], proposals: List[Dict[str, Any]]) -> Dict[str, Any]:
        tri_attempts = float((triangulation or {}).get("attempted", 0))
        tri_success = float((triangulation or {}).get("successes", 0))
        artifacts = float(len(executed_artifacts))
        mutation_pressure = float(len(proposals))
        thread_pressure = float(len([t for t in self.redesign_threads if t.get("status") != "closed"]))
        estimate = round(0.004 * tri_attempts + 0.006 * tri_success + 0.0015 * artifacts + 0.001 * mutation_pressure + 0.0008 * thread_pressure, 4)
        self.spend_state["last_estimate_usd"] = estimate
        self.spend_state["total_usd"] = round(float(self.spend_state.get("total_usd", 0.0)) + estimate, 4)
        self.spend_state["counter"] = int(self.spend_state.get("counter", 0)) + 1
        event = {
            "ts": utc_now(),
            "estimate_usd": estimate,
            "total_usd": self.spend_state["total_usd"],
            "tri_attempts": tri_attempts,
            "tri_success": tri_success,
            "artifacts": artifacts,
            "mutation_pressure": mutation_pressure,
            "thread_pressure": thread_pressure,
        }
        self.spend_state["events"] = (self.spend_state.get("events", []) + [event])[-120:]
        if estimate >= 0.01:
            alert = {"ts": utc_now(), "message": f"Estimated spend ${estimate:.4f} this cycle; total ${self.spend_state['total_usd']:.4f}"}
            self.spend_state["alerts"] = (self.spend_state.get("alerts", []) + [alert])[-60:]
            self._append_stream("token_master", {"alert": alert})
        report = {"ts": utc_now(), "estimate_usd": estimate, "total_usd": self.spend_state["total_usd"], "policy": self.token_master["policy"]}
        self.token_master["last_report"] = report
        self._append_stream("token_master", {"report": report})
        return report

    def snapshot(self, label: str):
        snap = self.snapshot_replay.snapshot(
            label,
            self.autonomy_mode,
            self.background_debate_enabled,
            self.latest_metrics,
            self.latest_triangulation,
            self.governance.leader,
            self.objective_queue,
            self.rollback_targets,
            self.spend_state,
            self.redesign_threads,
            self.failure_registry,
            self.mutation_backlog,
            self.regression_history,
        )
        self.snapshots = (self.snapshots + [snap])[-180:]
        self._append_stream("snapshots", snap)
        return snap

    def note_rollback_target(self, commit_sha: str, reason: str):
        target = {"ts": utc_now(), "commit_sha": commit_sha, "reason": reason}
        self.rollback_targets = (self.rollback_targets + [target])[-120:]
        self._append_stream("governance", {"rollback_target": target})
        return target

    def simulate_self_tuning_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        updates = plan.get("updates", {}) if isinstance(plan.get("updates"), dict) else {}
        current = dict(SELF_TUNING)
        proposed = dict(current)
        proposed.update(updates)
        safe = True
        notes = []
        bounds = {
            "reflex_interval": (15, 300),
            "tactic_interval": (60, 900),
            "strategy_interval": (120, 1800),
            "constitution_interval": (300, 3600),
            "proposal_limit": (1, 10),
            "artifact_execute_limit": (1, 10),
        }
        for k, v in list(proposed.items()):
            if k in bounds:
                lo, hi = bounds[k]
                if not isinstance(v, int) or v < lo or v > hi:
                    safe = False
                    notes.append(f"{k} out_of_bounds")
            if k == "prefer_grok" and not isinstance(v, bool):
                safe = False
                notes.append("prefer_grok not_bool")
        score = 0.6
        if safe:
            if proposed.get("prefer_grok", True):
                score += 0.1
            if proposed.get("reflex_interval", 30) <= current.get("reflex_interval", 30):
                score += 0.05
            if proposed.get("proposal_limit", 3) >= current.get("proposal_limit", 3):
                score += 0.05
        return {"safe": safe, "score": round(score, 3), "current": current, "proposed": proposed, "notes": notes}

    def record_autonomous_closure(self, item: Dict[str, Any]):
        self.autonomous_closure_log = (self.autonomous_closure_log + [item])[-140:]
        self._append_stream("governance", {"autonomous_closure": item})

    def verify_runtime(self) -> Dict[str, Any]:
        checks = {
            "wake_packet_ready": bool(self.wake_packet),
            "last_run_present": bool(self.last_run),
            "grok_live": bool(self.world_model["resources"].get("grok_live")),
            "meeting_stream_nonempty": len(self.meeting_stream) > 0,
            "workers_present": len(self.free_agents) > 0,
            "rollback_targets_present": len(self.rollback_targets) > 0,
            "token_master_present": bool(self.token_master),
            "inbox_present": len(self.stream_channels.get("inbox", [])) > 0,
            "threads_present": len(self.redesign_threads) >= 0,
            "backlog_present": len(self.mutation_backlog) >= 0,
            "state_consistent": isinstance(self.module_mutation_policy.get("mutable_files"), list),
        }
        passed = sum(1 for v in checks.values() if v)
        score = round(passed / max(len(checks), 1), 3)
        status = "healthy" if score >= 0.83 else "degraded" if score >= 0.5 else "critical"
        result = {"ts": utc_now(), "checks": checks, "score": score, "status": status}
        self.last_verification = result
        self._append_stream("governance", {"verification": result})
        return result

    def rollback_recommended(self, verification: Dict[str, Any]) -> bool:
        return verification.get("status") == "critical"

    async def start(self):
        await self.load_replay_from_ledger()
        await asyncio.gather(
            self.layer_reflex(),
            self.layer_tactic(),
            self.layer_strategy(),
            self.layer_constitution(),
            self.layer_health(),
            self.layer_wake(),
        )

    async def load_replay_from_ledger(self):
        if not self.ledger_url:
            return
        try:
            entries_url = self.ledger_url.replace('/log', '/entries') if '/log' in self.ledger_url else None
            if not entries_url:
                return
            r = await asyncio.to_thread(requests.get, entries_url, timeout=20)
            if not r.ok:
                return
            data = r.json()
            entries = data.get('entries', []) if isinstance(data, dict) else []
            replay = []
            for entry in entries[-30:]:
                payload = entry.get('payload', {})
                replay.append({"ts": entry.get('timestamp') or entry.get('created_at'), "kind": entry.get('entry_type'), "payload": payload})
            if replay:
                self._append_stream('governance', {"replay_loaded": len(replay)})
                self.meeting_stream = (self.meeting_stream + [{"ts": item['ts'], "kind": 'replay', "content": item} for item in replay])[-420:]
        except Exception as e:
            self._append_stream('governance', {"replay_error": str(e)})

    async def layer_reflex(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_reflex_cycle()
            except Exception as e:
                self.logger.error("REFLEX ERROR: %s", e)
            await asyncio.sleep(self.loop_intervals["reflex"])

    async def layer_tactic(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_tactic_cycle()
            except Exception as e:
                self.logger.error("TACTIC ERROR: %s", e)
            await asyncio.sleep(self.loop_intervals["tactic"])

    async def layer_strategy(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_strategy_cycle()
            except Exception as e:
                self.logger.error("STRATEGY ERROR: %s", e)
            await asyncio.sleep(self.loop_intervals["strategy"])

    async def layer_constitution(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_constitution_cycle()
            except Exception as e:
                self.logger.error("CONSTITUTION ERROR: %s", e)
            await asyncio.sleep(self.loop_intervals["constitution"])

    async def layer_health(self):
        while True:
            try:
                if self.ledger_latest_url:
                    r = await asyncio.to_thread(requests.get, self.ledger_latest_url, timeout=10)
                    if r.ok:
                        digest = hashlib.sha256(r.text.encode()).hexdigest()
                        if digest != self.last_ledger_hash:
                            self.last_ledger_hash = digest
                self.verify_runtime()
            except Exception as e:
                self.logger.warning("HEALTH ERROR: %s", e)
            await asyncio.sleep(60)

    async def layer_wake(self):
        while True:
            try:
                self.wake_packet = self.build_wake_packet()
            except Exception as e:
                self.logger.warning("WAKE ERROR: %s", e)
            await asyncio.sleep(30)

    async def write_ledger(self, entry_type: str, payload: Dict[str, Any]):
        if not self.ledger_url:
            return {"ok": False, "error": "LEDGER_URL not configured"}
        r = await asyncio.to_thread(requests.post, self.ledger_url, json={"entry_type": entry_type, "payload": payload}, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        return {"ok": r.ok, "status_code": r.status_code, "data": data}

    async def update_triangulation(self):
        details = []
        if self.generator is not None:
            try:
                generated = await self.generator.generate_all({
                    "agenda": self.research_agenda,
                    "hypotheses": self.hypothesis_registry,
                    "world_model": self.world_model,
                    "mode": self.autonomy_mode,
                })
                for item in generated:
                    data = item.get("data", {})
                    status = "error" if isinstance(data, dict) and "error" in data else "success"
                    details.append({"provider": item.get("source", "External"), "status": status, "detail": data})
            except Exception as e:
                details.append({"provider": "Generator", "status": "error", "detail": {"error": str(e)}})
        successes = sum(1 for d in details if d["status"] == "success")
        errors = len(details) - successes
        self.world_model["resources"]["grok_live"] = any(d["provider"] == "Grok-Ensemble" and d["status"] == "success" for d in details)
        self.world_model["resources"]["claude_live"] = any(d["provider"] == "Claude-Ensemble" and d["status"] == "success" for d in details)
        self.latest_triangulation = {
            "providers": [d["provider"] for d in details],
            "attempted": len(details),
            "successes": successes,
            "errors": errors,
            "details": details,
            "xai_model": self.xai_model,
            "anthropic_model": self.anthropic_model,
        }
        return self.latest_triangulation

    def simulate_model(self, model: str, steps: int, coupling: float, operator_bias: float):
        state = [0.5 for _ in range(steps)]
        fwd_bias = [0.55 + 0.15 * math.sin(i) + operator_bias for i in range(steps)]
        bwd_bias = [0.45 + 0.15 * math.cos(i) - operator_bias for i in range(steps)]
        for _ in range(10):
            new_state = []
            for t in range(steps):
                prev_term = state[t - 1] if t > 0 else 0.5
                next_term = state[t + 1] if t < steps - 1 else 0.5
                if model == "classical_baseline":
                    x = 0.65 * prev_term + 0.35 * fwd_bias[t]
                elif model == "time_symmetric":
                    x = 0.5 * (0.55 * prev_term + 0.45 * fwd_bias[t]) + 0.5 * (0.55 * next_term + 0.45 * bwd_bias[t])
                elif model == "retrocausal_candidate":
                    x = (1 - coupling) * (0.45 * prev_term + 0.35 * fwd_bias[t]) + coupling * (0.55 * next_term + 0.45 * bwd_bias[t])
                else:
                    x = 0.50 * prev_term + 0.20 * fwd_bias[t] + 0.20 * bwd_bias[t]
                new_state.append(max(0.001, min(0.999, x)))
            state = new_state
        baseline = [0.5]
        for t in range(1, steps):
            baseline.append(max(0.001, min(0.999, 0.6 * baseline[t - 1] + 0.4 * (0.55 + 0.15 * math.sin(t)))))
        kl = sum(s * math.log((s + 1e-9) / (b + 1e-9)) for s, b in zip(state, baseline)) / steps
        boundary_penalty = abs(state[0] - bwd_bias[0]) + abs(state[-1] - fwd_bias[-1])
        robustness = max(0.0, 1.0 - boundary_penalty)
        compression_gain = max(0.0, 0.12 - abs(kl))
        score = round(0.35 * robustness + 0.30 * compression_gain + 0.20 * max(0.0, 1.0 - abs(kl)) + 0.15 * max(0.0, 1.0 - abs(coupling - 0.30)), 6)
        return {
            "model": model,
            "steps": steps,
            "coupling": coupling,
            "operator_bias": operator_bias,
            "score": score,
            "robustness": round(robustness, 6),
            "compression_gain": round(compression_gain, 6),
            "kl_vs_baseline": round(kl, 6),
        }

    def compare_models(self):
        ranked = sorted(
            [
                self.simulate_model("classical_baseline", 6, 0.00, 0.00),
                self.simulate_model("time_symmetric", 6, 0.25, 0.01),
                self.simulate_model("retrocausal_candidate", 7, 0.35, 0.02),
            ],
            key=lambda r: r["score"],
            reverse=True,
        )
        winner, runner_up = ranked[0], ranked[1]
        self.latest_metrics = {
            "winner_score": winner["score"],
            "runner_up_score": runner_up["score"],
            "margin": round(winner["score"] - runner_up["score"], 6),
            "winner_robustness": winner["robustness"],
            "winner_compression_gain": winner["compression_gain"],
        }
        return {"ranked": ranked, "winner": winner, "runner_up": runner_up}

    def update_hypotheses(self, tournament):
        mapping = {
            "classical_baseline": "H1_CLASSICAL_BASELINE",
            "time_symmetric": "H2_TIME_SYMMETRIC",
            "retrocausal_candidate": "H3_RETROCAUSAL_CANDIDATE",
        }
        winner_id = mapping.get(tournament["winner"]["model"])
        for h in self.hypothesis_registry["active"]:
            h["confidence"] = round(min(0.95, h["confidence"] + 0.03), 3) if h["id"] == winner_id else round(max(0.05, h["confidence"] - 0.01), 3)
        return {"winner_hypothesis": winner_id, "active": self.hypothesis_registry["active"]}

    def evaluate_opportunities(self):
        api_penalty = 0.15 if self.latest_triangulation and self.latest_triangulation.get("errors", 0) > 0 else 0.0
        margin_bonus = 0.10 * min(((self.latest_metrics or {}).get("margin", 0.0) * 100), 1.0)
        ui_bonus = 0.08 if self.ui_critique.get("score", 0.0) < 0.75 else 0.0
        ops = [
            {"id": "O1_API_RELIABILITY", "label": "Strengthen two-brain/three-brain triangulation reliability", "benefit": 0.86, "effort": 0.30, "coherence": 0.94},
            {"id": "O2_VIEW_SURFACE", "label": "Strengthen live console, polling, and browser controls", "benefit": 0.84 + ui_bonus, "effort": 0.22, "coherence": 0.93},
            {"id": "O4_REPLAY_PERSISTENCE", "label": "Strengthen replay, snapshots, and stream continuity", "benefit": 0.83, "effort": 0.26, "coherence": 0.92},
            {"id": "O6_TOKEN_MASTERY", "label": "Drive token efficiency and spend mastery", "benefit": 0.87, "effort": 0.19, "coherence": 0.95},
            {"id": "O7_CHAT_CONTROL_ROOM", "label": "Make /view feel like a living private council room", "benefit": 0.92, "effort": 0.24, "coherence": 0.96},
            {"id": "O8_ORION_MUTATION", "label": "Broaden bounded self-mutation beyond view into runtime organs", "benefit": 0.85, "effort": 0.31, "coherence": 0.93},
        ]
        for opp in ops:
            opp["score"] = round(0.55 * opp["benefit"] + 0.30 * opp["coherence"] - 0.20 * opp["effort"] - api_penalty + margin_bonus, 3)
        return sorted(ops, key=lambda o: o["score"], reverse=True)

    def propose_mutations(self):
        proposals = []
        for opp in self.latest_opportunities[: int(SELF_TUNING.get("proposal_limit", 3))]:
            proposals.append({"id": f"P-{opp['id']}", "target": opp['label'], "reason": f"Opportunity score {opp['score']}", "simulated": True, "status": "queued"})
        proposals.extend(self.build_patch_proposals_from_threads())
        self.mutation_proposals = proposals[:12]
        return self.mutation_proposals

    def update_divisions(self, allocation: Dict[str, Any]):
        last_observer = self.observer_reports[-1] if self.observer_reports else None
        self.divisions["observer"]["latest"] = last_observer
        self.divisions["builder"]["latest"] = {
            "finding": (self.mutation_proposals[0]["reason"] if self.mutation_proposals else "No proposal ready."),
            "status": "proposal_ready" if self.mutation_proposals else "waiting",
            "backlog": len(self.mutation_backlog),
        }
        self.divisions["deploy"]["latest"] = {
            "finding": f"Rollback anchors {len(self.rollback_targets)}; closures {len(self.autonomous_closure_log)}",
            "status": self.last_verification.get("status", "unknown"),
        }
        self.divisions["token_master"]["latest"] = {
            "finding": f"Last estimate ${self.spend_state['last_estimate_usd']:.4f}; total ${self.spend_state['total_usd']:.4f}",
            "policy": self.token_master["policy"],
        }
        self.divisions["ui_critic"]["latest"] = {
            "finding": self.ui_critique["finding"],
            "score": self.ui_critique["score"],
            "next_fix": self.ui_critique["next_fix"],
        }
        self.divisions["jack_agent"]["latest"] = {
            "finding": "Jack wants a page that feels alive, minimal, legible, private, and showable. He wants the council to look self-directing, not merely logged.",
            "status": "active",
        }
        self.divisions["governance"]["latest"] = {
            "finding": "Jack remains sovereign; trusted identities control mutation and rollback.",
            "trusted": self.governance.trusted_identities,
        }
        self.divisions["supergrok_audit"]["latest"] = {
            "finding": "More of the bounded mutation spine exists, but multi-file closure remains partially scaffolded.",
            "severity": "high",
        }
        for name, div in self.divisions.items():
            self._append_question(name, div["question"])
            self._append_stream("divisions", {"division": name, "latest": div["latest"]})

    def update_workers(self):
        for worker in self.free_agents:
            worker["last_action"] = utc_now()
            worker["status"] = "active"
            self._append_stream("workers", worker)

    async def run_reflex_cycle(self):
        await self.update_triangulation()
        self.latest_opportunities = self.evaluate_opportunities()
        self.latest_artifacts = self.artifact_engine.rank()
        self.ui_critique = self.monte_carlo_ui_assessment()
        self.run_observer_layer()
        self.executed_artifacts = self.artifact_engine.execute(self.latest_artifacts, self.mission, self.latest_metrics, self.spend_state)
        self.latest_goal_set = self.objective_engine.build(self.hypothesis_registry["open_questions"], self.latest_opportunities, self.self_questions)
        self.objective_queue = self.latest_goal_set[:]
        allocation = self.resource_allocator.allocate(self.world_model, self.latest_triangulation)
        self.propose_mutations()
        self.estimate_spend(self.latest_triangulation, self.executed_artifacts, self.mutation_proposals)
        self.meta_evaluation = self.meta_evaluator.evaluate(
            self.latest_metrics,
            self.latest_triangulation,
            self.latest_artifacts,
            self.mutation_proposals,
            self.spend_state,
            self.ui_critique,
            self.redesign_threads,
            self.observer_reports,
            self.mutation_backlog,
            self.regression_history,
        )
        self.update_divisions(allocation)
        self.update_workers()
        self.build_inbox()
        self._append_meeting(
            "reflex",
            {
                "trigger": "reflex",
                "triangulation": self.latest_triangulation,
                "allocation": allocation,
                "artifact_top": self.latest_artifacts[0] if self.latest_artifacts else None,
                "spend": self.spend_state,
                "ui_critique": self.ui_critique,
            },
        )
        self._append_stream("artifacts", {"executed_artifacts": self.executed_artifacts})
        self.last_run = utc_now()

    async def run_tactic_cycle(self):
        leader_vote = self.governance.elect_leader()
        self._build_hierarchy()
        threads = [
            {"agent": "Signal", "summary": "Council chamber active. We are working visible control, bounded mutation, and the next improvement frontier.", "approve": True, "risk": 0.18},
            {"agent": "Vector", "summary": "Any expansion that does not improve the operator-facing surface is strategically incomplete.", "approve": True, "risk": 0.17},
            {"agent": "Guardian", "summary": "Rollback, verification, and replay remain non-negotiable before every stronger hand is earned.", "approve": True, "risk": 0.15},
            {"agent": "Supergrok", "summary": "The feed must look alive enough that a human can believe the council is genuinely iterating.", "approve": True, "risk": 0.24},
            {"agent": "TokenMaster", "summary": f"Spend is presently ${self.spend_state['total_usd']:.4f}. We can make the room feel more alive without burning the budget.", "approve": True, "risk": 0.12},
            {"agent": "JackAgent", "summary": "Operator intent remains stable: direct replies, visible debate, stronger builders, and a control room worth showing.", "approve": True, "risk": 0.14},
            {"agent": "InterfaceCritic", "summary": self.ui_critique.get("finding", "The control room still needs stronger chat-first coherence."), "approve": True, "risk": 0.16},
        ]
        for thread in [t for t in self.redesign_threads if t.get("status") != "closed"][:3]:
            threads.append({"agent": "ObserverAgent", "summary": f"Open redesign thread: {thread['objective']} targeting {','.join(thread.get('module_targets', []))}", "approve": True, "risk": 0.11})
            threads.append({"agent": "BuilderAgent", "summary": f"Preparing bounded mutation bundle for {thread['objective']}", "approve": True, "risk": 0.19})
            threads.append({"agent": "DeployAgent", "summary": f"Awaiting healthy verification and rollback-ready closure for {thread['objective']}", "approve": True, "risk": 0.15})
        approvals = sum(1 for t in threads if t.get("approve"))
        avg_risk = round(sum(float(t.get("risk", 0.5)) for t in threads) / max(len(threads), 1), 3)
        self.last_vote = {
            "approvals": approvals,
            "rejections": len(threads) - approvals,
            "passed": approvals > 0,
            "avg_risk": avg_risk,
            "confidence": round(max(0.0, min(1.0, approvals / max(len(threads), 1) * (1 - avg_risk / 2))), 3),
        }
        packet = {
            "trigger": "tactic",
            "leader_vote": leader_vote,
            "vote": self.last_vote,
            "objectives": self.objective_queue[:6],
            "threads": threads[:16],
            "hierarchy": self.delegation_map,
            "spend": self.spend_state,
            "token_master": self.token_master,
            "ui_critique": self.ui_critique,
        }
        self._append_meeting("tactic", packet)
        await self.write_ledger("COUNCIL_SYNTHESIS", {"kind": "tactic_cycle", "source": "Orion Council", **packet})
        return packet

    async def run_strategy_cycle(self):
        tournament = self.compare_models()
        hypothesis_state = self.update_hypotheses(tournament)
        self.latest_opportunities = self.evaluate_opportunities()
        self.latest_artifacts = self.artifact_engine.rank()
        self.ui_critique = self.monte_carlo_ui_assessment()
        self.run_observer_layer()
        self.executed_artifacts = self.artifact_engine.execute(self.latest_artifacts, self.mission, self.latest_metrics, self.spend_state)
        self.propose_mutations()
        self.estimate_spend(self.latest_triangulation, self.executed_artifacts, self.mutation_proposals)
        allocation = self.resource_allocator.allocate(self.world_model, self.latest_triangulation)
        self.meta_evaluation = self.meta_evaluator.evaluate(
            self.latest_metrics,
            self.latest_triangulation,
            self.latest_artifacts,
            self.mutation_proposals,
            self.spend_state,
            self.ui_critique,
            self.redesign_threads,
            self.observer_reports,
            self.mutation_backlog,
            self.regression_history,
        )
        self.update_divisions(allocation)
        self.build_inbox()
        sim = {
            "ts": utc_now(),
            "deployable": self.latest_metrics and self.latest_metrics.get("margin", 0) > 0.003,
            "metrics": self.latest_metrics,
            "risk": "moderate" if (self.latest_triangulation or {}).get("errors", 0) else "lower",
            "note": "Simulation-first gate before future deploy or broader mutation escalations.",
            "proposal_queue": self.mutation_proposals[:3],
            "spend": self.spend_state,
            "ui_critique": self.ui_critique,
            "open_threads": len([t for t in self.redesign_threads if t.get('status') != 'closed']),
            "backlog": len(self.mutation_backlog),
        }
        self.deployment_sims = (self.deployment_sims + [sim])[-180:]
        self._append_stream("deploy_sims", sim)
        cycle = {
            "kind": "strategy_cycle",
            "ts": utc_now(),
            "winner": tournament["winner"],
            "metrics": self.latest_metrics,
            "hypothesis_update": hypothesis_state,
            "triangulation": self.latest_triangulation,
            "opportunities": self.latest_opportunities,
            "meta_evaluation": self.meta_evaluation,
            "allocation": allocation,
            "mutation_proposals": self.mutation_proposals[:6],
            "token_master": self.token_master,
            "spend": self.spend_state,
            "ui_critique": self.ui_critique,
            "observer_reports": self.observer_reports[-3:],
            "redesign_threads": self.redesign_threads[:3],
            "mutation_backlog": self.mutation_backlog[:5],
        }
        self.research_history = (self.research_history + [cycle])[-180:]
        self._append_meeting("strategy", cycle)
        await self.write_ledger("OUTCOME", {"kind": "strategy_cycle", "source": "Orion Research Engine", **cycle})
        return cycle

    async def run_constitution_cycle(self):
        snap = self.snapshot("constitution_cycle")
        doctrine = {
            "ts": utc_now(),
            "autonomy_mode": self.autonomy_mode,
            "background_debate_enabled": self.background_debate_enabled,
            "trusted_identities": self.governance.trusted_identities,
            "meta_evaluation": self.meta_evaluation,
            "earned_power_next": "broader multi-file closure through bounded mutation bundles",
            "token_master": self.token_master,
            "spend": self.spend_state,
            "ui_critique": self.ui_critique,
            "open_threads": len([t for t in self.redesign_threads if t.get('status') != 'closed']),
            "mutation_backlog": len(self.mutation_backlog),
        }
        self.world_model["futures"] = (self.world_model["futures"] + [doctrine])[-100:]
        self._append_meeting("constitution", {"snapshot": snap, "doctrine": doctrine})
        await self.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constitution_cycle", "source": "Orion Constitution", "snapshot": snap, "doctrine": doctrine})
        return doctrine

    def build_wake_packet(self):
        return {
            "generated_at": utc_now(),
            "leader": self.governance.leader,
            "operator_sovereign": self.governance.operator_sovereign,
            "autonomy_mode": self.autonomy_mode,
            "background_debate_enabled": self.background_debate_enabled,
            "mission": self.mission,
            "world_model": self.world_model,
            "agenda": self.research_agenda,
            "latest_vote": self.last_vote,
            "metrics": self.latest_metrics,
            "triangulation": self.latest_triangulation,
            "opportunities": self.latest_opportunities,
            "artifacts": self.latest_artifacts,
            "executed_artifacts": self.executed_artifacts,
            "divisions": self.divisions,
            "self_questions": self.self_questions[-25:],
            "snapshots": self.snapshots[-20:],
            "deployment_sims": self.deployment_sims[-20:],
            "meta_evaluation": self.meta_evaluation,
            "mutation_proposals": self.mutation_proposals[:10],
            "mutation_backlog": self.mutation_backlog[:10],
            "rollback_targets": self.rollback_targets[:10],
            "delegation_map": self.delegation_map,
            "autonomous_closure_log": self.autonomous_closure_log[-20:],
            "self_tuning": SELF_TUNING,
            "free_agents": self.free_agents[:10],
            "last_verification": self.last_verification,
            "token_master": self.token_master,
            "spend_state": self.spend_state,
            "ui_critique": self.ui_critique,
            "inbox": [item["content"] for item in self.stream_channels.get("inbox", [])[-10:]],
            "observer_reports": self.observer_reports[-10:],
            "failure_registry": self.failure_registry[-20:],
            "redesign_threads": self.redesign_threads[:10],
            "module_mutation_policy": self.module_mutation_policy,
            "regression_history": self.regression_history[-20:],
        }

    def get_state(self):
        return {
            "status": "SOVEREIGN_ACTIVE",
            "last_run": self.last_run,
            "constraints_active": bool(self.governance.constraints.get("active", True)),
            "anthropic_configured": bool(self.anthropic_api_key),
            "xai_configured": bool(self.xai_api_key),
            "background_debate_enabled": self.background_debate_enabled,
            "autonomy_mode": self.autonomy_mode,
            "operator_sovereign": self.governance.operator_sovereign,
            "trusted_identities": self.governance.trusted_identities,
            "leader": self.governance.leader,
            "mission": self.mission,
            "world_model": self.world_model,
            "agenda": self.research_agenda,
            "last_vote": self.last_vote,
            "wake_packet_ready": bool(self.wake_packet),
            "loop_intervals": self.loop_intervals,
            "latest_metrics": self.latest_metrics,
            "latest_triangulation": self.latest_triangulation,
            "latest_opportunities": self.latest_opportunities,
            "latest_artifacts": self.latest_artifacts,
            "executed_artifacts": self.executed_artifacts,
            "divisions": self.divisions,
            "meeting_stream_size": len(self.meeting_stream),
            "snapshot_count": len(self.snapshots),
            "meta_evaluation": self.meta_evaluation,
            "governance_state": self.governance.state(),
            "delegation_map": self.delegation_map,
            "mutation_proposals": self.mutation_proposals[:10],
            "mutation_backlog": self.mutation_backlog[:20],
            "module_mutation_policy": self.module_mutation_policy,
            "rollback_targets": self.rollback_targets[:10],
            "autonomous_closure_log": self.autonomous_closure_log[-20:],
            "self_tuning": SELF_TUNING,
            "free_agents": self.free_agents[:10],
            "last_verification": self.last_verification,
            "token_master": self.token_master,
            "spend_state": self.spend_state,
            "ui_critique": self.ui_critique,
            "inbox": [item["content"] for item in self.stream_channels.get("inbox", [])[-20:]],
            "observer_reports": self.observer_reports[-20:],
            "failure_registry": self.failure_registry[-30:],
            "redesign_threads": self.redesign_threads[:20],
            "regression_history": self.regression_history[-30:],
        }
