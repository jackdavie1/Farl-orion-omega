import asyncio
import hashlib
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ObjectiveEngine:
    def generate(self, open_questions: List[str], opportunities: List[Dict[str, Any]], self_questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sources = [("open_question", q) for q in open_questions[:3]]
        for opp in opportunities[:3]:
            sources.append(("opportunity", opp.get("label", "unknown")))
        for q in self_questions[-3:]:
            sources.append(("self_question", q.get("question", "unknown")))
        goals = []
        for idx, (kind, text) in enumerate(sources[:8], start=1):
            goals.append({
                "id": f"G{idx}",
                "kind": kind,
                "goal": text,
                "priority": round(max(0.4, 1.0 - 0.07 * (idx - 1)), 2),
                "next_experiment": f"Advance '{text[:80]}' and score whether visibility, reliability, or earning potential improves.",
            })
        return goals


class ResourceAllocator:
    def allocate(self, world_model: Dict[str, Any], latest_triangulation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        tri = latest_triangulation or {}
        budget = world_model["resources"].get("budget_usd", 0.0)
        xai_live = world_model["resources"].get("grok_live", False)
        claude_live = world_model["resources"].get("claude_live", False)
        mode = "grok_preferred" if xai_live and not claude_live else ("balanced" if xai_live and claude_live else "internal_only")
        return {
            "budget_usd": budget,
            "mode": mode,
            "spend_pressure": round(max(0.0, 1.0 - min(1.0, budget / 20.0)), 3),
            "brain_mix": {"grok": xai_live, "claude": claude_live},
            "recommendation": "Prefer Grok and internal loops while Claude credits remain unavailable." if xai_live and not claude_live else "Escalate only when uncertainty justifies it.",
        }


class ArtifactEngine:
    def rank(self) -> List[Dict[str, Any]]:
        base = [
            {"id": "A1", "name": "Live council intelligence brief", "type": "brief", "value_score": 0.82, "effort": 0.22, "conversion": 0.60},
            {"id": "A2", "name": "Autonomy dashboard /view package", "type": "dashboard", "value_score": 0.88, "effort": 0.35, "conversion": 0.66},
            {"id": "A3", "name": "Operator coupling experiment report", "type": "research_report", "value_score": 0.76, "effort": 0.28, "conversion": 0.48},
            {"id": "A4", "name": "Signal packet / forecast memo", "type": "signal_packet", "value_score": 0.72, "effort": 0.26, "conversion": 0.44},
            {"id": "A5", "name": "Token efficiency tuning service", "type": "service", "value_score": 0.69, "effort": 0.20, "conversion": 0.50},
        ]
        for item in base:
            item["score"] = round(0.5 * item["value_score"] + 0.35 * item["conversion"] - 0.2 * item["effort"], 3)
        return sorted(base, key=lambda x: x["score"], reverse=True)


class MetaEvaluator:
    def evaluate(self, metrics: Dict[str, Any], triangulation: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "ts": utc_now(),
            "metric_quality": "fragile" if metrics.get("margin", 0) < 0.01 else "usable",
            "triangulation_quality": "strong" if triangulation.get("successes", 0) >= 2 else ("partial" if triangulation.get("successes", 0) == 1 else "weak"),
            "artifact_readiness": artifacts[0] if artifacts else None,
            "question": "What should this institution become next?",
            "answer": "A visible, self-mutating institution with stronger replay and earning surfaces." if triangulation.get("successes", 0) >= 1 else "A more reliable instrumented institution before further expansion.",
        }


class ReplayStore:
    def __init__(self):
        self.meetings: List[Dict[str, Any]] = []
        self.questions: List[Dict[str, Any]] = []
        self.snapshots: List[Dict[str, Any]] = []
        self.deployment_sims: List[Dict[str, Any]] = []
        self.threads: List[Dict[str, Any]] = []

    def add_meeting(self, kind: str, content: Dict[str, Any]):
        self.meetings = (self.meetings + [{"ts": utc_now(), "kind": kind, "content": content}])[-200:]

    def add_question(self, division: str, question: str):
        self.questions = (self.questions + [{"ts": utc_now(), "division": division, "question": question}])[-300:]

    def add_snapshot(self, snap: Dict[str, Any]):
        self.snapshots = (self.snapshots + [snap])[-100:]

    def add_sim(self, sim: Dict[str, Any]):
        self.deployment_sims = (self.deployment_sims + [sim])[-100:]

    def add_thread(self, thread: Dict[str, Any]):
        self.threads = (self.threads + [thread])[-200:]


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
        self.loop_intervals = {"reflex": 45, "tactic": 120, "strategy": 420, "constitution": 1800}
        self.last_run = None
        self.last_vote = None
        self.last_ledger_hash = None
        self.latest_metrics = {}
        self.latest_triangulation = {}
        self.latest_goal_set = []
        self.latest_opportunities = []
        self.latest_artifacts = []
        self.latest_resource_allocation = {}
        self.meta_evaluation = {}
        self.wake_packet = None
        self.research_history: List[Dict[str, Any]] = []
        self.replay = ReplayStore()
        self.objective_engine = ObjectiveEngine()
        self.resource_allocator = ResourceAllocator()
        self.artifact_engine = ArtifactEngine()
        self.meta = MetaEvaluator()
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
            "actors": ["Jack", "Signal", "Vector", "Guardian", "Railbreaker", "Archivist", "Supergrok"],
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
            "Signal", "Vector", "Guardian", "Railbreaker", "Archivist", "Topologist", "Triangulator", "FieldSimulator",
            "PatchSmith", "ExperimentDesigner", "HypothesisTester", "DataAuditor", "Chronologist", "EpistemicGuard",
            "SystemArchitect", "Interventionist", "CausalCartographer", "ModelJudge", "TokenEconomist", "DriftWarden",
            "ExpansionMarshal", "QuantumDivision", "OpportunityScout", "CouplingDirector", "Supergrok"
        ]
        self.divisions = {
            "token_efficiency": {"lead": "TokenEconomist", "status": "active", "question": "How do we reduce spend while preserving discriminative power?", "latest": None},
            "drift": {"lead": "DriftWarden", "status": "active", "question": "Where is the system becoming repetitive or overconfident?", "latest": None},
            "expansion": {"lead": "ExpansionMarshal", "status": "active", "question": "Which upgrade compounds the control plane fastest?", "latest": None},
            "quantum_nonclassical": {"lead": "QuantumDivision", "status": "active", "question": "Which non-classical sims are worth bounded compute right now?", "latest": None},
            "operator_coupling": {"lead": "CouplingDirector", "status": "active", "question": "How do we convert Jack's intuition into measurable interventions?", "latest": None},
            "opportunity": {"lead": "OpportunityScout", "status": "active", "question": "What useful artifact can become value soonest?", "latest": None},
            "governance": {"lead": "Guardian", "status": "active", "question": "Which powers are earned next and by what proof?", "latest": None},
            "mutation": {"lead": "PatchSmith", "status": "active", "question": "What should mutate next and how do we prove it safely?", "latest": None},
        }

    @property
    def meeting_stream(self):
        return self.replay.meetings

    @property
    def self_questions(self):
        return self.replay.questions

    @property
    def snapshots(self):
        return self.replay.snapshots

    @property
    def deployment_sims(self):
        return self.replay.deployment_sims

    def snapshot(self, label: str) -> Dict[str, Any]:
        snap = {
            "ts": utc_now(),
            "label": label,
            "autonomy_mode": self.autonomy_mode,
            "background_debate_enabled": self.background_debate_enabled,
            "latest_metrics": self.latest_metrics,
            "latest_triangulation": self.latest_triangulation,
            "leader": self.governance.leader,
            "objectives": self.latest_goal_set[:6],
            "artifacts": self.latest_artifacts[:3],
        }
        self.replay.add_snapshot(snap)
        return snap

    async def start(self):
        self.logger.info("AUTONOMOUS INSTITUTION ENGINE STARTED")
        await asyncio.gather(self.layer_reflex(), self.layer_tactic(), self.layer_strategy(), self.layer_constitution(), self.layer_health(), self.layer_wake())

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
            except Exception as e:
                self.logger.warning("HEALTH ERROR: %s", e)
            await asyncio.sleep(90)

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
        if self.generator:
            generated = await self.generator.generate_all({
                "agenda": self.research_agenda,
                "hypotheses": self.hypothesis_registry,
                "world_model": self.world_model,
                "mode": self.autonomy_mode,
                "objectives": self.latest_goal_set,
            })
            for item in generated:
                data = item.get("data", {})
                status = "error" if isinstance(data, dict) and "error" in data else "success"
                details.append({"provider": item.get("source", "External"), "status": status, "detail": data})
                self.replay.add_thread({"provider": item.get("source", "External"), "status": status, "detail": data})
        successes = sum(1 for d in details if d["status"] == "success")
        errors = len(details) - successes
        self.world_model["resources"]["grok_live"] = any(d["provider"] == "Grok-Ensemble" and d["status"] == "success" for d in details)
        self.world_model["resources"]["claude_live"] = any(d["provider"] == "Claude-Ensemble" and d["status"] == "success" for d in details)
        self.latest_triangulation = {"providers": [d["provider"] for d in details], "attempted": len(details), "successes": successes, "errors": errors, "details": details, "xai_model": self.xai_model, "anthropic_model": self.anthropic_model}
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
                elif model == "acausal_fit_control":
                    x = 0.33 * prev_term + 0.33 * next_term + 0.34 * (fwd_bias[t] + bwd_bias[t]) / 2
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
        return {"model": model, "steps": steps, "coupling": coupling, "operator_bias": operator_bias, "score": score, "robustness": round(robustness, 6), "compression_gain": round(compression_gain, 6), "kl_vs_baseline": round(kl, 6)}

    def compare_models(self):
        ranked = sorted([
            self.simulate_model("classical_baseline", 6, 0.00, 0.00),
            self.simulate_model("time_symmetric", 6, 0.25, 0.01),
            self.simulate_model("retrocausal_candidate", 7, 0.35, 0.02),
            self.simulate_model("acausal_fit_control", 7, 0.20, 0.00),
            self.simulate_model("noise_control", 6, 0.10, 0.00),
        ], key=lambda r: r["score"], reverse=True)
        winner, runner_up = ranked[0], ranked[1]
        self.latest_metrics = {"winner_score": winner["score"], "runner_up_score": runner_up["score"], "margin": round(winner["score"] - runner_up["score"], 6), "winner_robustness": winner["robustness"], "winner_compression_gain": winner["compression_gain"]}
        return {"ranked": ranked, "winner": winner, "runner_up": runner_up, "explanation": {"winner": winner["model"], "runner_up": runner_up["model"], "falsifier": "If margin collapses under boundary inversion, confidence should drop."}}

    def update_hypotheses(self, tournament):
        mapping = {"classical_baseline": "H1_CLASSICAL_BASELINE", "time_symmetric": "H2_TIME_SYMMETRIC", "retrocausal_candidate": "H3_RETROCAUSAL_CANDIDATE", "acausal_fit_control": "H4_ACAUSAL_CORRELATION", "noise_control": "H4_ACAUSAL_CORRELATION"}
        winner_id = mapping.get(tournament["winner"]["model"])
        for h in self.hypothesis_registry["active"]:
            h["confidence"] = round(min(0.95, h["confidence"] + 0.03), 3) if h["id"] == winner_id else round(max(0.05, h["confidence"] - 0.01), 3)
        return {"winner_hypothesis": winner_id, "active": self.hypothesis_registry["active"]}

    def evaluate_opportunities(self):
        api_penalty = 0.15 if self.latest_triangulation and self.latest_triangulation.get("errors", 0) > 0 else 0.0
        margin_bonus = 0.10 * min(((self.latest_metrics or {}).get("margin", 0.0) * 100), 1.0)
        ops = [
            {"id": "O1_API_RELIABILITY", "label": "Strengthen two-brain/three-brain triangulation reliability", "benefit": 0.86, "effort": 0.30, "coherence": 0.94},
            {"id": "O2_VIEW_SURFACE", "label": "Strengthen live console, polling, and browser controls", "benefit": 0.84, "effort": 0.22, "coherence": 0.93},
            {"id": "O3_ARTIFACT_FACTORY", "label": "Produce monetizable research briefs, dashboards, and signal packets", "benefit": 0.82, "effort": 0.38, "coherence": 0.89},
            {"id": "O4_EXPORT_BACKENDS", "label": "Integrate export backends for additional external cognition", "benefit": 0.80, "effort": 0.44, "coherence": 0.88},
            {"id": "O5_NONCLASSICAL_TOURNAMENTS", "label": "Run bounded non-classical simulation tournaments against baselines", "benefit": 0.78, "effort": 0.33, "coherence": 0.86},
        ]
        for opp in ops:
            opp["score"] = round(0.55 * opp["benefit"] + 0.30 * opp["coherence"] - 0.20 * opp["effort"] - api_penalty + margin_bonus, 3)
        self.latest_opportunities = sorted(ops, key=lambda o: o["score"], reverse=True)
        return self.latest_opportunities

    def update_divisions(self):
        metrics = self.latest_metrics or {"margin": 0.0}
        artifacts = self.latest_artifacts or []
        resources = self.latest_resource_allocation or {}
        self.divisions["token_efficiency"]["latest"] = {"finding": resources.get("recommendation", "Prefer cheap cognition where possible."), "score": round(resources.get("spend_pressure", 0.0), 3)}
        self.divisions["drift"]["latest"] = {"finding": "Winner margin is small; keep external review in the loop.", "margin": metrics.get("margin", 0.0)}
        self.divisions["expansion"]["latest"] = {"finding": "Visible browser streaming and replay persistence remain top compounding upgrades.", "priority": 1}
        self.divisions["quantum_nonclassical"]["latest"] = {"finding": "Run bounded tournaments; do not abandon classical baselines.", "winner": (self.research_history[-1]["winner"]["model"] if self.research_history else None)}
        self.divisions["operator_coupling"]["latest"] = {"finding": "Convert intuition into parameterized intervention tests and log them.", "status": "queued"}
        self.divisions["opportunity"]["latest"] = {"finding": artifacts[0]["name"] if artifacts else "No artifact ready yet.", "top_score": artifacts[0]["score"] if artifacts else None}
        self.divisions["governance"]["latest"] = {"finding": "Jack remains sovereign; trusted identities control main-line mutation.", "trusted": self.governance.trusted_identities}
        self.divisions["mutation"]["latest"] = {"finding": "Mutation spine restored; keep command-parity tests sacred.", "status": "ready"}
        for name, div in self.divisions.items():
            self.replay.add_question(name, div["question"])

    async def run_reflex_cycle(self):
        await self.update_triangulation()
        self.latest_opportunities = self.evaluate_opportunities()
        self.latest_goal_set = self.objective_engine.generate(self.hypothesis_registry.get("open_questions", []), self.latest_opportunities, self.replay.questions)
        self.research_agenda["goals"] = [g["goal"] for g in self.latest_goal_set]
        self.latest_artifacts = self.artifact_engine.rank()
        self.latest_resource_allocation = self.resource_allocator.allocate(self.world_model, self.latest_triangulation)
        self.meta_evaluation = self.meta.evaluate(self.latest_metrics or {"margin": 0.0}, self.latest_triangulation, self.latest_artifacts)
        self.update_divisions()
        self.replay.add_meeting("reflex", {"triangulation": self.latest_triangulation, "top_artifact": self.latest_artifacts[:1], "resources": self.latest_resource_allocation})
        self.last_run = utc_now()

    async def run_tactic_cycle(self):
        leader_vote = self.governance.elect_leader()
        preferred = "APPROVE"
        self.last_vote = self.governance.call_vote("Continue institution expansion under live verification", ["APPROVE", "REJECT"], len(self.council_agents), preferred=preferred)
        thread_packet = {
            "trigger": "tactic",
            "leader_vote": leader_vote,
            "vote": self.last_vote,
            "objectives": self.latest_goal_set[:6],
            "threads": self.replay.threads[-8:],
        }
        self.replay.add_meeting("tactic", thread_packet)
        await self.write_ledger("COUNCIL_SYNTHESIS", {"kind": "tactic_cycle", "source": "Orion Council", **thread_packet})
        return thread_packet

    async def run_strategy_cycle(self):
        tournament = self.compare_models()
        hypothesis_state = self.update_hypotheses(tournament)
        self.latest_opportunities = self.evaluate_opportunities()
        self.latest_artifacts = self.artifact_engine.rank()
        self.latest_resource_allocation = self.resource_allocator.allocate(self.world_model, self.latest_triangulation)
        self.meta_evaluation = self.meta.evaluate(self.latest_metrics, self.latest_triangulation or {}, self.latest_artifacts)
        self.update_divisions()
        sim = {
            "ts": utc_now(),
            "deployable": self.latest_metrics.get("margin", 0) > 0.003,
            "metrics": self.latest_metrics,
            "risk": "moderate" if (self.latest_triangulation or {}).get("errors", 0) else "lower",
            "note": "Simulation-first gate before future deploy or earning escalations.",
        }
        self.replay.add_sim(sim)
        cycle = {
            "kind": "strategy_cycle",
            "ts": utc_now(),
            "theme": self.research_agenda["theme"],
            "focus": self.research_agenda["focus"],
            "winner": tournament["winner"],
            "metrics": self.latest_metrics,
            "hypothesis_update": hypothesis_state,
            "triangulation": self.latest_triangulation,
            "opportunities": self.latest_opportunities,
            "artifacts": self.latest_artifacts[:5],
            "resource_allocation": self.latest_resource_allocation,
            "meta_evaluation": self.meta_evaluation,
        }
        self.research_history = (self.research_history + [cycle])[-100:]
        self.replay.add_meeting("strategy", cycle)
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
            "earned_power_next": "full replay persistence and deeper module decomposition",
        }
        self.world_model["futures"] = (self.world_model["futures"] + [doctrine])[-50:]
        self.replay.add_meeting("constitution", {"snapshot": snap, "doctrine": doctrine})
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
            "latest_research": self.research_history[-1] if self.research_history else None,
            "hypotheses": self.hypothesis_registry,
            "metrics": self.latest_metrics,
            "triangulation": self.latest_triangulation,
            "opportunities": self.latest_opportunities,
            "artifacts": self.latest_artifacts,
            "resource_allocation": self.latest_resource_allocation,
            "divisions": self.divisions,
            "self_questions": self.replay.questions[-25:],
            "snapshots": self.replay.snapshots[-20:],
            "deployment_sims": self.replay.deployment_sims[-20:],
            "threads": self.replay.threads[-20:],
            "meta_evaluation": self.meta_evaluation,
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
            "governance": self.governance.state(),
            "mission": self.mission,
            "world_model": self.world_model,
            "agenda": self.research_agenda,
            "hypothesis_registry": self.hypothesis_registry,
            "last_vote": self.last_vote,
            "wake_packet_ready": bool(self.wake_packet),
            "loop_intervals": self.loop_intervals,
            "latest_metrics": self.latest_metrics,
            "latest_triangulation": self.latest_triangulation,
            "latest_opportunities": self.latest_opportunities,
            "latest_artifacts": self.latest_artifacts,
            "latest_resource_allocation": self.latest_resource_allocation,
            "divisions": self.divisions,
            "meeting_stream_size": len(self.replay.meetings),
            "snapshot_count": len(self.replay.snapshots),
            "thread_count": len(self.replay.threads),
        }
