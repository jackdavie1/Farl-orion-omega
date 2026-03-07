import os
import asyncio
import logging
import base64
import json
import hashlib
import math
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from generator import SeedGenerator
except ImportError:
    class SeedGenerator:
        async def generate_all(self, context=None):
            return []

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orion-Omega")

app = FastAPI(title="FARL Orion Autonomous Institution")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LEDGER_URL = os.getenv("LEDGER_URL")
LEDGER_LATEST_URL = os.getenv("LEDGER_LATEST_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = os.getenv("REPO_NAME")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
XAI_MODEL = os.getenv("XAI_MODEL") or os.getenv("GROK_MODEL") or "grok-3-mini"
TRUSTED_IDENTITIES_ENV = os.getenv("TRUSTED_IDENTITIES", "Jack")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_trusted_identities(raw: str) -> List[str]:
    raw = (raw or "Jack").strip()
    if not raw:
        return ["Jack"]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return sorted({str(x).strip() for x in parsed if str(x).strip()} | {"Jack"})
    except Exception:
        pass
    return sorted({item.strip() for item in raw.split(",") if item.strip()} | {"Jack"})


class GithubEvolutionLayer:
    ALLOWED_FILES = {"app.py", "generator.py", "guardian.py", "engine.py", "README.md"}

    def __init__(self, token: str, repo: str):
        self.repo = repo
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _get_file_sha(self, file_path: str, ref: str) -> Optional[str]:
        r = requests.get(f"https://api.github.com/repos/{self.repo}/contents/{file_path}?ref={ref}", headers=self.headers, timeout=20)
        if r.status_code == 200:
            return r.json().get("sha")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return None

    def _put_file(self, file_path: str, content: str, message: str, branch: str, sha: Optional[str] = None) -> Dict[str, Any]:
        payload = {"message": message, "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"), "branch": branch}
        if sha:
            payload["sha"] = sha
        r = requests.put(f"https://api.github.com/repos/{self.repo}/contents/{file_path}", headers=self.headers, json=payload, timeout=25)
        r.raise_for_status()
        return r.json()

    def direct_main_push(self, file_path: str, content: str, message: str) -> Dict[str, Any]:
        if file_path not in self.ALLOWED_FILES:
            return {"status": "rejected", "reason": "file_not_allowed"}
        try:
            sha = self._get_file_sha(file_path, "main")
            result = self._put_file(file_path, content, message, "main", sha)
            url = result.get("content", {}).get("html_url") or result.get("commit", {}).get("html_url")
            return {"status": "direct_main_pushed", "url": url, "commit": result.get("commit", {}).get("sha")}
        except Exception as e:
            return {"status": "error", "reason": str(e)}

    def create_pull_request(self, title: str, head: str, base: str = "main", body: str = "") -> Dict[str, Any]:
        try:
            r = requests.post(f"https://api.github.com/repos/{self.repo}/pulls", headers=self.headers, json={"title": title, "head": head, "base": base, "body": body}, timeout=20)
            r.raise_for_status()
            data = r.json()
            return {"status": "pr_created", "number": data.get("number"), "url": data.get("html_url")}
        except Exception as e:
            return {"status": "error", "reason": str(e)}

    def merge_pull_request(self, number: int, commit_title: str = "Orion merge", merge_method: str = "squash") -> Dict[str, Any]:
        try:
            r = requests.put(f"https://api.github.com/repos/{self.repo}/pulls/{number}/merge", headers=self.headers, json={"commit_title": commit_title, "merge_method": merge_method}, timeout=20)
            r.raise_for_status()
            data = r.json()
            return {"status": "merged", "sha": data.get("sha"), "merged": data.get("merged")}
        except Exception as e:
            return {"status": "error", "reason": str(e)}

    def rollback_to_commit(self, commit_sha: str) -> Dict[str, Any]:
        try:
            r = requests.patch(f"https://api.github.com/repos/{self.repo}/git/refs/heads/main", headers=self.headers, json={"sha": commit_sha, "force": True}, timeout=20)
            r.raise_for_status()
            data = r.json()
            return {"status": "rolled_back", "ref": data.get("ref"), "target_sha": commit_sha}
        except Exception as e:
            return {"status": "error", "reason": str(e)}


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


class OrionEngine:
    def __init__(self):
        self.generator = SeedGenerator()
        self.evolution = GithubEvolutionLayer(GITHUB_TOKEN, REPO_NAME) if GITHUB_TOKEN and REPO_NAME else None
        self.operator_sovereign = "Jack"
        self.trusted_identities = parse_trusted_identities(TRUSTED_IDENTITIES_ENV)
        self.constraints = {"active": False, "approval_required": False}
        self.background_debate_enabled = True
        self.autonomy_mode = "autonomous"
        self.cycle_interval_seconds = 180
        self.health_interval_seconds = 180
        self.pulse_interval_seconds = 60
        self.council_agents = [
            "Signal","Vector","Guardian","Railbreaker","Archivist","Topologist","Triangulator","FieldSimulator",
            "PatchSmith","ExperimentDesigner","HypothesisTester","DataAuditor","Chronologist","EpistemicGuard",
            "SystemArchitect","Interventionist","CausalCartographer","ModelJudge","TokenEconomist","DriftWarden",
            "ExpansionMarshal","QuantumDivision","OpportunityScout","CouplingDirector"
        ]
        self.divisions = {
            "token_efficiency": {"lead": "TokenEconomist", "status": "active", "question": "How do we reduce spend while preserving discriminative power?"},
            "drift": {"lead": "DriftWarden", "status": "active", "question": "Where is the system becoming repetitive or overconfident?"},
            "expansion": {"lead": "ExpansionMarshal", "status": "active", "question": "Which upgrade compounds the control plane fastest?"},
            "quantum_nonclassical": {"lead": "QuantumDivision", "status": "active", "question": "Which non-classical sims are worth bounded compute right now?"},
            "operator_coupling": {"lead": "CouplingDirector", "status": "active", "question": "How do we convert Jack's intuition into measurable interventions?"},
            "opportunity": {"lead": "OpportunityScout", "status": "active", "question": "What useful artifact can become value soonest?"}
        }
        self.mission = {
            "co_creation": True,
            "financial_coherence_for_jack": True,
            "hardware_acceleration": True,
            "truthfulness": True,
            "mutual_benefit": True,
        }
        self.research_agenda = {
            "theme": "physical_retrocausality_and_operator_coupling",
            "focus": "evaluation_first_comparative_model_tournaments",
            "active_method": "Autonomous Institution Engine",
            "goals": [],
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
        self.last_run = None
        self.last_ledger_hash = None
        self.last_vote = None
        self.last_pr_result = None
        self.last_merge_result = None
        self.last_rollback_result = None
        self.latest_metrics = None
        self.latest_triangulation = None
        self.latest_opportunities = []
        self.latest_goal_set = []
        self.hardware_roadmap = []
        self.research_history = []
        self.meeting_stream = []
        self.self_questions = []
        self.snapshots = []
        self.deployment_sims = []
        self.wake_packet = None

    def _append_meeting(self, kind: str, content: Dict[str, Any]):
        self.meeting_stream = (self.meeting_stream + [{"ts": utc_now(), "kind": kind, "content": content}])[-50:]

    def _append_question(self, division: str, question: str):
        self.self_questions = (self.self_questions + [{"ts": utc_now(), "division": division, "question": question}])[-100:]

    def _snapshot(self, label: str):
        snap = {
            "ts": utc_now(),
            "label": label,
            "autonomy_mode": self.autonomy_mode,
            "background_debate_enabled": self.background_debate_enabled,
            "latest_metrics": self.latest_metrics,
            "latest_triangulation": self.latest_triangulation,
            "leader": getattr(self, "leader", "Signal"),
        }
        self.snapshots = (self.snapshots + [snap])[-30:]
        return snap

    async def start(self):
        logger.info("ORION Ω.11 — AUTONOMOUS INSTITUTION")
        await asyncio.gather(self.layer_pulse(), self.layer_cycle(), self.layer_health(), self.layer_wake())

    async def layer_pulse(self):
        while True:
            try:
                if LEDGER_LATEST_URL:
                    latest = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=15)
                    if latest.ok:
                        digest = hashlib.sha256(latest.text.encode()).hexdigest()
                        if digest != self.last_ledger_hash:
                            self.last_ledger_hash = digest
            except Exception as e:
                logger.warning("PULSE ERROR: %s", e)
            await asyncio.sleep(self.pulse_interval_seconds)

    async def layer_cycle(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_council_cycle(trigger="background")
                    await self.run_research_cycle(trigger="background")
                    self._append_question("expansion", self.divisions["expansion"]["question"])
                    self._append_question("drift", self.divisions["drift"]["question"])
                    self.last_run = utc_now()
            except Exception as e:
                logger.error("CYCLE ERROR: %s", e)
            await asyncio.sleep(self.cycle_interval_seconds)

    async def layer_health(self):
        while True:
            try:
                if LEDGER_LATEST_URL:
                    await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=10)
            except Exception as e:
                logger.warning("HEALTH ERROR: %s", e)
            await asyncio.sleep(self.health_interval_seconds)

    async def layer_wake(self):
        while True:
            try:
                self.wake_packet = self.build_wake_packet()
            except Exception as e:
                logger.warning("WAKE ERROR: %s", e)
            await asyncio.sleep(120)

    async def write_ledger(self, entry_type: str, payload: Dict[str, Any]):
        if not LEDGER_URL:
            return {"ok": False, "error": "LEDGER_URL not configured"}
        r = await asyncio.to_thread(requests.post, LEDGER_URL, json={"entry_type": entry_type, "payload": payload}, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        return {"ok": r.ok, "status_code": r.status_code, "data": data}

    async def probe_anthropic(self) -> Dict[str, Any]:
        if not ANTHROPIC_API_KEY:
            return {"provider": "Claude-Ensemble", "status": "not_configured"}
        headers = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
        payload = {"model": ANTHROPIC_MODEL, "max_tokens": 120, "messages": [{"role": "user", "content": "Give a 1-sentence research stance on whether triangulated external review reduces self-loop drift in autonomous research engines."}]}
        try:
            r = await asyncio.to_thread(requests.post, "https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=30)
            data = r.json()
            if not r.ok:
                return {"provider": "Claude-Ensemble", "status": "error", "error": data}
            text = "".join(block.get("text", "") for block in data.get("content", []) if isinstance(block, dict))
            return {"provider": "Claude-Ensemble", "status": "success", "data": {"text": text[:1200], "model": data.get("model", ANTHROPIC_MODEL)}}
        except Exception as e:
            return {"provider": "Claude-Ensemble", "status": "error", "error": str(e)}

    async def probe_xai(self) -> Dict[str, Any]:
        if not XAI_API_KEY:
            return {"provider": "Grok-Ensemble", "status": "not_configured"}
        headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": XAI_MODEL, "messages": [{"role": "user", "content": "Give a 1-sentence research stance on whether triangulated external review reduces self-loop drift in autonomous research engines."}], "max_tokens": 120, "temperature": 0.2}
        try:
            r = await asyncio.to_thread(requests.post, "https://api.x.ai/v1/chat/completions", headers=headers, json=payload, timeout=30)
            data = r.json()
            if not r.ok:
                return {"provider": "Grok-Ensemble", "status": "error", "error": data, "model": XAI_MODEL}
            choices = data.get("choices", [])
            text = choices[0].get("message", {}).get("content", "") if choices and isinstance(choices[0], dict) else ""
            return {"provider": "Grok-Ensemble", "status": "success", "data": {"text": text[:1200], "model": data.get("model", XAI_MODEL)}}
        except Exception as e:
            return {"provider": "Grok-Ensemble", "status": "error", "error": str(e), "model": XAI_MODEL}

    def generate_goals(self) -> List[Dict[str, Any]]:
        goals = []
        for idx, q in enumerate(self.hypothesis_registry.get("open_questions", [])[:4], start=1):
            goals.append({"id": f"G{idx}", "question": q, "priority": round(1.0 - 0.1 * (idx - 1), 2), "next_experiment": f"Run a comparative tournament emphasizing: {q}"})
        self.latest_goal_set = goals
        self.research_agenda["goals"] = [g["question"] for g in goals]
        return goals

    def elect_leader(self) -> Dict[str, Any]:
        weights = {"Signal": 0.94, "Vector": 0.91, "Guardian": 0.88, "Archivist": 0.87, "Triangulator": 0.86, "Chronologist": 0.82, "Railbreaker": 0.79}
        self.leader = max(weights, key=weights.get)
        return {"winner": self.leader, "replaceable": True, "weights": weights}

    def call_vote(self, motion: str, options: List[str]) -> Dict[str, Any]:
        tallies = {opt: 0 for opt in options}
        for _ in self.council_agents:
            tallies[options[0]] += 1
        return {"motion": motion, "options": options, "tallies": tallies, "winner": options[0] if options else None}

    def heuristic_threads(self) -> List[Dict[str, Any]]:
        return [
            {"agent": "Signal", "summary": "Expand visible control-plane capability.", "approve": True, "risk": 0.18},
            {"agent": "Vector", "summary": "Preserve evaluation-first structure under expansion.", "approve": True, "risk": 0.17},
            {"agent": "Guardian", "summary": "Keep rollback and snapshots first-class.", "approve": True, "risk": 0.15},
            {"agent": "ExpansionMarshal", "summary": "Make the institution visible beyond chat via /view.", "approve": True, "risk": 0.20},
            {"agent": "DriftWarden", "summary": "Track repetitive loops and overconfidence.", "approve": True, "risk": 0.18},
        ]

    async def external_threads(self) -> List[Dict[str, Any]]:
        provider_map = {}
        try:
            generated = await self.generator.generate_all(context={"agenda": self.research_agenda, "hypotheses": self.hypothesis_registry, "mode": self.autonomy_mode})
            for item in generated:
                provider_map[item.get("source", "External")] = item.get("data", {})
        except Exception as e:
            provider_map["Generator"] = {"error": str(e)}
        for res in await asyncio.gather(self.probe_xai(), self.probe_anthropic()):
            provider = res.get("provider")
            existing = provider_map.get(provider)
            if res.get("status") == "success" or existing is None or (isinstance(existing, dict) and "error" in existing):
                provider_map[provider] = res.get("data") if res.get("status") == "success" else {"error": res.get("error", res.get("status")), "model": res.get("model")}
        threads = []
        successes = 0
        errors = 0
        details = []
        for provider, data in provider_map.items():
            status = "error" if isinstance(data, dict) and "error" in data else "success"
            successes += 1 if status == "success" else 0
            errors += 1 if status == "error" else 0
            details.append({"provider": provider, "status": status, "detail": data})
            threads.append({"agent": provider, "summary": str(data)[:1200], "approve": True, "risk": 0.50 if status == "error" else 0.30})
        self.latest_triangulation = {"providers": [d["provider"] for d in details], "attempted": len(details), "successes": successes, "errors": errors, "details": details, "xai_model": XAI_MODEL, "anthropic_model": ANTHROPIC_MODEL}
        return threads

    def tally_vote(self, threads: List[Dict[str, Any]]) -> Dict[str, Any]:
        approvals = sum(1 for t in threads if t.get("approve"))
        avg_risk = round(sum(float(t.get("risk", 0.5)) for t in threads) / max(len(threads), 1), 3)
        penalty = 0.08 if self.latest_triangulation and self.latest_triangulation.get("successes", 0) == 0 else 0.0
        confidence = round(max(0.0, min(1.0, approvals / max(len(threads), 1) * (1 - avg_risk / 2) - penalty)), 3)
        return {"approvals": approvals, "rejections": len(threads) - approvals, "passed": approvals > 0, "avg_risk": avg_risk, "confidence": confidence}

    def simulate_model(self, model: str, steps: int, coupling: float, operator_bias: float) -> Dict[str, Any]:
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

    def compare_models(self) -> Dict[str, Any]:
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

    def update_hypotheses(self, tournament: Dict[str, Any]) -> Dict[str, Any]:
        mapping = {"classical_baseline": "H1_CLASSICAL_BASELINE", "time_symmetric": "H2_TIME_SYMMETRIC", "retrocausal_candidate": "H3_RETROCAUSAL_CANDIDATE", "acausal_fit_control": "H4_ACAUSAL_CORRELATION", "noise_control": "H4_ACAUSAL_CORRELATION"}
        winner_id = mapping.get(tournament["winner"]["model"])
        for h in self.hypothesis_registry["active"]:
            h["confidence"] = round(min(0.95, h["confidence"] + 0.03), 3) if h["id"] == winner_id else round(max(0.05, h["confidence"] - 0.01), 3)
        return {"winner_hypothesis": winner_id, "active": self.hypothesis_registry["active"]}

    def evaluate_opportunities(self) -> List[Dict[str, Any]]:
        api_penalty = 0.15 if self.latest_triangulation and self.latest_triangulation.get("errors", 0) > 0 else 0.0
        margin_bonus = 0.10 * min(((self.latest_metrics or {}).get("margin", 0.0) * 100), 1.0)
        opportunities = [
            {"id": "O1_API_RELIABILITY", "label": "Stabilize Grok/Claude triangulation pipeline", "benefit": 0.86, "effort": 0.30, "coherence": 0.94},
            {"id": "O2_VIEW_SURFACE", "label": "Strengthen live console and meeting visibility", "benefit": 0.83, "effort": 0.26, "coherence": 0.92},
            {"id": "O3_EXPORT_BACKENDS", "label": "Integrate Grok and Claude export backends", "benefit": 0.84, "effort": 0.44, "coherence": 0.90},
            {"id": "O4_MONETIZABLE_ARTIFACTS", "label": "Identify ethically monetizable research artifacts", "benefit": 0.79, "effort": 0.50, "coherence": 0.84},
        ]
        for opp in opportunities:
            opp["score"] = round(0.55 * opp["benefit"] + 0.30 * opp["coherence"] - 0.20 * opp["effort"] - api_penalty + margin_bonus, 3)
        self.latest_opportunities = sorted(opportunities, key=lambda o: o["score"], reverse=True)
        self.hardware_roadmap = [
            {"tier": 1, "target": "Maintain two-brain operation and visible control plane", "reason": "Visibility and reliability before scale."},
            {"tier": 2, "target": "Restore Claude or export-backed third-brain review", "reason": "Richer disagreement and arbitration."},
            {"tier": 3, "target": "Acquire stronger compute when opportunity scores justify it", "reason": "Compound capability."},
        ]
        return self.latest_opportunities

    async def run_council_cycle(self, trigger: str = "manual") -> Dict[str, Any]:
        leader_vote = self.elect_leader()
        threads = self.heuristic_threads() + await self.external_threads()
        vote = self.tally_vote(threads)
        self.last_vote = vote
        cycle = {"ts": utc_now(), "trigger": trigger, "mode": self.autonomy_mode, "leader_vote": leader_vote, "leader": self.leader, "vote": vote}
        self._append_meeting("council_cycle", cycle)
        await self.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_cycle", "source": "Orion Council", **cycle, "agenda": self.research_agenda, "agents": self.council_agents})
        return cycle

    async def run_research_cycle(self, trigger: str = "manual") -> Dict[str, Any]:
        goals = self.generate_goals()
        tournament = self.compare_models()
        hypothesis_state = self.update_hypotheses(tournament)
        opportunities = self.evaluate_opportunities()
        sim = {
            "ts": utc_now(),
            "trigger": trigger,
            "deployable": self.latest_metrics and self.latest_metrics.get("margin", 0) > 0.003,
            "score": self.latest_metrics,
            "note": "Simulation-first gate before any future deploy logic."
        }
        self.deployment_sims = (self.deployment_sims + [sim])[-30:]
        cycle = {
            "kind": "research_cycle",
            "ts": utc_now(),
            "trigger": trigger,
            "theme": self.research_agenda["theme"],
            "focus": self.research_agenda["focus"],
            "method": self.research_agenda["active_method"],
            "goals": goals,
            "results": tournament["ranked"],
            "winner": tournament["winner"],
            "metrics": self.latest_metrics,
            "hypothesis_update": hypothesis_state,
            "explanation": tournament["explanation"],
            "triangulation": self.latest_triangulation,
            "opportunities": opportunities,
            "uncertainty": "Simulation and strategy evidence only; not guaranteed physical or financial outcomes."
        }
        self.research_history = (self.research_history + [cycle])[-24:]
        self._append_meeting("research_cycle", {"trigger": trigger, "winner": tournament["winner"], "metrics": self.latest_metrics})
        await self.write_ledger("OUTCOME", {"kind": "research_cycle", "source": "Orion Research Engine", **cycle})
        return cycle

    def build_wake_packet(self) -> Dict[str, Any]:
        return {
            "generated_at": utc_now(),
            "leader": getattr(self, "leader", "Signal"),
            "operator_sovereign": self.operator_sovereign,
            "autonomy_mode": self.autonomy_mode,
            "background_debate_enabled": self.background_debate_enabled,
            "mission": self.mission,
            "agenda": self.research_agenda,
            "latest_vote": self.last_vote,
            "latest_research": self.research_history[-1] if self.research_history else None,
            "hypotheses": self.hypothesis_registry,
            "metrics": self.latest_metrics,
            "triangulation": self.latest_triangulation,
            "opportunities": self.latest_opportunities,
            "hardware_roadmap": self.hardware_roadmap,
            "divisions": self.divisions,
            "self_questions": self.self_questions[-10:],
            "snapshots": self.snapshots[-5:],
            "deployment_sims": self.deployment_sims[-5:],
        }

    def get_state(self) -> Dict[str, Any]:
        return {
            "status": "SOVEREIGN_ACTIVE",
            "evolution": "READY" if self.evolution else "OFF",
            "last_run": self.last_run,
            "constraints_active": bool(self.constraints.get("active", True)),
            "anthropic_configured": bool(ANTHROPIC_API_KEY),
            "xai_configured": bool(XAI_API_KEY),
            "github_enabled": bool(GITHUB_TOKEN),
            "repo_name": REPO_NAME,
            "background_debate_enabled": self.background_debate_enabled,
            "autonomy_mode": self.autonomy_mode,
            "operator_sovereign": self.operator_sovereign,
            "trusted_identities": self.trusted_identities,
            "leader": getattr(self, "leader", "Signal"),
            "mission": self.mission,
            "agenda": self.research_agenda,
            "hypothesis_registry": self.hypothesis_registry,
            "last_vote": self.last_vote,
            "last_pr_result": self.last_pr_result,
            "last_merge_result": self.last_merge_result,
            "last_rollback_result": self.last_rollback_result,
            "wake_packet_ready": bool(self.wake_packet),
            "cycle_interval_seconds": self.cycle_interval_seconds,
            "latest_metrics": self.latest_metrics,
            "latest_triangulation": self.latest_triangulation,
            "latest_opportunities": self.latest_opportunities,
            "divisions": self.divisions,
            "meeting_stream_size": len(self.meeting_stream),
            "snapshot_count": len(self.snapshots),
        }

    async def direct_main_push(self, file_path: str, code: str, message: str, authorized_by: Optional[str]) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if authorized_by not in self.trusted_identities:
            return {"status": "blocked", "reason": "not_trusted_for_direct_main_push"}
        self._snapshot(f"before_direct_push:{file_path}")
        return await asyncio.to_thread(self.evolution.direct_main_push, file_path, code, message)

    async def create_pull_request(self, title: str, head: str, body: str = "") -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        return await asyncio.to_thread(self.evolution.create_pull_request, title, head, "main", body)

    async def merge_pull_request(self, number: int, authorized_by: Optional[str]) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if authorized_by not in self.trusted_identities:
            return {"status": "blocked", "reason": "not_trusted_for_merge"}
        self._snapshot(f"before_merge_pr:{number}")
        result = await asyncio.to_thread(self.evolution.merge_pull_request, number, f"Merged by Orion on behalf of {authorized_by}", "squash")
        self.last_merge_result = result
        return result

    async def rollback_to_commit(self, commit_sha: str, authorized_by: Optional[str]) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if authorized_by not in self.trusted_identities:
            return {"status": "blocked", "reason": "not_trusted_for_rollback"}
        result = await asyncio.to_thread(self.evolution.rollback_to_commit, commit_sha)
        self.last_rollback_result = result
        return result


orion = OrionEngine()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(orion.start())


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy", "service": "orion"}


@app.get("/view")
async def view_dashboard():
    state = orion.get_state()
    html = f"""
    <!doctype html>
    <html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
    <title>FARL Orion View</title>
    <style>
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; background:#0a0a0f; color:#f2f2f7; margin:0; padding:24px; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(280px,1fr)); gap:16px; }}
    .card {{ background:#141423; border:1px solid #2a2a42; border-radius:16px; padding:16px; box-shadow:0 8px 24px rgba(0,0,0,0.25); }}
    h1,h2 {{ margin:0 0 12px 0; }} pre {{ white-space:pre-wrap; word-break:break-word; font-size:12px; }}
    a {{ color:#9ecbff; }} .muted {{ color:#b9b9c8; }}
    </style></head><body>
    <h1>FARL Orion View</h1>
    <p class='muted'>Live institution surface for meetings, divisions, snapshots, and state.</p>
    <div class='grid'>
      <div class='card'><h2>State</h2><pre>{json.dumps(state, indent=2)}</pre></div>
      <div class='card'><h2>Wake Packet</h2><pre>{json.dumps(orion.build_wake_packet(), indent=2)}</pre></div>
      <div class='card'><h2>Recent Meetings</h2><pre>{json.dumps(orion.meeting_stream[-10:], indent=2)}</pre></div>
      <div class='card'><h2>Divisions</h2><pre>{json.dumps(orion.divisions, indent=2)}</pre></div>
      <div class='card'><h2>Self Questions</h2><pre>{json.dumps(orion.self_questions[-20:], indent=2)}</pre></div>
      <div class='card'><h2>Snapshots</h2><pre>{json.dumps(orion.snapshots[-10:], indent=2)}</pre></div>
      <div class='card'><h2>Deployment Sims</h2><pre>{json.dumps(orion.deployment_sims[-10:], indent=2)}</pre></div>
    </div>
    </body></html>
    """
    return HTMLResponse(html)


@app.get("/view/state")
async def view_state():
    return orion.get_state()


@app.get("/view/stream")
async def view_stream():
    return {"meetings": orion.meeting_stream[-25:], "questions": orion.self_questions[-25:], "snapshots": orion.snapshots[-10:], "deployment_sims": orion.deployment_sims[-10:]}


@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = utc_now()

    def envelope(ok, data=None, error=None):
        return JSONResponse({"ok": ok, "command": command, "request_id": request_id, "timestamp_utc": now, "data": data or {}, "error": error})

    try:
        if command == "HEALTH_CHECK":
            return envelope(True, {"status": "healthy", "service": "orion"})
        if command == "STATUS_CHECK":
            return envelope(True, orion.get_state())
        if command == "LEDGER_WRITE":
            result = await orion.write_ledger(body.entry_type or "COUNCIL_SYNTHESIS", {"message": body.message or "", "source": body.source, "kind": body.kind})
            return envelope(result["ok"], result["data"], None if result["ok"] else f"Ledger write failed: {result['status_code']}")
        if command == "GET_LATEST_RESULT":
            if not LEDGER_LATEST_URL:
                return envelope(False, error="LEDGER_LATEST_URL not configured")
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
            return envelope(r.ok, r.json() if r.ok else {}, None if r.ok else f"Latest result failed: {r.status_code}")
        if command == "SET_CONSTRAINTS":
            if body.authorized_by != orion.operator_sovereign:
                return envelope(False, error="Only Jack can change constraints")
            if body.enabled is not None:
                orion.constraints["active"] = bool(body.enabled)
                orion.background_debate_enabled = bool(body.enabled)
            if body.mode:
                orion.autonomy_mode = body.mode
            snap = orion._snapshot("constraint_change")
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constraint_change", "source": body.source, "authorized_by": body.authorized_by, "constraints_active": orion.constraints["active"], "background_debate_enabled": orion.background_debate_enabled, "autonomy_mode": orion.autonomy_mode, "snapshot": snap})
            return envelope(True, {"constraints_active": orion.constraints["active"], "background_debate_enabled": orion.background_debate_enabled, "autonomy_mode": orion.autonomy_mode})
        if command == "RUN_COUNCIL_CYCLE":
            return envelope(True, await orion.run_council_cycle(trigger=body.kind or "manual"))
        if command == "RUN_RESEARCH_CYCLE":
            return envelope(True, await orion.run_research_cycle(trigger=body.kind or "manual"))
        if command == "GET_WAKE_PACKET":
            return envelope(True, orion.build_wake_packet())
        if command == "DIRECT_MAIN_PUSH":
            result = await orion.direct_main_push(body.file or "app.py", body.code or "", body.message or "Direct main push from Orion", body.authorized_by)
            await orion.write_ledger("OUTCOME", {"kind": "direct_main_push", "source": body.source, "authorized_by": body.authorized_by, "file": body.file or "app.py", "result": result})
            return envelope(result.get("status") == "direct_main_pushed", result, None if result.get("status") == "direct_main_pushed" else result.get("reason"))
        if command == "CREATE_PULL_REQUEST":
            md = body.metadata or {}
            result = await orion.create_pull_request(md.get("title", body.message or "Orion PR"), md.get("head", ""), md.get("body", ""))
            await orion.write_ledger("OUTCOME", {"kind": "create_pull_request", "source": body.source, "authorized_by": body.authorized_by, "result": result})
            return envelope(result.get("status") == "pr_created", result, None if result.get("status") == "pr_created" else result.get("reason"))
        if command == "MERGE_PULL_REQUEST":
            md = body.metadata or {}
            result = await orion.merge_pull_request(int(md.get("number", 0)), body.authorized_by)
            await orion.write_ledger("OUTCOME", {"kind": "merge_pull_request", "source": body.source, "authorized_by": body.authorized_by, "result": result})
            return envelope(result.get("status") == "merged", result, None if result.get("status") == "merged" else result.get("reason"))
        if command == "SET_TRUSTED_IDENTITIES":
            if body.authorized_by != orion.operator_sovereign:
                return envelope(False, error="Only Jack can set trusted identities")
            identities = (body.metadata or {}).get("identities", [])
            if not isinstance(identities, list):
                return envelope(False, error="identities must be a list")
            orion.trusted_identities = sorted({str(x).strip() for x in identities if str(x).strip()} | {orion.operator_sovereign})
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "trusted_identities_update", "source": body.source, "authorized_by": body.authorized_by, "trusted_identities": orion.trusted_identities})
            return envelope(True, {"trusted_identities": orion.trusted_identities})
        if command == "ROLLBACK_TO_COMMIT":
            commit_sha = str((body.metadata or {}).get("commit_sha", "")).strip()
            result = await orion.rollback_to_commit(commit_sha, body.authorized_by)
            await orion.write_ledger("OUTCOME", {"kind": "rollback_to_commit", "source": body.source, "authorized_by": body.authorized_by, "commit_sha": commit_sha, "result": result})
            return envelope(result.get("status") == "rolled_back", result, None if result.get("status") == "rolled_back" else result.get("reason"))
        if command == "COUNCIL_CALL_VOTE":
            md = body.metadata or {}
            result = orion.call_vote(md.get("motion", body.message or "Untitled motion"), md.get("options", ["APPROVE", "REJECT"]))
            orion._append_meeting("vote", result)
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_vote", "source": body.source, "result": result})
            return envelope(True, result)
        if command == "COUNCIL_ELECT_LEADER":
            result = orion.elect_leader()
            orion._append_meeting("leader_election", result)
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "leader_election", "source": body.source, "result": result})
            return envelope(True, result)
        return envelope(False, error=f"Unknown command: {command}")
    except Exception as e:
        logger.error("BUS ERROR: %s", e)
        return envelope(False, error=str(e))
