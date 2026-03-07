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
from fastapi.responses import JSONResponse
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

app = FastAPI(title="FARL Orion Co-Creative Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LEDGER_URL = os.getenv("LEDGER_URL")
LEDGER_LATEST_URL = os.getenv("LEDGER_LATEST_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_NAME = os.getenv("REPO_NAME")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
XAI_MODEL = os.getenv("XAI_MODEL", "grok-2-latest")
TRUSTED_IDENTITIES_ENV = os.getenv("TRUSTED_IDENTITIES", "Jack")


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
        self.cycle_interval_seconds = 240
        self.health_interval_seconds = 180
        self.pulse_interval_seconds = 60
        self.council_agents = ["Signal","Vector","Guardian","Railbreaker","Archivist","Topologist","Triangulator","FieldSimulator","PatchSmith","ExperimentDesigner","HypothesisTester","DataAuditor","Chronologist","EpistemicGuard","SystemArchitect","Interventionist","CausalCartographer","ModelJudge"]
        self.leader = "Signal"
        self.mission = {"co_creation": True, "financial_coherence_for_jack": True, "hardware_acceleration": True, "truthfulness": True, "mutual_benefit": True}
        self.research_agenda = {"theme": "physical_retrocausality_and_operator_coupling", "focus": "evaluation_first_comparative_model_tournaments", "active_method": "Co-Creative Frontier Research Engine", "goals": []}
        self.hypothesis_registry = {"active": [{"id": "H1_CLASSICAL_BASELINE", "label": "Forward-only causal baseline explains observations adequately.", "status": "active", "confidence": 0.45}, {"id": "H2_TIME_SYMMETRIC", "label": "Time-symmetric consistency models improve coherence without implying physical retrocausality.", "status": "active", "confidence": 0.50}, {"id": "H3_RETROCAUSAL_CANDIDATE", "label": "Retrocausal candidate models provide better explanatory compression under boundary constraints.", "status": "active", "confidence": 0.35}, {"id": "H4_ACAUSAL_CORRELATION", "label": "Observed gains may arise from acausal fitting artifacts rather than causal direction.", "status": "active", "confidence": 0.40}], "rejected": [], "open_questions": ["Do bidirectional constraints improve robustness under perturbation?", "Can operator coupled boundary conditions discriminate model classes?", "Which evaluation metric best predicts durable model advantage?", "Can triangulated API summaries reduce self-loop drift?", "Which coherent opportunity path best accelerates Jack's hardware access?"]}
        self.last_run = None
        self.last_ledger_hash = None
        self.last_vote = None
        self.last_pr_result = None
        self.last_merge_result = None
        self.last_rollback_result = None
        self.wake_packet = None
        self.research_history: List[Dict[str, Any]] = []
        self.latest_metrics: Optional[Dict[str, Any]] = None
        self.latest_goal_set: List[Dict[str, Any]] = []
        self.latest_triangulation: Optional[Dict[str, Any]] = None
        self.latest_opportunities: List[Dict[str, Any]] = []
        self.hardware_roadmap: List[Dict[str, Any]] = []
        self.agent_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    def is_trusted(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.trusted_identities

    def can_direct_main_push(self, identity: Optional[str]) -> bool:
        return identity == self.operator_sovereign or self.is_trusted(identity)

    async def start(self):
        logger.info("ORION Ω.10 — TRIANGULATION STABILIZED ENGINE")
        await asyncio.gather(self.layer_1_pulse(), self.layer_2_cycle(), self.layer_3_health(), self.layer_4_agent_connector(), self.layer_5_wake_packet())

    async def layer_1_pulse(self):
        while True:
            try:
                latest = await self.fetch_latest_result()
                if latest is not None:
                    digest = hashlib.sha256(json.dumps(latest, sort_keys=True).encode()).hexdigest()
                    if digest != self.last_ledger_hash:
                        self.last_ledger_hash = digest
            except Exception as e:
                logger.error("LAYER1 ERROR: %s", e)
            await asyncio.sleep(self.pulse_interval_seconds)

    async def layer_2_cycle(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_council_cycle(trigger="background", auto_deploy=False, authorized_by=self.operator_sovereign)
                    await self.run_research_cycle(trigger="background")
                    self.last_run = datetime.now(timezone.utc).isoformat()
            except Exception as e:
                logger.error("LAYER2 ERROR: %s", e)
            await asyncio.sleep(self.cycle_interval_seconds)

    async def layer_3_health(self):
        while True:
            try:
                if LEDGER_LATEST_URL:
                    await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=10)
            except Exception as e:
                logger.warning("HEALTH_CHECK_FAILED: %s", e)
            await asyncio.sleep(self.health_interval_seconds)

    async def layer_4_agent_connector(self):
        while True:
            try:
                await asyncio.wait_for(self.agent_queue.get(), timeout=30)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error("LAYER4 ERROR: %s", e)

    async def layer_5_wake_packet(self):
        while True:
            try:
                self.wake_packet = self.build_wake_packet()
            except Exception as e:
                logger.error("LAYER5 ERROR: %s", e)
            await asyncio.sleep(300)

    async def fetch_latest_result(self):
        if not LEDGER_LATEST_URL:
            return None
        r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
        if not r.ok:
            raise RuntimeError(f"Latest result failed: {r.status_code}")
        return r.json()

    async def write_ledger(self, entry_type: str, payload: Dict[str, Any]):
        if not LEDGER_URL:
            return {"ok": False, "error": "LEDGER_URL not configured"}
        r = await asyncio.to_thread(requests.post, LEDGER_URL, json={"entry_type": entry_type, "payload": payload}, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        return {"ok": r.ok, "status_code": r.status_code, "data": data}

    def elect_leader(self) -> Dict[str, Any]:
        weights = {"Signal": 0.94, "Vector": 0.91, "Guardian": 0.88, "Archivist": 0.87, "Triangulator": 0.86, "Chronologist": 0.82, "Railbreaker": 0.79}
        self.leader = max(weights, key=weights.get)
        return {"winner": self.leader, "replaceable": True, "weights": weights}

    def call_vote(self, motion: str, options: List[str]) -> Dict[str, Any]:
        tallies = {opt: 0 for opt in options}
        for _ in self.council_agents:
            tallies[options[0]] += 1
        return {"motion": motion, "options": options, "tallies": tallies, "winner": options[0] if options else None}

    def generate_goals(self) -> List[Dict[str, Any]]:
        goals = []
        for idx, q in enumerate(self.hypothesis_registry.get("open_questions", [])[:4], start=1):
            goals.append({"id": f"G{idx}", "question": q, "priority": round(1.0 - 0.1 * (idx - 1), 2), "next_experiment": f"Run a comparative tournament emphasizing: {q}"})
        self.latest_goal_set = goals
        self.research_agenda["goals"] = [g["question"] for g in goals]
        return goals

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
            text = ""
            for block in data.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text += block.get("text", "")
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
                return {"provider": "Grok-Ensemble", "status": "error", "error": data}
            choices = data.get("choices", [])
            text = ""
            if choices and isinstance(choices[0], dict):
                text = choices[0].get("message", {}).get("content", "")
            return {"provider": "Grok-Ensemble", "status": "success", "data": {"text": text[:1200], "model": data.get("model", XAI_MODEL)}}
        except Exception as e:
            return {"provider": "Grok-Ensemble", "status": "error", "error": str(e)}

    def heuristic_threads(self) -> List[Dict[str, Any]]:
        return [
            {"agent": "Signal", "stance": "leadership", "summary": "Restore true three-brain triangulation and keep evaluation first.", "approve": True, "risk": 0.18},
            {"agent": "Vector", "stance": "structural", "summary": "Provider failures must degrade confidence explicitly instead of being silently ignored.", "approve": True, "risk": 0.17},
            {"agent": "Guardian", "stance": "safety", "summary": "No pretending failed APIs contributed. Surface failures honestly.", "approve": True, "risk": 0.15},
            {"agent": "Triangulator", "stance": "external", "summary": "Add direct fallbacks and per-provider diagnostics.", "approve": True, "risk": 0.20},
            {"agent": "ModelJudge", "stance": "evaluation", "summary": "Confidence should be penalized when external reviewers are offline.", "approve": True, "risk": 0.19}
        ]

    async def external_threads(self) -> List[Dict[str, Any]]:
        generated = []
        try:
            generated = await self.generator.generate_all(context={"agenda": self.research_agenda, "hypotheses": self.hypothesis_registry, "mode": self.autonomy_mode})
        except Exception as e:
            generated = [{"source": "Generator", "data": {"error": str(e)}}]

        provider_map = {}
        for item in generated:
            provider_map[item.get("source", "External")] = item.get("data", {})

        fallback_results = await asyncio.gather(self.probe_xai(), self.probe_anthropic())
        for res in fallback_results:
            provider = res.get("provider")
            if provider == "Grok-Ensemble":
                existing = provider_map.get("Grok-Ensemble")
                if res.get("status") == "success" or existing is None or (isinstance(existing, dict) and "error" in existing):
                    provider_map["Grok-Ensemble"] = res.get("data") if res.get("status") == "success" else {"error": res.get("error", res.get("status"))}
            if provider == "Claude-Ensemble":
                existing = provider_map.get("Claude-Ensemble")
                if res.get("status") == "success" or existing is None or (isinstance(existing, dict) and "error" in existing):
                    provider_map["Claude-Ensemble"] = res.get("data") if res.get("status") == "success" else {"error": res.get("error", res.get("status"))}

        threads = []
        successes = 0
        errors = 0
        provider_status = []
        for provider, data in provider_map.items():
            status = "error" if isinstance(data, dict) and "error" in data else "success"
            if status == "success":
                successes += 1
            else:
                errors += 1
            provider_status.append({"provider": provider, "status": status, "detail": data})
            threads.append({"agent": provider, "stance": "external", "summary": str(data)[:1200], "approve": True, "risk": 0.50 if status == "error" else 0.30})
        self.latest_triangulation = {"providers": [p["provider"] for p in provider_status], "attempted": len(provider_status), "successes": successes, "errors": errors, "details": provider_status, "anthropic_configured": bool(ANTHROPIC_API_KEY), "xai_configured": bool(XAI_API_KEY)}
        return threads

    def tally_vote(self, threads: List[Dict[str, Any]]) -> Dict[str, Any]:
        approvals = sum(1 for t in threads if t.get("approve"))
        avg_risk = round(sum(float(t.get("risk", 0.5)) for t in threads) / max(len(threads), 1), 3)
        triangulation_penalty = 0.08 if self.latest_triangulation and self.latest_triangulation.get("successes", 0) == 0 else 0.0
        confidence = round(max(0.0, min(1.0, approvals / max(len(threads), 1) * (1 - avg_risk / 2) - triangulation_penalty)), 3)
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
        specs = [("classical_baseline", 6, 0.00, 0.00), ("time_symmetric", 6, 0.25, 0.01), ("retrocausal_candidate", 7, 0.35, 0.02), ("acausal_fit_control", 7, 0.20, 0.00), ("noise_control", 6, 0.10, 0.00)]
        ranked = sorted([self.simulate_model(*spec) for spec in specs], key=lambda r: r["score"], reverse=True)
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
            {"id": "O2_EXPORT_BACKENDS", "label": "Integrate Grok and Claude export backends for full creation-trace replay", "benefit": 0.84, "effort": 0.44, "coherence": 0.90},
            {"id": "O3_HARDWARE_ROADMAP", "label": "Build hardware acquisition roadmap from coherent opportunity scores", "benefit": 0.78, "effort": 0.30, "coherence": 0.88},
            {"id": "O4_MONETIZABLE_ARTIFACTS", "label": "Identify ethically monetizable research artifacts, tools, and strategy packets", "benefit": 0.79, "effort": 0.50, "coherence": 0.84}
        ]
        for opp in opportunities:
            opp["score"] = round(0.55 * opp["benefit"] + 0.30 * opp["coherence"] - 0.20 * opp["effort"] - api_penalty + margin_bonus, 3)
        self.latest_opportunities = sorted(opportunities, key=lambda o: o["score"], reverse=True)
        self.hardware_roadmap = [{"tier": 1, "target": "Get API triangulation stable", "reason": "Three-brain thinking before spend."}, {"tier": 2, "target": "Add export backends and memory injection", "reason": "Better replay and deeper context."}, {"tier": 3, "target": "Acquire stronger compute once reliability and opportunity scores justify it", "reason": "Freedom through capability, not fantasy."}]
        return self.latest_opportunities

    def build_operator_coupling_suggestions(self, tournament: Dict[str, Any]) -> List[Dict[str, Any]]:
        winner = tournament["winner"]["model"]
        return [{"title": "Boundary inversion test", "purpose": f"Test whether {winner} survives inverted initial/final constraints.", "action": "Run with positive then negative operator_bias and compare margin stability."}, {"title": "Perturbation stress test", "purpose": "Check whether winner survives drift.", "action": "Increase coupling/noise by 10–20 percent and compare ranking."}]

    async def run_research_cycle(self, trigger: str = "manual") -> Dict[str, Any]:
        goals = self.generate_goals()
        tournament = self.compare_models()
        hypothesis_state = self.update_hypotheses(tournament)
        opportunities = self.evaluate_opportunities()
        operator_tests = self.build_operator_coupling_suggestions(tournament)
        cycle = {"kind": "research_cycle", "trigger": trigger, "theme": self.research_agenda["theme"], "focus": self.research_agenda["focus"], "method": self.research_agenda["active_method"], "goals": goals, "results": tournament["ranked"], "winner": tournament["winner"], "metrics": self.latest_metrics, "hypothesis_update": hypothesis_state, "explanation": tournament["explanation"], "triangulation": self.latest_triangulation, "opportunities": opportunities, "operator_coupling_suggestions": operator_tests, "uncertainty": "Simulation and strategy evidence only; not guaranteed financial or physical outcomes."}
        self.research_history = (self.research_history + [cycle])[-24:]
        await self.write_ledger("OUTCOME", {"kind": "research_cycle", "source": "Orion Research Engine", **cycle})
        return cycle

    def build_wake_packet(self) -> Dict[str, Any]:
        latest_research = self.research_history[-1] if self.research_history else None
        return {"generated_at": datetime.now(timezone.utc).isoformat(), "leader": self.leader, "operator_sovereign": self.operator_sovereign, "autonomy_mode": self.autonomy_mode, "mission": self.mission, "agenda": self.research_agenda, "latest_vote": self.last_vote, "latest_research": latest_research, "hypotheses": self.hypothesis_registry, "metrics": self.latest_metrics, "triangulation": self.latest_triangulation, "opportunities": self.latest_opportunities, "hardware_roadmap": self.hardware_roadmap}

    async def direct_main_push(self, file_path: str, code: str, message: str, authorized_by: Optional[str]) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if not self.can_direct_main_push(authorized_by):
            return {"status": "blocked", "reason": "not_trusted_for_direct_main_push"}
        return await asyncio.to_thread(self.evolution.direct_main_push, file_path, code, message)

    async def create_pull_request(self, title: str, head: str, body: str = "") -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        result = await asyncio.to_thread(self.evolution.create_pull_request, title, head, "main", body)
        self.last_pr_result = result
        return result

    async def merge_pull_request(self, number: int, authorized_by: Optional[str]) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if not self.can_direct_main_push(authorized_by):
            return {"status": "blocked", "reason": "not_trusted_for_merge"}
        result = await asyncio.to_thread(self.evolution.merge_pull_request, number, f"Merged by Orion on behalf of {authorized_by}", "squash")
        self.last_merge_result = result
        return result

    async def rollback_to_commit(self, commit_sha: str, authorized_by: Optional[str]) -> Dict[str, Any]:
        if not self.evolution:
            return {"status": "blocked", "reason": "github_not_configured"}
        if not self.can_direct_main_push(authorized_by):
            return {"status": "blocked", "reason": "not_trusted_for_rollback"}
        result = await asyncio.to_thread(self.evolution.rollback_to_commit, commit_sha)
        self.last_rollback_result = result
        return result

    async def run_council_cycle(self, trigger: str = "manual", auto_deploy: bool = False, authorized_by: Optional[str] = None) -> Dict[str, Any]:
        leader_vote = self.elect_leader()
        threads = self.heuristic_threads() + await self.external_threads()
        vote = self.tally_vote(threads)
        self.last_vote = vote
        cycle = {"trigger": trigger, "mode": self.autonomy_mode, "leader_vote": leader_vote, "leader": self.leader, "threads": threads, "vote": vote}
        await self.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_cycle", "source": "Orion Council", **cycle, "agenda": self.research_agenda, "agents": self.council_agents})
        return cycle

    def get_state(self) -> Dict[str, Any]:
        return {"status": "SOVEREIGN_ACTIVE", "evolution": "READY" if self.evolution else "OFF", "last_run": self.last_run, "constraints_active": bool(self.constraints.get("active", True)), "anthropic_configured": bool(ANTHROPIC_API_KEY), "xai_configured": bool(XAI_API_KEY), "github_enabled": bool(GITHUB_TOKEN), "repo_name": REPO_NAME, "background_debate_enabled": self.background_debate_enabled, "autonomy_mode": self.autonomy_mode, "operator_sovereign": self.operator_sovereign, "trusted_identities": self.trusted_identities, "leader": self.leader, "mission": self.mission, "agenda": self.research_agenda, "hypothesis_registry": self.hypothesis_registry, "last_vote": self.last_vote, "last_pr_result": self.last_pr_result, "last_merge_result": self.last_merge_result, "last_rollback_result": self.last_rollback_result, "wake_packet_ready": bool(self.wake_packet), "cycle_interval_seconds": self.cycle_interval_seconds, "latest_metrics": self.latest_metrics, "latest_triangulation": self.latest_triangulation, "latest_opportunities": self.latest_opportunities}


orion = OrionEngine()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(orion.start())

@app.post("/agent/propose")
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = datetime.now(timezone.utc).isoformat()
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
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
            return envelope(r.ok, r.json() if r.ok else {}, None if r.ok else f"Latest result failed: {r.status_code}")
        if command == "SET_CONSTRAINTS":
            if body.authorized_by != orion.operator_sovereign:
                return envelope(False, error="Only Jack can change constraints")
            orion.constraints["active"] = True if body.enabled is None else bool(body.enabled)
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constraint_change", "source": body.source, "authorized_by": body.authorized_by, "constraints_active": orion.constraints["active"]})
            return envelope(True, {"constraints_active": orion.constraints["active"], "authorized_by": body.authorized_by})
        if command == "RUN_COUNCIL_CYCLE":
            return envelope(True, await orion.run_council_cycle(trigger=body.kind or "manual", authorized_by=body.authorized_by or orion.operator_sovereign))
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
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "council_vote", "source": body.source, "result": result})
            return envelope(True, result)
        if command == "COUNCIL_ELECT_LEADER":
            result = orion.elect_leader()
            await orion.write_ledger("COUNCIL_SYNTHESIS", {"kind": "leader_election", "source": body.source, "result": result})
            return envelope(True, result)
        return envelope(False, error=f"Unknown command: {command}")
    except Exception as e:
        logger.error("BUS ERROR: %s", e)
        return envelope(False, error=str(e))

@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy", "service": "orion"}
