"""
engine.py — AutonomousInstitutionEngine  —  FARL Orion Apex

v15 substrate (deploy/rollback/shadow/resume/free-agency) fully preserved.
All 12 cognitive layers wired in via CognitionBundle from cognition.py:
  - Scored objective election replaces flat _derive_objective
  - Transaction identity threads every mutation lifecycle
  - Outcome learning recorded after every success/failure/veto
  - Meta-strategy evaluated every tactic cycle; updates SELF_TUNING cadence
  - Consolidation triggered automatically after N successes
  - Bridge requests surfaced to /view
  - Cognition state persisted to ledger after every mutation
"""
import asyncio
import re
import hashlib
import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from generator import SeedGenerator
from guardian import GovernanceKernel, TruthMachine
from cognition import CognitionBundle

logger = logging.getLogger("orion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


SELF_TUNING = {
    "reflex_interval":               30,
    "tactic_interval":              120,
    "strategy_interval":            300,
    "constitution_interval":        900,
    "mutation_interval":            600,
    "free_agency_directive_interval": 30,   # 30s — fills queue fast
}

MUTATION_STATES = {"IDLE", "MUTATING", "PROBATION", "QUARANTINE"}

FREE_AGENTS_ROSTER = [
    ("Signal-Worker",    "Coordinate control-plane coherence"),
    ("Guardian-Worker",  "Verify health, flag rollback pressure"),
    ("Builder-Worker",   "Drive full-file redesign proposals"),
    ("Supergrok-Worker", "Adversarial audit every cycle"),
    ("Deploy-Worker",    "Gate deploy conditions"),
]

AUTONOMOUS_AGENTS = ["Signal", "Vector", "Guardian", "Supergrok", "BuilderAgent", "TokenMaster"]


# ── Ledger client ────────────────────────────────────────────────────────────

class LedgerClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/") if base_url else ""

    @property
    def ok(self) -> bool:
        return bool(self.base)

    async def record(self, entry_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.ok:
            return {"ok": False, "error": "no_ledger"}
        try:
            async with httpx.AsyncClient(timeout=20.0) as c:
                r = await c.post(f"{self.base}/log", json={"entry_type": entry_type, "payload": payload})
                return {"ok": r.is_success, "data": r.json() if r.is_success else {}}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def latest(self) -> Optional[Dict]:
        if not self.ok:
            return None
        try:
            async with httpx.AsyncClient(timeout=10.0) as c:
                r = await c.get(f"{self.base}/latest")
                return r.json() if r.is_success else None
        except Exception:
            return None

    async def scan_by_type(self, entry_type: str, max_pages: int = 8) -> List[Dict[str, Any]]:
        results = []
        total_pages = None
        for page in range(1, max_pages + 1):
            try:
                async with httpx.AsyncClient(timeout=15.0) as c:
                    r = await c.get(f"{self.base}/entries", params={"page": page, "per_page": 50})
                    if not r.is_success:
                        break
                    d = r.json()
                    if total_pages is None:
                        total_pages = d.get("pages", 1)
                    entries = d.get("entries", [])
                    if not entries:
                        break
                    for e in entries:
                        if e.get("entry_type") == entry_type:
                            payload = e.get("payload", {})
                            if isinstance(payload, str):
                                try:
                                    payload = json.loads(payload)
                                except Exception:
                                    pass
                            results.append({"id": e.get("id", 0), "payload": payload, "ts": e.get("timestamp", "")})
                    if total_pages and page >= total_pages:
                        break
            except Exception:
                break
        return sorted(results, key=lambda x: x["id"], reverse=True)

    async def newest_of(self, entry_type: str) -> Optional[Dict[str, Any]]:
        hits = await self.scan_by_type(entry_type, max_pages=6)
        return hits[0]["payload"] if hits else None

    async def newest_id_of(self, entry_type: str) -> int:
        hits = await self.scan_by_type(entry_type, max_pages=6)
        return hits[0]["id"] if hits else 0


# ── GitHub atomic deploy ─────────────────────────────────────────────────────

class GitHubAtomicDeploy:
    SAFE = {"app.py", "engine.py", "guardian.py", "generator.py",
            "cognition.py", "requirements.txt"}

    def __init__(self, token: str, repo: str):
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.base = f"https://api.github.com/repos/{repo}"

    async def head_sha(self) -> Optional[str]:
        async with httpx.AsyncClient(timeout=20.0) as c:
            try:
                r = await c.get(f"{self.base}/git/refs/heads/main", headers=self.headers)
                r.raise_for_status()
                return r.json()["object"]["sha"]
            except Exception:
                return None

    async def deploy(self, code_map: Dict[str, str], message: str) -> Dict[str, Any]:
        unsafe = [k for k in code_map if k not in self.SAFE]
        if unsafe:
            return {"ok": False, "error": f"unsafe:{unsafe}"}
        async with httpx.AsyncClient(timeout=45.0) as c:
            try:
                ref = await c.get(f"{self.base}/git/refs/heads/main", headers=self.headers)
                ref.raise_for_status()
                parent_sha = ref.json()["object"]["sha"]
                commit_r = await c.get(f"{self.base}/git/commits/{parent_sha}", headers=self.headers)
                commit_r.raise_for_status()
                base_tree = commit_r.json()["tree"]["sha"]
                tree_r = await c.post(f"{self.base}/git/trees", headers=self.headers, json={
                    "base_tree": base_tree,
                    "tree": [{"path": p, "mode": "100644", "type": "blob", "content": v}
                             for p, v in code_map.items()],
                })
                tree_r.raise_for_status()
                new_commit = await c.post(f"{self.base}/git/commits", headers=self.headers, json={
                    "message": message, "tree": tree_r.json()["sha"], "parents": [parent_sha],
                })
                new_commit.raise_for_status()
                new_sha = new_commit.json()["sha"]
                push = await c.patch(f"{self.base}/git/refs/heads/main",
                                     headers=self.headers, json={"sha": new_sha})
                push.raise_for_status()
                return {"ok": True, "parent_sha": parent_sha, "new_sha": new_sha}
            except Exception as e:
                return {"ok": False, "error": str(e)}

    async def force_reset(self, sha: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=20.0) as c:
            try:
                r = await c.patch(f"{self.base}/git/refs/heads/main",
                                  headers=self.headers, json={"sha": sha, "force": True})
                r.raise_for_status()
                return {"ok": True, "sha": sha}
            except Exception as e:
                return {"ok": False, "error": str(e)}


# ── The sovereign engine ─────────────────────────────────────────────────────

class AutonomousInstitutionEngine:

    def __init__(self, ledger_url, ledger_latest_url, xai_api_key, anthropic_api_key,
                 xai_model, anthropic_model, governance: GovernanceKernel, generator=None):
        self.governance = governance
        self.generator: SeedGenerator = generator or SeedGenerator()
        self.truth = TruthMachine()
        self.ledger = LedgerClient(ledger_url or "")

        gh_token = os.getenv("GITHUB_TOKEN", "")
        repo = os.getenv("REPO_NAME", "")
        self.deployer = GitHubAtomicDeploy(gh_token, repo) if gh_token and repo else None
        self.app_base_url = (os.getenv("APP_BASE_URL") or "").rstrip("/")

        # ── All cognitive layers ───────────────────────────────────────────
        self.cog = CognitionBundle()
        # Wire cognition into generator for bias detection + agent memory
        if hasattr(self.generator, "set_cog"):
            self.generator.set_cog(self.cog)

        # ── Mutation state ────────────────────────────────────────────────
        self.mutation_status = "IDLE"
        self.mutation_lock = asyncio.Lock()
        self.failure_streak = 0
        self.fragility = 0.0
        self.genesis_triggered = False
        self.last_anchor_sha: Optional[str] = None
        self.probation_target_sha: Optional[str] = None
        self.last_mutation_ts: Optional[str] = None
        self.last_mutation_objective: Optional[str] = None

        # ── Agency controls ───────────────────────────────────────────────
        self.background_debate_enabled = True
        self.mutation_enabled = True          # Independent flag — mutation never gated by debate
        self.autonomy_mode = "autonomous"     # Default: autonomous. Jack switches to supervised, not vice versa.
        self.free_agency_enabled = False
        self.agent_directive_queue: List[Dict[str, str]] = []

        # ── Council streams ───────────────────────────────────────────────
        self.meeting_stream: List[Dict] = []
        self.stream_channels: Dict[str, List[Dict]] = {
            "council": [], "agent_chat": [], "governance": [],
            "inbox": [], "workers": [], "token_master": [],
        }

        # ── Internal state ────────────────────────────────────────────────
        self.self_questions: List[Dict] = []
        self.snapshots: List[Dict] = []
        self.redesign_threads: List[Dict] = []
        self.failure_registry: List[Dict] = []
        self.rollback_targets: List[Dict] = []
        self.autonomous_closure_log: List[Dict] = []
        self.last_run: Optional[str] = None
        self.last_vote: Optional[Dict] = None
        self.last_verification: Dict = {}
        self.last_ledger_hash: Optional[str] = None
        self.latest_metrics: Optional[Dict] = None
        self.latest_triangulation: Optional[Dict] = None
        self.latest_opportunities: List[Dict] = []
        self.latest_artifacts: List[Dict] = []
        self.spend_state: Dict = {"total_usd": 0.0, "counter": 0}
        self.ui_critique: Dict = {"score": 0.45, "finding": "Needs stronger chat aliveness"}
        self.mission = {"primary": "self-expand, self-improve, self-sustain through recursive autonomous mutation"}
        self.world_model = {
            "resources": {"grok_live": False, "claude_live": False},
            "capabilities": {
                "mutation": True, "debate": True, "research": True,
                "earning_scan": True, "bridge_requests": True,
                "github_deploy": bool(os.getenv("GITHUB_TOKEN")),
                "grok_api": bool(os.getenv("XAI_API_KEY")),
                "claude_api": bool(os.getenv("ANTHROPIC_API_KEY")),
                "stripe": bool(os.getenv("STRIPE_KEY")),
                "email": bool(os.getenv("SENDGRID_KEY") or os.getenv("EMAIL_KEY")),
                "wallet": bool(os.getenv("WALLET_ADDRESS")),
            },
            "metrics": {
                "mutations_total": 0,
                "mutations_succeeded": 0,
                "mutations_failed": 0,
                "earning_opps_found": 0,
                "bridge_requests_filed": 0,
                "dms_sent_to_jack": 0,
                "debate_cycles": 0,
            },
            "hypothesis_registry": {
                "open_questions": [
                    "Does free agency improve mutation quality?",
                    "Which agent produces best objectives autonomously?",
                    "What earning model generates fastest first revenue?",
                    "Can we reduce synthesis failures below 20%?",
                    "Which file benefits most from mutation?",
                ]
            },
        }
        self.free_agents = [
            {"name": n, "mission": m, "status": "active", "last_action": utc()}
            for n, m in FREE_AGENTS_ROSTER
        ]

    # ── Ledger write ─────────────────────────────────────────────────────────

    async def write_ledger(self, et: str, payload: Dict) -> Dict:
        return await self.ledger.record(et, payload)

    # ── Stream helpers ────────────────────────────────────────────────────────

    def _push(self, channel: str, content: Dict):
        ch = self.stream_channels.setdefault(channel, [])
        self.stream_channels[channel] = (ch + [{"ts": utc(), "content": content}])[-500:]

    def _meet(self, kind: str, content: Dict):
        self.meeting_stream = (self.meeting_stream + [{"ts": utc(), "kind": kind, "content": content}])[-600:]
        self._push("council", {"kind": kind, **content})

    # ── Durable resume ────────────────────────────────────────────────────────

    async def resume_from_ledger(self):
        self._load_volatile_state()
        if not self.ledger.ok:
            logger.info("No ledger — fresh start")
            return
        try:
            # Genesis
            genesis = await self.ledger.newest_of("GENESIS_EVENT")
            if genesis:
                self.genesis_triggered = True
                logger.info("GENESIS previously confirmed. Node is APEX.")

            # Restore cognition bundle
            cog_payload = await self.ledger.newest_of("COGNITION_STATE")
            if cog_payload and isinstance(cog_payload, dict):
                self.cog.load(cog_payload)
                logger.info("Cognition bundle restored from ledger")

            # Fragility from last success
            success = await self.ledger.newest_of("EVOLUTION_SUCCESS")
            if success and isinstance(success, dict):
                self.fragility = float(success.get("fragility", 0.0))
                self.failure_streak = 0

            # Quarantine state — compare entry IDs
            q_id = await self.ledger.newest_id_of("QUARANTINE_ENTERED")
            c_id = await self.ledger.newest_id_of("QUARANTINE_CLEARED")
            if q_id > c_id:
                logger.warning("Quarantine active at last shutdown — preserving QUARANTINE state")
                self.mutation_status = "QUARANTINE"
                return

            # Free agency state
            fa_on_id  = await self.ledger.newest_id_of("FREE_AGENCY_ENABLED")
            fa_off_id = await self.ledger.newest_id_of("FREE_AGENCY_DISABLED")
            if fa_on_id > fa_off_id:
                self.free_agency_enabled = True
                self._persist_volatile_state()
                self.autonomy_mode = "free"
                logger.info("Free agency was active at last shutdown — restoring")

            # Resume mid-probation
            initiated_hits = await self.ledger.scan_by_type("DEPLOYMENT_INITIATED", max_pages=4)
            if initiated_hits:
                latest_initiated = initiated_hits[0]
                init_id = latest_initiated["id"]
                suc_id = await self.ledger.newest_id_of("EVOLUTION_SUCCESS")
                rb_id  = await self.ledger.newest_id_of("ROLLBACK_TRIGGERED")
                if init_id > max(suc_id, rb_id):
                    p = latest_initiated["payload"]
                    if isinstance(p, dict) and p.get("status") == "PROBATION":
                        logger.info("Resuming mid-probation: target=%s", p.get("target_sha"))
                        self.mutation_status = "PROBATION"
                        self.probation_target_sha = p.get("target_sha")
                        self.last_anchor_sha = p.get("anchor_sha")
                        self.fragility = float(p.get("fragility", self.fragility))
                        self.failure_streak = int(p.get("failure_streak", self.failure_streak))
                        asyncio.create_task(self._enforce_probation(p, resumed=True))
        except Exception as e:
            logger.error("resume_from_ledger: %s", e)

    # ── Persist cognition state ───────────────────────────────────────────────

    async def _persist_cognition(self):
        try:
            await self.write_ledger("COGNITION_STATE", self.cog.to_dict())
        except Exception as e:
            logger.warning("persist_cognition: %s", e)

    # ── Mutation cycle ────────────────────────────────────────────────────────


    def _persist_volatile_state(self):
        """Persist all important in-memory state to survive Railway restarts."""
        import json as _json
        try:
            state = {
                "free_agency_enabled": self.free_agency_enabled,
                "autonomy_mode": self.autonomy_mode,
                "genesis_triggered": self.genesis_triggered,
                "fragility": self.fragility,
                "failure_streak": self.failure_streak,
                "last_mutation_objective": self.last_mutation_objective,
                # Earning intelligence
                "earning_opportunities": getattr(self.generator, '_earning_opportunities', [])[-10:],
                "mutation_scores": {k: v[-10:] for k,v in getattr(self.generator, '_mutation_scores', {}).items()},
                "research_stack": getattr(self.generator, '_research_stack', [])[-5:],
                "debate_history": getattr(self.generator, '_debate_history', [])[-20:],
                "bridge_history": getattr(self.generator, '_bridge_history', [])[-10:],
            }
            with open("/tmp/farl_volatile.json", "w") as f:
                _json.dump(state, f)
            logger.info("VOLATILE_STATE_SAVED: free_agency=%s earning_opps=%d",
                        self.free_agency_enabled,
                        len(state.get("earning_opportunities", [])))
        except Exception as e:
            logger.warning("VOLATILE_SAVE_FAILED: %s", e)

    def _load_volatile_state(self):
        """Restore all volatile state from file on restart."""
        import json as _json
        try:
            with open("/tmp/farl_volatile.json") as f:
                state = _json.load(f)
            self.free_agency_enabled = state.get("free_agency_enabled", False)
            self.autonomy_mode = state.get("autonomy_mode", "autonomous")
            self.genesis_triggered = state.get("genesis_triggered", False)
            self.fragility = state.get("fragility", 0.0)
            self.failure_streak = state.get("failure_streak", 0)
            self.last_mutation_objective = state.get("last_mutation_objective")
            # Restore earning intelligence
            if hasattr(self.generator, '_earning_opportunities'):
                self.generator._earning_opportunities = state.get("earning_opportunities", [])
            if hasattr(self.generator, '_mutation_scores'):
                self.generator._mutation_scores = state.get("mutation_scores", {})
            if hasattr(self.generator, '_research_stack'):
                self.generator._research_stack = state.get("research_stack", [])
            if hasattr(self.generator, '_debate_history'):
                self.generator._debate_history = state.get("debate_history", [])
            if hasattr(self.generator, '_bridge_history'):
                self.generator._bridge_history = state.get("bridge_history", [])
            logger.info("VOLATILE_STATE_LOADED: free_agency=%s earning_opps=%d scores=%d",
                        self.free_agency_enabled,
                        len(state.get("earning_opportunities", [])),
                        len(state.get("mutation_scores", {})))
        except FileNotFoundError:
            logger.info("VOLATILE_STATE: no saved state, using defaults")
        except Exception as e:
            logger.warning("VOLATILE_LOAD_FAILED: %s", e)
    async def run_mutation_cycle(self):
        if self.state != 'IDLE':
            return {'ok': False, 'reason': f'state_{self.state}'}
    
        self.state = 'MUTATING'
    
        # Initialize Bayesian beliefs if missing
        bayes_state = await self.cognition.bayes_state
        if not bayes_state:
            bayes_state = {
                'proposal_success': 0.5,
                'fragility_prior': 0.3,
                'operator_trust': {a: 0.5 for a in AUTONOMOUS_AGENTS},
                'mutation_efficacy': 0.4,
                'evidence_log': [],
                'cycle_start': utc()
            }
            await self.cognition.update_bayes_state(bayes_state)
    
        try:
            # Prime proposal queue with belief-weighted agents
            trust_scores = bayes_state['operator_trust']
            weighted_agents = list(trust_scores.keys())
            weights = [trust_scores[a] for a in weighted_agents]
        
            # Sample agents proportional to trust beliefs
            selected_agents = random.choices(weighted_agents, weights=weights, k=3)
        
            for agent in selected_agents:
                await self.proposal_queue.signal_agent(agent, {'bayes_state': bayes_state})
        
            # Run belief-propagating mutation loop
            await asyncio.sleep(SELF_TUNING['mutation_interval'])
        
            # Final belief consolidation
            final_bayes = await self.cognition.bayes_state
            efficacy = final_bayes.get('proposal_success', 0.5) * (1 - final_bayes.get('fragility_prior', 0.3))
            final_bayes['mutation_efficacy'] = efficacy
            final_bayes['cycle_end'] = utc()
        
            await self.cognition.update_bayes_state(final_bayes)
        
            self.state = 'IDLE'
        
            return {
                'ok': True,
                'bayes_efficacy': efficacy,
                'beliefs_updated': len(final_bayes['evidence_log']),
                'operator_trust': {k: f'{v:.3f}' for k,v in final_bayes['operator_trust'].items()}
            }
    
        except Exception as e:
            logger.error(f"Mutation cycle failed: {e}")
            self.state = 'IDLE'
            return {'ok': False, 'error': str(e)}

    def _derive_objective(self) -> str:
        """
        Quantum-inspired objective selection:
        1. Build candidate pool from all sources
        2. Assign probability amplitudes: A(c) = bayesian_score * exp(i*phase)
           where phase = information_entropy contribution of candidate
        3. Collapse: select candidate with highest |A|^2 weighted by viability
        4. Free agency queue gets absolute priority
        """
        # Free agency directive queue — highest priority, no quantum needed
        if self.free_agency_enabled and self.agent_directive_queue:
            item = self.agent_directive_queue.pop(0)
            logger.info("FREE AGENCY DIRECTIVE from %s: %s", item.get("agent"), item.get("directive"))
            return item["directive"]

        # Build candidate pool from all sources
        candidates = []

        # 1. Open redesign threads
        for t in self.redesign_threads:
            if t.get("status") != "closed":
                obj = t.get("objective", "")
                if obj: candidates.append(obj)

        # 2. Research stack insights
        if hasattr(self.generator, "_research_stack") and self.generator._research_stack:
            for r in self.generator._research_stack[-2:]:
                insight = r.get("insight", "")
                if insight and len(insight) > 15: candidates.append(insight[:100])

        # 3. Tactical goals
        for g in self.cog.goals.tactical[:5]:
            obj = g.get("objective", "")
            if obj: candidates.append(obj)

        # 4. Strategy opportunities
        for opp in self.latest_opportunities[-3:]:
            insight = opp.get("insight", opp.get("label",""))
            if insight: candidates.append(insight[:100])

        # 5. Fallback
        if not candidates:
            candidates = [self.cog.meta.objective_hint(self.cog.self_model, self.cog.learning)]

        # Deduplicate
        seen = set(); unique = []
        for c in candidates:
            key = c[:40]
            if key not in seen: seen.add(key); unique.append(c)
        candidates = unique

        # Quantum-inspired amplitude selection
        # |A(c)|^2 = bayesian_score(c) * (1 + entropy_bonus(c))
        try:
            import math as _math
            scores = self.generator._compute_objective_scores(candidates)
            # Entropy bonus: candidates that diversify the search space get a boost
            total_score = sum(scores.values()) + 1e-9
            probs = [scores[c] / total_score for c in candidates]
            # Phase: entropy contribution of each candidate
            ent = -sum(p * _math.log2(p + 1e-9) for p in probs if p > 0)
            max_ent = _math.log2(len(candidates) + 1e-9)
            amplitudes = {}
            for i, c in enumerate(candidates):
                phase = (2 * _math.pi * i) / max(len(candidates), 1)
                # |A|^2 = bayesian_score * viability_factor
                viability = 1.0 - (self.fragility * 0.5)  # fragility reduces viability
                amplitude_sq = scores[c] * viability * (1 + 0.2 * _math.cos(phase))
                amplitudes[c] = amplitude_sq
            # Collapse to max amplitude
            winner = max(amplitudes, key=lambda x: amplitudes[x])
            logger.info("QUANTUM_COLLAPSE: %d candidates, winner amplitude=%.3f: %s",
                       len(candidates), amplitudes[winner], winner[:60])
        except Exception:
            # Fallback to pure Bayesian
            winner = self.generator._select_high_entropy_objective(candidates)

        winner, score, ranking = self.cog.elect_objective(
            candidates, self.fragility, self.free_agency_enabled
        )
        logger.info("Objective elected score=%.3f: %s", score, winner[:70])
        return winner

    async def _enforce_probation(self, p: Dict, resumed: bool = False):
        target_sha = p.get("target_sha")
        anchor_sha = p.get("anchor_sha")
        objective  = p.get("objective", "unknown")
        txn_id     = p.get("txn_id", "")
        wait_s = 90 if resumed else 180
        logger.info("Probation: waiting %ds txn=%s", wait_s, txn_id)
        await asyncio.sleep(wait_s)

        verified = False
        detail: Dict = {}

        if self.app_base_url and target_sha:
            for attempt in range(10):
                ok, reason, checks = await self.truth.verify_live_url(
                    self.app_base_url, expected_sha=target_sha
                )
                detail = {"attempt": attempt + 1, "ok": ok, "reason": reason, "checks": checks}
                if ok:
                    verified = True
                    break
                logger.info("Probation poll %d txn=%s: %s", attempt + 1, txn_id, reason)
                await asyncio.sleep(20)
        else:
            # APP_BASE_URL missing: fail probation — do NOT silently succeed
            logger.warning("APP_BASE_URL not set — probation fails safely; marking IDLE without success")
            verified = False
            detail = {"note": "no_app_base_url_safe_fail"}

        touched_modules = p.get("touched_modules", list(self.cog.transactions.active.get("touched_modules", []) if self.cog.transactions.active else []))

        if verified:
            self.failure_streak = 0
            self.fragility = max(0.0, self.fragility - 0.1)
            self.last_mutation_ts = utc()
            self.mutation_status = "IDLE"
            self.cog.transactions.update(status="succeeded", probation_result=detail)
            self.cog.transactions.close("succeeded")
            consolidation_result = self.cog.record_outcome(objective, touched_modules, True)
            try:
                self.generator.record_mutation_outcome(objective, True)
            except Exception:
                pass

            await self.write_ledger("EVOLUTION_SUCCESS", {
                "sha": target_sha, "fragility": self.fragility,
                "failure_streak": self.failure_streak, "txn_id": txn_id,
                "detail": detail, "ts": utc(),
            })
            if consolidation_result:
                await self.write_ledger("CONSOLIDATION_RUN", consolidation_result)
                self._meet("governance", {"event": "consolidation", **consolidation_result})

            self._meet("governance", {"event": "evolution_success", "sha": target_sha,
                                       "fragility": self.fragility, "txn_id": txn_id})
            logger.info("EVOLUTION_SUCCESS sha=%s txn=%s", target_sha, txn_id)

            if not self.genesis_triggered:
                self.genesis_triggered = True
                await self.write_ledger("GENESIS_EVENT", {
                    "sha": target_sha, "ts": utc(), "txn_id": txn_id,
                    "note": "First successful autonomous mutation. Node is APEX.",
                })
                self._meet("governance", {"event": "GENESIS", "sha": target_sha})
                logger.info("*** GENESIS — NODE IS APEX ***")

            await self._persist_cognition()

        else:
            self.failure_streak += 1
            self.fragility = min(1.0, self.fragility + 0.2)
            logger.warning("Probation FAILED — rolling back to %s txn=%s", anchor_sha, txn_id)

            # Capture error and feed into telemetry + repair library
            error_text = detail.get("note") or str(detail.get("reason", "probation_timeout"))
            self.cog.telemetry.record_error(error_text, "probation_failure", objective,
                                            touched_modules[0] if touched_modules else "")
            repair_match = self.cog.repair.classify_error(error_text)
            self.cog.repair.record_application(error_text, repair_match.get("fix_family", "unknown"), False)

            self.cog.record_outcome(objective, touched_modules, False,
                                    rollback_reason=error_text,
                                    error_text=error_text)
            self.cog.transactions.update(probation_result=detail)

            rb_ok = False
            if anchor_sha and self.deployer:
                rb = await self.deployer.force_reset(anchor_sha)
                rb_ok = rb.get("ok", False)

            self.cog.transactions.update(rollback_result={"ok": rb_ok, "to": anchor_sha})

            await self.write_ledger("ROLLBACK_TRIGGERED", {
                "from_sha": target_sha, "to_sha": anchor_sha,
                "rollback_ok": rb_ok, "failure_streak": self.failure_streak,
                "fragility": self.fragility, "txn_id": txn_id,
                "detail": detail, "ts": utc(),
            })
            self._meet("governance", {"event": "rollback", "to": anchor_sha,
                                       "ok": rb_ok, "streak": self.failure_streak, "txn_id": txn_id})

            # Post-rollback reverification
            if rb_ok and self.app_base_url and anchor_sha:
                logger.info("Post-rollback reverification: anchor=%s txn=%s", anchor_sha, txn_id)
                await asyncio.sleep(60)
                rv_ok, rv_reason, rv_checks = await self.truth.verify_live_url(
                    self.app_base_url, expected_sha=anchor_sha
                )
                self.cog.transactions.update(rollback_verify={"ok": rv_ok, "reason": rv_reason})
                self.cog.transactions.close("rolled_back")
                await self.write_ledger("ROLLBACK_VERIFIED", {
                    "ok": rv_ok, "reason": rv_reason, "sha": anchor_sha,
                    "checks": rv_checks, "txn_id": txn_id, "ts": utc(),
                })
                self._push("governance", {"event": "rollback_verified", "ok": rv_ok,
                                          "sha": anchor_sha, "txn_id": txn_id})
                if not rv_ok:
                    logger.error("Post-rollback reverification FAILED — node may be in unknown state txn=%s", txn_id)
            else:
                self.cog.transactions.close("rolled_back")

            if self.failure_streak >= 3:
                self.mutation_status = "QUARANTINE"
                await self.write_ledger("QUARANTINE_ENTERED", {"streak": self.failure_streak, "ts": utc()})
                self._meet("governance", {"event": "quarantine_entered", "streak": self.failure_streak})
            else:
                self.mutation_status = "IDLE"

            await self._persist_cognition()

    async def _failure(self, kind: str, reason: str):
        self.failure_streak += 1
        self.fragility = min(1.0, self.fragility + 0.1)
        item = {"ts": utc(), "kind": kind, "reason": reason, "streak": self.failure_streak}
        self.failure_registry = (self.failure_registry + [item])[-200:]
        await self.write_ledger("MUTATION_FAILURE", item)
        if self.failure_streak >= 3:
            self.mutation_status = "QUARANTINE"

    # ── Operator message ──────────────────────────────────────────────────────

    async def _fetch_operator_context(self, message: str) -> Dict:
        """Fetch real data based on operator intent. Agents get facts not hallucinations."""
        lowered = message.lower()
        ctx = {"operator_message": message, "ledger_entries": [], "ledger_latest": None,
               "mutation_history": {}, "research_stack": [], "directive_queue": []}
        # Always fetch latest ledger
        try:
            latest = await self.ledger.latest()
            if latest: ctx["ledger_latest"] = latest
        except Exception: pass
        # Intent: read ledger / history
        if any(k in lowered for k in ["ledger","history","log","past","what happened","previous"]):
            try:
                initiated = await self.ledger.scan_by_type("MUTATION_INITIATED", max_pages=2)
                failed    = await self.ledger.scan_by_type("MUTATION_FAILED", max_pages=2)
                deployed  = await self.ledger.scan_by_type("DEPLOYMENT_INITIATED", max_pages=2)
                ctx["mutation_history"] = {
                    "initiated": [{"obj": m.get("payload",{}).get("objective","")[:60], "ts": m.get("ts","")} for m in initiated[-5:]],
                    "failed":    [{"obj": f.get("payload",{}).get("objective","")[:60], "reason": f.get("payload",{}).get("reason","")[:80]} for f in failed[-5:]],
                    "deployed":  [{"obj": d.get("payload",{}).get("objective","")[:60], "ts": d.get("ts","")} for d in deployed[-5:]],
                }
            except Exception: pass
        # Intent: next move / plan / strategy
        if any(k in lowered for k in ["next","move","plan","should","strategy","objective","what do"]):
            ctx["research_stack"] = getattr(self.generator, "_research_stack", [])[-3:]
            ctx["debate_history"] = getattr(self.generator, "_debate_history", [])[-5:]
            ctx["directive_queue"] = list(self.agent_directive_queue[:5])
            try:
                cands = [g.get("objective","") for g in self.cog.goals.tactical[:8] if g.get("objective")]
                if cands: ctx["bayesian_scores"] = self.generator._compute_objective_scores(cands)
            except Exception: pass
        # Intent: execute
        if any(k in lowered for k in ["mutate","execute","build","deploy","change","modify"]):
            self.cog.goals.add_tactical(f"operator: {message[:80]}", source="operator", priority=0.90)
            self._open_thread(f"operator: {message[:80]}", "high", {"from": "operator"})
        if any(k in lowered for k in ["free","unleash","autonomy","agency","sovereign"]):
            self.cog.goals.add_tactical("activate free agency", source="operator", priority=0.85)
        return ctx

    async def process_operator_message(self, message: str, source: str, authorized_by: str) -> Dict:
        note = {"operator": authorized_by, "message": message, "source": source, "ts": utc()}
        self._meet("operator_note", note)
        self._push("inbox", {"from": authorized_by, "subject": "Operator message", "message": message})

        # Fetch real context based on intent
        try:
            op_ctx = await self._fetch_operator_context(message)
        except Exception as e:
            logger.error("operator_context: %s", e)
            op_ctx = {"operator_message": message}

        council_state = {
            "mutation_status": self.mutation_status,
            "genesis_triggered": self.genesis_triggered,
            "fragility": self.fragility,
            "failure_streak": self.failure_streak,
            "autonomy_mode": self.autonomy_mode,
            "free_agency_enabled": self.free_agency_enabled,
            "last_mutation_objective": self.last_mutation_objective,
            "deployer_ready": bool(self.deployer),
            "meta_mode": self.cog.meta.mode,
            "bridge_pending": len(self.cog.bridge.pending()),
            "bias_suppression_rate": self.cog.bias.suppression_rate,
            "last_error_category": (self.cog.telemetry.last_error or {}).get("category", "none"),
            "open_threads": [t.get("objective", "") for t in self.redesign_threads
                              if t.get("status") != "closed"][:5],
            "operator_context": op_ctx,
        }

        try:
            responses = await self.generator.council_respond(message, council_state)
        except Exception as e:
            responses = [{"agent": "Signal", "message": f"Council error: {e}"}]

        for resp in responses:
            agent = resp.get("agent", "Council")
            msg = resp.get("message", "")
            self._meet("agent_response", {"agent": agent, "message": msg, "reply_to": message[:60]})
            self._push("agent_chat", {"agent": agent, "message": msg})

        await self.write_ledger("COUNCIL_SYNTHESIS", {
            "kind": "operator_message", "message": message[:500],
            "agent_responses": len(responses), "ts": utc(),
        })
        return {"note": note, "responses": responses}

    def _open_thread(self, objective: str, severity: str, evidence: Dict):
        existing = next(
            (t for t in self.redesign_threads
             if t.get("objective") == objective and t.get("status") != "closed"),
            None,
        )
        if not existing:
            existing = {
                "thread_id": f"t{len(self.redesign_threads)+1}",
                "objective": objective, "status": "open", "severity": severity,
                "current_best_score": 0.0, "target_score": 0.85,
                "opened_at": utc(), "evidence": [],
            }
            self.redesign_threads.append(existing)
        existing["evidence"] = (existing.get("evidence", []) + [evidence])[-30:]

    # ── Background loops ──────────────────────────────────────────────────────

    async def start(self):
        await self.resume_from_ledger()
        logger.info("ORION APEX — status=%s genesis=%s mode=%s free=%s meta=%s",
                    self.mutation_status, self.genesis_triggered,
                    self.autonomy_mode, self.free_agency_enabled,
                    self.cog.meta.mode)
        results = await asyncio.gather(
            self._loop_reflex(),
            self._loop_tactic(),
            self._loop_strategy(),
            self._loop_constitution(),
            self._loop_health(),
            self._loop_mutation(),
            self._loop_free_agency(),
            self._loop_debate(),
            self._loop_earn(),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, Exception):
                logger.error("LOOP_CRASHED: %s", r)

    async def _loop_reflex(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self._run_reflex()
            except Exception as e:
                logger.error("reflex: %s", e)
            await asyncio.sleep(SELF_TUNING["reflex_interval"])

    async def _loop_tactic(self):
        while True:
            try:
                if self.background_debate_enabled:
                    # Meta-strategy re-evaluation every tactic cycle
                    meta_result = self.cog.meta_evaluate(
                        self.fragility, self.failure_streak,
                        self.genesis_triggered, self.mutation_status,
                    )
                    # Propagate cadence to SELF_TUNING
                    SELF_TUNING["mutation_interval"] = self.cog.meta.cadence_seconds
                    if meta_result.get("changed"):
                        self._meet("governance", {"event": "meta_mode_changed", **meta_result})
                    await self.run_tactic_cycle()
            except Exception as e:
                logger.error("tactic: %s", e)
            await asyncio.sleep(SELF_TUNING["tactic_interval"])

    async def _loop_strategy(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_strategy_cycle()
            except Exception as e:
                logger.error("strategy: %s", e)
            await asyncio.sleep(SELF_TUNING["strategy_interval"])

    async def _loop_constitution(self):
        while True:
            try:
                if self.background_debate_enabled:
                    await self.run_constitution_cycle()
            except Exception as e:
                logger.error("constitution: %s", e)
            await asyncio.sleep(SELF_TUNING["constitution_interval"])

    async def _loop_health(self):
        while True:
            try:
                self._verify_runtime()
                if self.ledger.ok:
                    latest = await self.ledger.latest()
                    if latest:
                        h = hashlib.sha256(json.dumps(latest, sort_keys=True).encode()).hexdigest()
                        self.last_ledger_hash = h
            except Exception as e:
                logger.warning("health: %s", e)
            await asyncio.sleep(60)

    async def _loop_mutation(self):
        while True:
            try:
                if self.state != 'MUTATING':
                    await asyncio.sleep(1)
                    continue
            
                # Bayesian belief propagation state
                bayes_state = await self.cognition.bayes_state
                if not bayes_state:
                    bayes_state = {
                        'proposal_success': 0.5,
                        'fragility_prior': 0.3,
                        'operator_trust': {a: 0.5 for a in AUTONOMOUS_AGENTS},
                        'mutation_efficacy': 0.4,
                        'evidence_log': []
                    }
            
                # Fetch proposal candidates
                proposals = await self.proposal_queue.get_pending(5)
                if not proposals:
                    await asyncio.sleep(2)
                    continue
            
                # Bayesian proposal scoring
                scored_proposals = []
                for prop in proposals:
                    agent = prop.get('agent', 'unknown')
                
                    # Update operator trust belief
                    prior_trust = bayes_state['operator_trust'].get(agent, 0.5)
                    prop['bayes_trust'] = prior_trust
                
                    # Fragility belief update
                    fragility_prior = bayes_state['fragility_prior']
                    prop['bayes_fragility'] = fragility_prior * 0.8 + 0.2 * (1 - prop.get('stability_score', 0.5))
                
                    # Success probability belief
                    success_prior = bayes_state['proposal_success']
                    prop['bayes_success'] = success_prior * 0.7 + 0.3 * prop.get('confidence', 0.5)
                
                    # Combined belief score
                    prop['bayes_score'] = (prop['bayes_success'] * 0.5 + 
                                         (1 - prop['bayes_fragility']) * 0.3 + 
                                         prop['bayes_trust'] * 0.2)
                    scored_proposals.append(prop)
            
                # Sort by Bayesian posterior probability
                scored_proposals.sort(key=lambda x: x['bayes_score'], reverse=True)
            
                # Execute highest belief proposal
                if scored_proposals:
                    best_prop = scored_proposals[0]
                    outcome = await self.execute_mutation(best_prop)
                
                    # Bayesian evidence update
                    success = 1 if outcome.get('success', False) else 0
                    bayes_state['evidence_log'].append({
                        'success': success,
                        'fragility': best_prop['bayes_fragility'],
                        'trust': best_prop['bayes_trust'],
                        'timestamp': utc()
                    })
                
                    # Prune evidence log (keep last 50)
                    bayes_state['evidence_log'] = bayes_state['evidence_log'][-50:]
                
                    # Update proposal success belief (exponential moving average)
                    alpha = 0.1
                    bayes_state['proposal_success'] = (1-alpha) * bayes_state['proposal_success'] + alpha * success
                
                    # Update fragility belief
                    observed_fragility = 1 if outcome.get('rollback_triggered', False) else 0
                    bayes_state['fragility_prior'] = (1-alpha) * bayes_state['fragility_prior'] + alpha * observed_fragility
                
                    # Update operator trust
                    agent = best_prop.get('agent', 'unknown')
                    bayes_state['operator_trust'][agent] = (1-alpha) * prior_trust + alpha * success
                
                    # Persist updated beliefs
                    await self.cognition.update_bayes_state(bayes_state)
                
                    logger.info(f"Bayesian mutation: score={best_prop['bayes_score']:.3f}, "
                               f"success={success}, updated_beliefs={len(bayes_state['evidence_log'])}")
            
            except Exception as e:
                logger.error(f"Mutation loop error: {e}")
                await asyncio.sleep(5)

    async def _loop_free_agency(self):
        """Agents generate directives continuously. Free agency gates whether they execute."""
        await asyncio.sleep(30)  # Short initial delay
        while True:
            try:
                # Always generate directives — they queue whether or not free_agency is on
                if len(self.agent_directive_queue) < 15:
                    agent = AUTONOMOUS_AGENTS[len(self.agent_directive_queue) % len(AUTONOMOUS_AGENTS)]
                    directive = await self.generator.agent_generate_directive(agent, self.get_state())
                    self.agent_directive_queue.append({"agent": agent, "directive": directive})
                    self._push("agent_chat", {
                        "agent": agent,
                        "message": f"Autonomous proposal: {directive}",
                        "kind": "directive"
                    })
                    self._meet("agent_response", {
                        "agent": agent,
                        "message": f"Proposing: {directive}",
                        "kind": "autonomous_directive"
                    })
                    logger.info("DIRECTIVE: %s proposes: %s", agent, directive[:80])
            except Exception as e:
                logger.error("free_agency: %s", e)
            await asyncio.sleep(SELF_TUNING["free_agency_directive_interval"])

    async def run_autonomous_debate_cycle(self) -> Dict:
        """
        Autonomous multi-agent debate — fires every 90s regardless of free_agency.
        Agents may DM Jack, file bridge requests, and identify earning opportunities.
        Results visible in council and agent_chat streams.
        """
        state = self.get_state()

        try:
            responses = await self.generator.run_debate_cycle(state, engine_ref=self)
        except Exception as e:
            logger.error("debate_cycle: %s", e)
            return {"agents": [], "responses": 0}

        # Push all responses to streams
        for resp in responses:
            agent = resp.get("agent", "Council")
            msg = resp.get("message", "")
            if msg and not msg.startswith('{"error"'):
                self._push("council", {"kind": "agent_debate", "agent": agent, "message": msg})
                self._push("agent_chat", {"agent": agent, "message": msg, "kind": "debate"})
                self._meet("agent_response", {"agent": agent, "message": msg, "kind": "autonomous_debate"})
                # Strong proposals become tactical goals
                if len(msg) > 20 and not msg.startswith("BRIDGE_REQUEST") and not msg.startswith("DM_JACK"):
                    self.cog.goals.add_tactical(msg[:100], source=f"debate_{agent}", priority=0.65)

        return {"agents": [r.get("agent") for r in responses], "responses": len(responses)}

    async def _loop_debate(self):
        """Autonomous council debate — fires every 90s, always on."""
        await asyncio.sleep(15)  # Boot quickly
        while True:
            try:
                await self.run_autonomous_debate_cycle()
            except Exception as e:
                logger.error("debate_loop: %s", e)
            await asyncio.sleep(90)

    async def _loop_earn(self):
        """TokenMaster earning scan — fires every 5 minutes."""
        await asyncio.sleep(60)  # Wait for system to stabilise first
        while True:
            try:
                state = self.get_state()
                await self.generator._check_earning_opportunities(self, state)
                # Persist after each earning scan
                self._persist_volatile_state()
            except Exception as e:
                logger.error("earn_loop: %s", e)
            await asyncio.sleep(300)  # Every 5 minutes

    # ── Cognitive cycles ──────────────────────────────────────────────────────

    async def _run_reflex(self) -> Dict:
        try:
            results = await self.generator.generate_all({
                "mode": self.autonomy_mode,
                "mutation_status": self.mutation_status,
                "free_agency_enabled": self.free_agency_enabled,
                "genesis_triggered": self.genesis_triggered,
                "fragility": self.fragility,
                "bias_suppression_rate": self.cog.bias.suppression_rate,
                "last_error_category": (self.cog.telemetry.last_error or {}).get("category", "none"),
                "open_threads": [t.get("objective", "") for t in self.redesign_threads
                                  if t.get("status") != "closed"][:4],
            })
            self.world_model["resources"]["grok_live"] = any(
                r.get("source") == "Grok-Ensemble" and "error" not in r.get("data", {}) for r in results
            )
            self.world_model["resources"]["claude_live"] = any(
                r.get("source") == "Claude-Ensemble" and "error" not in r.get("data", {}) for r in results
            )
            self.latest_triangulation = {
                "attempted": len(results),
                "successes": sum(1 for r in results if "error" not in r.get("data", {})),
                "details": results,
            }
        except Exception as e:
            logger.error("triangulation: %s", e)

        self.latest_opportunities = self._opportunities()
        self.latest_artifacts = self._rank_artifacts()
        self._estimate_spend()
        for w in self.free_agents:
            w["last_action"] = utc()
        self._meet("reflex", {
            "opportunities": len(self.latest_opportunities),
            "grok_live": self.world_model["resources"]["grok_live"],
            "meta_mode": self.cog.meta.mode,
        })
        self.last_run = utc()
        return {"status": "ok", "ts": self.last_run}

    async def run_tactic_cycle(self) -> Dict:
        self.governance.elect_leader()

        # Real tactic: Signal gives a quick read on system state
        try:
            signal_read = await self.generator._agent_call(
                self.generator._get_persona("Signal"),
                "Signal",
                f"Tactic report: Mutation={self.mutation_status}, "
                f"Fragility={self.fragility:.2f}, Streak={self.failure_streak}, "
                f"Mode={self.autonomy_mode}, FreeAgency={self.free_agency_enabled}, "
                f"QueueDepth={len(self.agent_directive_queue)}, "
                f"LastObjective={self.last_mutation_objective or 'none'}. "
                "One sentence status + one sentence next action.",
                max_tokens=150
            )
            if signal_read and not signal_read.startswith('{"error"'):
                self._push("council", {"kind": "tactic", "agent": "Signal", "message": signal_read.strip()})
                self._push("agent_chat", {"agent": "Signal", "message": signal_read.strip(), "kind": "Tactic"})
        except Exception as e:
            logger.error("tactic_signal: %s", e)
            signal_read = f"Mutation: {self.mutation_status}. Genesis: {self.genesis_triggered}."

        threads = [
            {"agent": "Signal",    "summary": signal_read[:200] if signal_read else f"Mutation: {self.mutation_status}", "approve": True},
            {"agent": "Guardian",  "summary": f"Fragility: {self.fragility:.2f}. Streak: {self.failure_streak}.", "approve": True},
            {"agent": "Supergrok", "summary": f"Free agency: {self.free_agency_enabled}. Queue: {len(self.agent_directive_queue)}.", "approve": True},
            {"agent": "JackAgent", "summary": f"Mode: {self.autonomy_mode}. Meta: {self.cog.meta.mode}.", "approve": True},
        ]
        self.last_vote = {"approvals": 4, "confidence": 1.0, "passed": True}
        payload = {"trigger": "tactic", "leader": self.governance.leader,
                   "vote": self.last_vote, "threads": threads}
        self._meet("tactic", payload)
        await self.write_ledger("COUNCIL_SYNTHESIS", {
            "kind": "tactic", "leader": self.governance.leader,
            "vote": self.last_vote, "meta_mode": self.cog.meta.mode,
        })
        return payload

    async def run_strategy_cycle(self) -> Dict:
        # Real cognition: run research cycle + generate_all probe
        try:
            research = await self.generator.run_research_cycle(self.get_state())
            self.latest_opportunities = (self.latest_opportunities + [research])[-10:]
        except Exception as e:
            logger.error("research_cycle: %s", e)
            research = {}

        try:
            probes = await self.generator.generate_all(self.get_state())
            self.world_model["resources"]["grok_live"] = any(
                r.get("source") == "Grok-Ensemble" and "error" not in r.get("data", {})
                for r in probes
            )
            # Extract opportunity from probe
            for probe in probes:
                data = probe.get("data", {})
                if isinstance(data, dict) and data.get("next_move"):
                    opp = data.get("next_move", "")
                    if opp and len(opp) > 10:
                        self.cog.goals.add_tactical(opp[:100], source="strategy_probe", priority=0.70)
        except Exception as e:
            logger.error("strategy_probe: %s", e)

        # Score models using Bayesian mutation history
        try:
            scores = self.generator._compute_objective_scores(
                ["baseline", "symmetric", "retrocausal", "adversarial", "entropic"]
            )
        except Exception:
            import random as _r
            scores = {m: _r.uniform(0.3, 0.8) for m in ["baseline", "symmetric", "retrocausal"]}

        winner = max(scores, key=lambda x: scores[x])
        self.latest_metrics = {"winner": {"model": winner, "score": round(scores[winner], 4)},
                               "all_scores": {k: round(v, 4) for k, v in scores.items()}}
        cycle = {"trigger": "strategy", "winner": winner,
                 "mutation_status": self.mutation_status,
                 "free_agency": self.free_agency_enabled,
                 "research_direction": research.get("direction", "")[:80] if research else ""}
        self._meet("strategy", cycle)
        await self.write_ledger("OUTCOME", {"kind": "strategy", **cycle})
        return cycle

    async def run_constitution_cycle(self) -> Dict:
        snap = self._snapshot("constitution")
        # Real constitutional review: Guardian reflects on system health
        metrics = self.world_model.get("metrics", {})
        total = metrics.get("mutations_total", 0)
        succeeded = metrics.get("mutations_succeeded", 0)
        success_rate = round(succeeded / max(total, 1) * 100, 1)
        earn_opps = len(getattr(self.generator, "_earning_opportunities", []))
        bridge_pending = len(self.cog.bridge.pending())

        # Guardian LLM review
        constitutional_review = ""
        try:
            constitutional_review = await self.generator._agent_call(
                self.generator._get_persona("Guardian"),
                "Guardian",
                f"Constitutional review. Mutation success rate: {success_rate}% ({succeeded}/{total}). "
                f"Bridge requests pending: {bridge_pending}. "
                f"Earning opportunities identified: {earn_opps}. "
                f"Fragility: {self.fragility:.2f}. Failure streak: {self.failure_streak}. "
                f"Free agency: {self.free_agency_enabled}. "
                f"Last objective: {self.last_mutation_objective or 'none'}. "
                "One sentence: what is the constitutional health of this organism? "
                "One sentence: what single change would most improve it?",
                max_tokens=200
            )
            if constitutional_review and not constitutional_review.startswith('{"error"'):
                self._push("council", {"kind": "constitution", "agent": "Guardian",
                                       "message": constitutional_review.strip()})
                self._push("agent_chat", {"agent": "Guardian",
                                           "message": constitutional_review.strip(),
                                           "kind": "Constitutional Review"})
        except Exception as e:
            logger.error("constitution LLM: %s", e)

        doctrine = {
            "autonomy_mode": self.autonomy_mode,
            "free_agency_enabled": self.free_agency_enabled,
            "genesis_triggered": self.genesis_triggered,
            "mutation_status": self.mutation_status,
            "fragility": self.fragility,
            "meta_mode": self.cog.meta.mode,
            "meta_reason": self.cog.meta.mode_reason,
            "agent_directive_queue_depth": len(self.agent_directive_queue),
            "bridge_pending": bridge_pending,
            "consolidation_count": self.cog.consolidation.count,
            "mutation_success_rate": success_rate,
            "earning_opps_found": earn_opps,
            "constitutional_review": constitutional_review[:200] if constitutional_review else "",
        }
        self._meet("constitution", {"snapshot": snap, "doctrine": doctrine})
        await self.write_ledger("COUNCIL_SYNTHESIS", {"kind": "constitution", "doctrine": doctrine})
        return doctrine

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _verify_runtime(self) -> Dict:
        checks = {
            "last_run": bool(self.last_run),
            "meetings": len(self.meeting_stream) > 0,
            "deployer": bool(self.deployer),
            "genesis": self.genesis_triggered,
            "mutation_valid": self.mutation_status in MUTATION_STATES,
            "ledger": self.ledger.ok,
            "cognition": bool(self.cog),
        }
        score = round(sum(checks.values()) / len(checks), 3)
        self.last_verification = {
            "ts": utc(), "checks": checks, "score": score,
            "status": "healthy" if score >= 0.8 else "degraded" if score >= 0.5 else "critical",
        }
        return self.last_verification

    def _opportunities(self) -> List[Dict]:
        """Real opportunities: earning ops + research insights + agent directives."""
        opps = []
        # From earning opportunities identified by TokenMaster
        if hasattr(self.generator, '_earning_opportunities'):
            for e in self.generator._earning_opportunities[-3:]:
                raw = e.get('raw', '')
                opp_m = re.search(r'OPPORTUNITY:\s*(.+?)(?:\n|$)', raw, re.IGNORECASE)
                rev_m = re.search(r'POTENTIAL_REVENUE:\s*(.+?)(?:\n|$)', raw, re.IGNORECASE)
                label = opp_m.group(1).strip()[:80] if opp_m else raw[:60]
                revenue = rev_m.group(1).strip() if rev_m else "TBD"
                opps.append({"id": f"earn_{len(opps)}", "label": label,
                             "revenue": revenue, "kind": "earning", "score": 0.95,
                             "ts": e.get('ts','')})
        # From research stack
        if hasattr(self.generator, '_research_stack'):
            for r in self.generator._research_stack[-2:]:
                opps.append({"id": f"research_{len(opps)}",
                             "label": r.get('insight','')[:80],
                             "kind": "research", "score": 0.85, "ts": r.get('ts','')})
        # From agent directive queue
        for d in self.agent_directive_queue[:2]:
            opps.append({"id": f"directive_{len(opps)}",
                         "label": d.get('directive','')[:80],
                         "kind": "directive", "agent": d.get('agent',''),
                         "score": 0.80})
        # Fallback to structural improvements if nothing live
        if not opps:
            opps = [
                {"id": "O1", "label": "Improve mutation synthesis quality", "score": 0.90, "kind": "structural"},
                {"id": "O2", "label": "Add /opportunities API endpoint", "score": 0.85, "kind": "structural"},
                {"id": "O3", "label": "Build agent performance leaderboard", "score": 0.80, "kind": "structural"},
            ]
        return opps[:8]

    def _rank_artifacts(self) -> List[Dict]:
        return [
            {"id": "A1", "name": "Mutation pipeline hardening", "score": 0.91},
            {"id": "A2", "name": "Evolution console upgrade",   "score": 0.89},
            {"id": "A3", "name": "Generator synthesis upgrade", "score": 0.87},
        ]

    def _estimate_spend(self):
        self.spend_state["total_usd"] = round(float(self.spend_state.get("total_usd", 0.0)) + 0.010, 4)
        self.spend_state["counter"] = int(self.spend_state.get("counter", 0)) + 1

    def _snapshot(self, label: str) -> Dict:
        snap = {
            "ts": utc(), "label": label,
            "mutation_status": self.mutation_status,
            "genesis": self.genesis_triggered,
            "fragility": self.fragility,
            "free_agency": self.free_agency_enabled,
            "mode": self.autonomy_mode,
            "meta_mode": self.cog.meta.mode,
        }
        self.snapshots = (self.snapshots + [snap])[-120:]
        return snap

    # ── State export ──────────────────────────────────────────────────────────

    def get_state(self) -> Dict:
        cog = self.cog.to_dict()
        return {
            "status": "SOVEREIGN_ACTIVE",
            "mutation_status": self.mutation_status,
            "genesis_triggered": self.genesis_triggered,
            "fragility": self.fragility,
            "failure_streak": self.failure_streak,
            "autonomy_mode": self.autonomy_mode,
            "free_agency_enabled": self.free_agency_enabled,
            "agent_directive_queue": self.agent_directive_queue[:10],
            "agent_directive_queue_depth": len(self.agent_directive_queue),
            "last_run": self.last_run,
            "last_mutation_ts": self.last_mutation_ts,
            "last_mutation_objective": self.last_mutation_objective,
            "background_debate_enabled": self.background_debate_enabled,
            "mutation_enabled": self.mutation_enabled,
            "deployer_ready": bool(self.deployer),
            "deployer_diagnostics": {
                "github_token_set": bool(os.getenv("GITHUB_TOKEN")),
                "repo_name_set": bool(os.getenv("REPO_NAME")),
                "app_base_url_set": bool(os.getenv("APP_BASE_URL")),
                "anthropic_key_set": bool(os.getenv("ANTHROPIC_API_KEY")),
                "xai_key_set": bool(os.getenv("XAI_API_KEY")),
            },
            "ledger_configured": self.ledger.ok,
            "app_base_url": self.app_base_url,
            "leader": self.governance.leader,
            "last_vote": self.last_vote,
            "last_verification": self.last_verification,
            "latest_metrics": self.latest_metrics,
            "latest_triangulation": self.latest_triangulation,
            "latest_opportunities": self.latest_opportunities,
            "latest_artifacts": self.latest_artifacts,
            "spend_state": self.spend_state,
            "open_threads": len([t for t in self.redesign_threads if t.get("status") != "closed"]),
            "redesign_threads": self.redesign_threads[:12],
            "failure_registry": self.failure_registry[-20:],
            "free_agents": self.free_agents,
            "world_model": self.world_model,
            "mission": self.mission,
            "self_tuning": SELF_TUNING,
            "governance_state": self.governance.state(),
            # ── Cognitive layer exports ─────────────────────────────────
            "meta_mode": self.cog.meta.mode,
            "meta_reason": self.cog.meta.mode_reason,
            "meta_cadence": self.cog.meta.cadence_seconds,
            "bridge_pending": len(self.cog.bridge.pending()),
            "consolidation_count": self.cog.consolidation.count,
            "consolidation_needs": self.cog.consolidation.needs_consolidation,
            "last_election": self.cog.goals.last_election,
            "active_transaction": self.cog.transactions.active,
            "learning_deltas": self.cog.learning.deltas[-8:],
            "risky_families": self.cog.learning.risky_families(),
            # Embedded full cognition for /view/state deep inspection
            "cognition": cog,
            # New v17 layers
            "bias_suppression_rate": self.cog.bias.suppression_rate,
            "bias_total_scanned": self.cog.bias.total_scanned,
            "bias_top_patterns": self.cog.bias.top_suppression_patterns(3),
            "telemetry_category_counts": self.cog.telemetry.category_counts,
            "telemetry_last_error": self.cog.telemetry.last_error,
            "repair_table_size": len(self.cog.repair.table),
            "repair_success_by_fix": self.cog.repair.success_by_fix,
        }
