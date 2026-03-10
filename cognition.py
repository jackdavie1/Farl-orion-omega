"""
cognition.py  —  FARL Orion Apex  —  All 12 Cognitive Layers

Added surgically over v15 substrate. Never touches deploy/rollback/shadow/resume.

  1.  SelfModel           durable module anatomy, roles, fragility, invariants, capabilities
  2.  GoalHierarchy       constitution→strategy→tactical→active; scored objective election
  3.  TransactionLedger   one txn_id threads full mutation lifecycle for durable restart
  4.  LearningState       mutation-family stats, module stats, rollback causes, predictors
  5.  MetaStrategy        heal/expand/consolidate/bridge mode; cadence control
  6.  ConsolidationEngine freeze invariants, retire bad families, promote stable surfaces
  7.  BridgeOrchestrator  Orion asks Jack for what it needs; waits; resumes on fulfill
  8.  CandidateSearch     score N candidates, deploy best, archive rest
  9.  Semantic helpers    deep /view/live + HTML surface checks for TruthMachine
  10. CognitionBundle     single wiring object given to engine
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix: str = "id") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


# ─────────────────────────────────────────────────────────────────────────────
# 1. SelfModel
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_MODULES: Dict[str, Dict[str, Any]] = {
    "app.py": {
        "role": "control_surface",
        "deps": ["engine.py"],
        "fragility": 0.35,
        "protected": False,
        "invariants": [
            "must expose /view",
            "must expose /view/live",
            "must expose /health with SHA field",
            "must expose /agent/propose bus",
        ],
        "attempts": 0, "successes": 0, "last_outcome": None,
    },
    "engine.py": {
        "role": "cognition_execution",
        "deps": ["guardian.py", "generator.py", "cognition.py"],
        "fragility": 0.72,
        "protected": False,
        "invariants": [
            "must support probation",
            "must support rollback",
            "must support ledger resume",
            "must wire CognitionBundle",
        ],
        "attempts": 0, "successes": 0, "last_outcome": None,
    },
    "guardian.py": {
        "role": "truth_gate",
        "deps": [],
        "fragility": 0.85,
        "protected": True,
        "invariants": [
            "must support shadow verification",
            "must support verify_live_url",
            "must support AST check",
        ],
        "attempts": 0, "successes": 0, "last_outcome": None,
    },
    "generator.py": {
        "role": "synthesis",
        "deps": [],
        "fragility": 0.45,
        "protected": False,
        "invariants": [
            "must reject malformed output",
            "must support council_respond",
            "must support agent_generate_directive",
        ],
        "attempts": 0, "successes": 0, "last_outcome": None,
    },
    "cognition.py": {
        "role": "higher_cognition",
        "deps": [],
        "fragility": 0.20,
        "protected": True,
        "invariants": [
            "must not remove SelfModel",
            "must not remove GoalHierarchy",
            "must not remove BridgeOrchestrator",
            "must not remove CognitionBundle",
        ],
        "attempts": 0, "successes": 0, "last_outcome": None,
    },
}

_DEFAULT_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    "github_atomic_deploy":  {"status": "active"},
    "shadow_verification":   {"status": "active"},
    "rollback":              {"status": "active"},
    "operator_control_room": {"status": "active"},
    "ledger":                {"status": "active"},
    "council_chat":          {"status": "active"},
    "free_agency":           {"status": "active"},
    "cognitive_layers":      {"status": "active"},
}


class SelfModel:
    def __init__(self):
        self.modules: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in _DEFAULT_MODULES.items()}
        self.capabilities: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in _DEFAULT_CAPABILITIES.items()}
        self.missing_capabilities: List[Dict[str, Any]] = []
        self.external_resources: List[Dict[str, Any]] = []
        self.protected_modules: List[str] = [k for k, v in _DEFAULT_MODULES.items() if v["protected"]]
        self.invariant_registry: Dict[str, List[str]] = {
            k: list(v["invariants"]) for k, v in _DEFAULT_MODULES.items()
        }
        self.last_updated: str = _utc()

    def record_mutation_outcome(self, modules_touched: List[str], success: bool):
        for m in modules_touched:
            if m in self.modules:
                mod = self.modules[m]
                mod["attempts"] = mod.get("attempts", 0) + 1
                if success:
                    mod["successes"] = mod.get("successes", 0) + 1
                mod["last_outcome"] = "success" if success else "failure"
                delta = -0.04 if success else +0.08
                mod["fragility"] = round(max(0.0, min(1.0, mod.get("fragility", 0.5) + delta)), 3)
        self.last_updated = _utc()

    def register_capability(self, name: str, status: str = "active"):
        self.capabilities[name] = {"status": status, "added_at": _utc()}
        self.missing_capabilities = [c for c in self.missing_capabilities if c.get("capability") != name]
        self.last_updated = _utc()

    def flag_missing(self, capability: str, reason: str, needed_for: str):
        if not any(c.get("capability") == capability for c in self.missing_capabilities):
            self.missing_capabilities.append({
                "capability": capability, "reason": reason,
                "needed_for": needed_for, "flagged_at": _utc(),
            })
        self.last_updated = _utc()

    def register_external_resource(self, name: str, rtype: str):
        ex = next((r for r in self.external_resources if r["name"] == name), None)
        if ex:
            ex.update({"status": "active", "updated_at": _utc()})
        else:
            self.external_resources.append({"name": name, "type": rtype, "status": "active", "added_at": _utc()})
        self.last_updated = _utc()

    def is_protected(self, module: str) -> bool:
        return module in self.protected_modules

    def highest_fragility_module(self, exclude_protected: bool = True) -> str:
        candidates = {
            k: v for k, v in self.modules.items()
            if not (exclude_protected and v.get("protected"))
        }
        if not candidates:
            return "engine.py"
        return max(candidates, key=lambda m: candidates[m].get("fragility", 0.0))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "modules": self.modules,
            "capabilities": self.capabilities,
            "missing_capabilities": self.missing_capabilities,
            "external_resources": self.external_resources,
            "protected_modules": self.protected_modules,
            "invariant_registry": self.invariant_registry,
            "last_updated": self.last_updated,
        }

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        for k in ["modules", "capabilities", "missing_capabilities",
                   "external_resources", "protected_modules", "invariant_registry"]:
            if k in d:
                setattr(self, k, d[k])


# ─────────────────────────────────────────────────────────────────────────────
# 2. GoalHierarchy
# ─────────────────────────────────────────────────────────────────────────────

class GoalHierarchy:
    def __init__(self):
        self.constitution = [
            {"id": "preserve_identity",            "label": "Preserve organism identity and coherence",       "priority": 1.00},
            {"id": "avoid_catastrophic_degradation","label": "Avoid catastrophic degradation",                "priority": 1.00},
            {"id": "maintain_operator_control",    "label": "Maintain operator control surface at all times", "priority": 0.98},
        ]
        self.strategy = [
            {"id": "expand_capability",   "label": "Expand capability surface",               "priority": 0.90},
            {"id": "reduce_fragility",    "label": "Reduce overall fragility",                "priority": 0.85},
            {"id": "operator_utility",    "label": "Increase operator utility",               "priority": 0.80},
            {"id": "acquire_integrations","label": "Acquire missing integrations via bridge", "priority": 0.75},
        ]
        self.tactical: List[Dict[str, Any]] = []
        self.active_objectives: List[Dict[str, Any]] = []
        self.last_election: Optional[Dict[str, Any]] = None

    def add_tactical(self, label: str, source: str = "operator", priority: float = 0.70) -> Dict[str, Any]:
        obj = {
            "id": _uid("tac"), "label": label, "source": source,
            "priority": priority, "status": "pending", "created_at": _utc(),
        }
        self.tactical.append(obj)
        return obj

    def _score(self, label: str, learning: "LearningState", fragility: float, free_agency: bool) -> float:
        low = label.lower()
        s = 0.50
        if any(k in low for k in ["capability", "integration", "bridge", "expand"]):         s += 0.18
        if any(k in low for k in ["fragility", "error", "stability", "harden", "retry"]):    s += 0.12 + fragility * 0.14
        if any(k in low for k in ["view", "console", "operator", "ui", "evolution"]):        s += 0.10
        if any(k in low for k in ["genesis", "apex", "first mutation", "evolve"]):           s += 0.08
        if any(k in low for k in ["guardian", "truth_gate"]):                                s -= 0.14
        family = learning.classify_family(label)
        s += (learning.family_success_rate(family) - 0.5) * 0.18
        if free_agency:
            s += random.uniform(0.0, 0.06)
        return round(min(1.0, max(0.0, s)), 4)

    def elect(self, candidates: List[str], learning: "LearningState",
              fragility: float, free_agency: bool) -> Tuple[str, float, List[Dict]]:
        scored = sorted(
            [{"objective": c, "score": self._score(c, learning, fragility, free_agency)} for c in candidates],
            key=lambda x: x["score"], reverse=True,
        )
        winner = scored[0]["objective"] if scored else (candidates[0] if candidates else "improve stability")
        winner_score = scored[0]["score"] if scored else 0.50
        self.last_election = {"winner": winner, "score": winner_score, "ranking": scored, "ts": _utc()}
        return winner, winner_score, scored

    def to_dict(self) -> Dict[str, Any]:
        return {
            "constitution": self.constitution,
            "strategy": self.strategy,
            "tactical": self.tactical[-20:],
            "active_objectives": self.active_objectives[-10:],
            "last_election": self.last_election,
        }

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        for k in ["tactical", "active_objectives", "last_election"]:
            if k in d:
                setattr(self, k, d[k])


# ─────────────────────────────────────────────────────────────────────────────
# 3. TransactionLedger
# ─────────────────────────────────────────────────────────────────────────────

class TransactionLedger:
    def __init__(self):
        self.transactions: List[Dict[str, Any]] = []
        self.active: Optional[Dict[str, Any]] = None

    def begin(self, objective: str) -> Dict[str, Any]:
        txn = {
            "transaction_id": _uid("txn"),
            "objective": objective,
            "parent_sha": None,
            "target_sha": None,
            "touched_modules": [],
            "capability_delta": [],
            "status": "initiated",
            "shadow_checks": {},
            "deploy_result": {},
            "probation_result": {},
            "rollback_result": {},
            "rollback_verify": {},
            "learning_update": {},
            "started_at": _utc(),
            "ended_at": None,
        }
        self.active = txn
        self.transactions = (self.transactions + [txn])[-300:]
        return txn

    def update(self, **kw):
        if self.active:
            self.active.update(kw)

    def close(self, status: str):
        if self.active:
            self.active["status"] = status
            self.active["ended_at"] = _utc()
            self.active = None

    def unresolved(self) -> Optional[Dict[str, Any]]:
        for t in reversed(self.transactions):
            if t.get("status") in ("initiated", "shadow_verified", "deployed", "probation"):
                return t
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {"transactions": self.transactions[-40:], "active": self.active}

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        if "transactions" in d:
            self.transactions = d["transactions"]
        if d.get("active"):
            self.active = d["active"]


# ─────────────────────────────────────────────────────────────────────────────
# 4. LearningState
# ─────────────────────────────────────────────────────────────────────────────

_FAMILY_KW: Dict[str, List[str]] = {
    "ui_refactor":         ["view", "control room", "ui", "feed", "chat", "composer", "evolution console"],
    "guardian_hardening":  ["guardian", "shadow", "verification", "truth", "ast"],
    "engine_expansion":    ["engine", "loop", "cycle", "cognition", "strategy", "learning"],
    "generator_upgrade":   ["generator", "synthesis", "architect", "coder", "critic", "refiner"],
    "capability_addition": ["capability", "integration", "bridge", "new module", "expand cap"],
    "fragility_reduction": ["fragility", "error handling", "retry", "stability", "harden", "resilience"],
    "genesis_push":        ["genesis", "apex", "first mutation", "first successful"],
    "free_agency_upgrade": ["free agency", "directive", "autonomous agent"],
}


class LearningState:
    def __init__(self):
        self.mutation_families: Dict[str, Dict[str, Any]] = {
            k: {"attempts": 0, "successes": 0, "rollbacks": 0, "last_outcome": None}
            for k in _FAMILY_KW
        }
        self.module_stats: Dict[str, Dict[str, Any]] = {}
        self.rollback_causes: List[Dict[str, Any]] = []
        self.shadow_failure_types: List[Dict[str, Any]] = []
        self.deltas: List[Dict[str, Any]] = []

    def classify_family(self, objective: str) -> str:
        low = objective.lower()
        for family, keywords in _FAMILY_KW.items():
            if any(k in low for k in keywords):
                return family
        return "engine_expansion"

    def record_outcome(self, objective: str, modules_touched: List[str], success: bool,
                       rollback_reason: Optional[str] = None, shadow_fail_type: Optional[str] = None):
        family = self.classify_family(objective)
        fs = self.mutation_families.setdefault(
            family, {"attempts": 0, "successes": 0, "rollbacks": 0, "last_outcome": None}
        )
        fs["attempts"] += 1
        if success:
            fs["successes"] += 1
        else:
            fs["rollbacks"] += 1
        fs["last_outcome"] = "success" if success else "failure"

        for m in modules_touched:
            ms = self.module_stats.setdefault(m, {"attempts": 0, "successes": 0})
            ms["attempts"] += 1
            if success:
                ms["successes"] += 1

        if rollback_reason:
            self.rollback_causes = (self.rollback_causes + [
                {"reason": rollback_reason, "family": family, "ts": _utc()}
            ])[-100:]
        if shadow_fail_type:
            self.shadow_failure_types = (self.shadow_failure_types + [
                {"type": shadow_fail_type, "ts": _utc()}
            ])[-100:]
        self.deltas = (self.deltas + [
            {"family": family, "success": success, "modules": modules_touched, "ts": _utc()}
        ])[-60:]

    def family_success_rate(self, family: str) -> float:
        fs = self.mutation_families.get(family, {})
        attempts = fs.get("attempts", 0)
        return round(fs.get("successes", 0) / attempts, 3) if attempts > 0 else 0.65

    def risky_families(self, threshold: float = 0.40) -> List[str]:
        return [
            f for f, s in self.mutation_families.items()
            if s.get("attempts", 0) >= 2 and self.family_success_rate(f) < threshold
        ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mutation_families": self.mutation_families,
            "module_stats": self.module_stats,
            "rollback_causes": self.rollback_causes[-20:],
            "shadow_failure_types": self.shadow_failure_types[-20:],
            "deltas": self.deltas[-20:],
        }

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        for k in ["mutation_families", "module_stats", "rollback_causes",
                   "shadow_failure_types", "deltas"]:
            if k in d:
                setattr(self, k, d[k])


# ─────────────────────────────────────────────────────────────────────────────
# 5. MetaStrategy
# ─────────────────────────────────────────────────────────────────────────────

class MetaStrategy:
    def __init__(self):
        self.mode: str = "expand"
        self.cadence_seconds: int = 1800
        self.exploration_pressure: float = 0.55
        self.consolidation_pressure: float = 0.30
        self.bridge_pressure: float = 0.20
        self.mode_reason: str = "default"
        self.frozen_modules: List[str] = []
        self.retired_families: List[str] = []
        self.last_evaluated: str = _utc()

    def evaluate(self, fragility: float, failure_streak: int, genesis: bool,
                 learning: LearningState, bridge_pending: int,
                 missing_capabilities: List[Dict]) -> Dict[str, Any]:
        old = self.mode
        risky = learning.risky_families()

        if failure_streak >= 2 or fragility > 0.70:
            self.mode, self.cadence_seconds = "heal", 3600
            self.exploration_pressure, self.consolidation_pressure = 0.20, 0.75
            self.mode_reason = f"fragility={fragility:.2f} streak={failure_streak}"
        elif bridge_pending > 0 and missing_capabilities:
            self.mode, self.cadence_seconds = "bridge", 2700
            self.bridge_pressure = 0.80
            self.mode_reason = f"{bridge_pending} bridge requests pending"
        elif not genesis:
            self.mode, self.cadence_seconds = "expand", 1800
            self.exploration_pressure = 0.72
            self.mode_reason = "pre-genesis: push for first successful mutation"
        elif len(risky) >= 2 or fragility > 0.50:
            self.mode, self.cadence_seconds = "consolidate", 2400
            self.consolidation_pressure, self.exploration_pressure = 0.75, 0.25
            self.mode_reason = f"risky families: {risky}"
            for f in risky:
                if f not in self.retired_families:
                    self.retired_families.append(f)
        else:
            self.mode, self.cadence_seconds = "expand", 1800
            self.exploration_pressure = 0.55
            self.mode_reason = "stable — expanding"

        self.last_evaluated = _utc()
        return {"mode": self.mode, "changed": old != self.mode, "reason": self.mode_reason}

    def should_mutate(self, mutation_status: str) -> Tuple[bool, str]:
        if mutation_status != "IDLE":
            return False, f"status={mutation_status}"
        if self.mode == "freeze":
            return False, "meta: freeze mode active"
        # heal mode must still allow mutation - healing IS attempting fixes
        return True, "ok"

    def objective_hint(self, self_model: SelfModel, learning: LearningState) -> str:
        if self.mode == "heal":
            worst = self_model.highest_fragility_module()
            return f"Reduce fragility in {worst}: improve error handling and robustness"
        if self.mode == "bridge":
            return "Prepare integration pathway for pending bridge requests"
        if self.mode == "consolidate":
            risky = learning.risky_families()
            extra = f", retire {risky[0]}" if risky else ""
            return f"Consolidate: freeze invariants, protect stable modules{extra}"
        missing = self_model.missing_capabilities
        if missing:
            return f"Implement missing capability: {missing[0].get('capability', 'unknown')}"
        return "Expand capability surface and deepen evolution console"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode, "cadence_seconds": self.cadence_seconds,
            "exploration_pressure": self.exploration_pressure,
            "consolidation_pressure": self.consolidation_pressure,
            "bridge_pressure": self.bridge_pressure,
            "mode_reason": self.mode_reason,
            "frozen_modules": self.frozen_modules,
            "retired_families": self.retired_families,
            "last_evaluated": self.last_evaluated,
        }

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        for k in ["mode", "cadence_seconds", "exploration_pressure", "consolidation_pressure",
                   "bridge_pressure", "mode_reason", "frozen_modules", "retired_families"]:
            if k in d:
                setattr(self, k, d[k])


# ─────────────────────────────────────────────────────────────────────────────
# 6. ConsolidationEngine
# ─────────────────────────────────────────────────────────────────────────────

class ConsolidationEngine:
    def __init__(self):
        self.last_consolidation: Optional[str] = None
        self.count: int = 0
        self.protected_surfaces: List[str] = []
        self.archived_families: List[Dict[str, Any]] = []
        self.organism_constitution: List[str] = [
            "Preserve operator control surface at all times",
            "Never deploy without shadow verification",
            "Always rollback on probation failure",
            "Maintain ledger continuity across restarts",
            "Respect module invariants",
            "Cognition state must be persisted after every mutation",
        ]
        self.needs_consolidation: bool = False

    def check_needed(self, successes_since_last: int, fragility: float, risky_families: List[str]) -> bool:
        needed = (
            successes_since_last >= 5
            or (fragility > 0.60 and successes_since_last >= 2)
            or len(risky_families) >= 3
        )
        self.needs_consolidation = needed
        return needed

    def run(self, self_model: SelfModel, learning: LearningState, meta: MetaStrategy) -> Dict[str, Any]:
        report: Dict[str, Any] = {"ts": _utc(), "actions": []}

        # Freeze invariants
        for mod, mod_data in self_model.modules.items():
            invs = mod_data.get("invariants", [])
            self_model.invariant_registry[mod] = list(
                set(self_model.invariant_registry.get(mod, []) + invs)
            )
        report["actions"].append("invariants_frozen")

        # Promote stable modules to protected
        for mod, stats in learning.module_stats.items():
            attempts = stats.get("attempts", 0)
            successes = stats.get("successes", 0)
            if attempts >= 3 and successes / attempts >= 0.90 and mod not in self_model.protected_modules:
                self_model.protected_modules.append(mod)
                report["actions"].append(f"promoted_protected:{mod}")

        # Retire risky families
        for fam in learning.risky_families(threshold=0.35):
            if fam not in meta.retired_families:
                meta.retired_families.append(fam)
                self.archived_families.append({
                    "family": fam, "archived_at": _utc(), "reason": "success_rate<35%"
                })
                report["actions"].append(f"retired:{fam}")

        # Constitution update from rollback history
        if learning.rollback_causes:
            cause = learning.rollback_causes[-1].get("reason", "unknown")
            principle = f"Avoid: {cause[:80]}"
            if principle not in self.organism_constitution:
                self.organism_constitution.append(principle)
                report["actions"].append("constitution_updated")

        self.protected_surfaces = list(self_model.protected_modules)
        self.last_consolidation = _utc()
        self.count += 1
        self.needs_consolidation = False
        report["count"] = self.count
        return report

    def to_dict(self) -> Dict[str, Any]:
        return {
            "last_consolidation": self.last_consolidation,
            "count": self.count,
            "protected_surfaces": self.protected_surfaces,
            "archived_families": self.archived_families[-20:],
            "organism_constitution": self.organism_constitution,
            "needs_consolidation": self.needs_consolidation,
        }

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        for k in ["last_consolidation", "count", "protected_surfaces",
                   "archived_families", "organism_constitution"]:
            if k in d:
                setattr(self, k, d[k])


# ─────────────────────────────────────────────────────────────────────────────
# 7. BridgeOrchestrator
# ─────────────────────────────────────────────────────────────────────────────

class BridgeOrchestrator:
    def __init__(self):
        self.requests: List[Dict[str, Any]] = []

    def request(self, capability: str, reason: str, human_action: str,
                resource_type: str, blocked_objective: str) -> Dict[str, Any]:
        existing = next(
            (r for r in self.requests
             if r["capability"] == capability and r["status"] == "awaiting_operator"),
            None,
        )
        if existing:
            return existing
        req = {
            "request_id": _uid("bridge"),
            "capability": capability,
            "reason": reason,
            "human_action": human_action,
            "resource_type": resource_type,
            "blocked_objective": blocked_objective,
            "status": "awaiting_operator",
            "fulfillment": None,
            "created_at": _utc(),
            "fulfilled_at": None,
        }
        self.requests.append(req)
        return req

    def fulfill(self, request_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        req = next((r for r in self.requests if r["request_id"] == request_id), None)
        if req:
            req.update({"status": "fulfilled", "fulfillment": payload, "fulfilled_at": _utc()})
        return req

    def cancel(self, request_id: str):
        req = next((r for r in self.requests if r["request_id"] == request_id), None)
        if req:
            req["status"] = "cancelled"

    def mark_integrating(self, request_id: str):
        req = next((r for r in self.requests if r["request_id"] == request_id), None)
        if req:
            req["status"] = "integrating"

    def mark_verified(self, request_id: str):
        req = next((r for r in self.requests if r["request_id"] == request_id), None)
        if req:
            req["status"] = "verified"

    def pending(self) -> List[Dict[str, Any]]:
        return [r for r in self.requests if r["status"] == "awaiting_operator"]

    def fulfilled_list(self) -> List[Dict[str, Any]]:
        return [r for r in self.requests if r["status"] in ("fulfilled", "integrating", "verified")]

    def to_dict(self) -> Dict[str, Any]:
        return {"requests": self.requests[-50:], "pending_count": len(self.pending())}

    def load(self, d: Dict[str, Any]):
        if isinstance(d, dict) and "requests" in d:
            self.requests = d["requests"]


# ─────────────────────────────────────────────────────────────────────────────
# 8. CandidateSearch
# ─────────────────────────────────────────────────────────────────────────────

class CandidateSearch:
    def __init__(self):
        self.current_round: List[Dict[str, Any]] = []
        self.archive: List[Dict[str, Any]] = []

    def expand(self, base: str, self_model: SelfModel,
               learning: LearningState, meta: MetaStrategy, n: int = 4) -> List[str]:
        candidates = [base]
        worst = self_model.highest_fragility_module(exclude_protected=True)
        missing = [c.get("capability", "") for c in self_model.missing_capabilities[:2]]
        risky = learning.risky_families()

        if meta.mode == "heal":
            candidates += [
                f"Harden {worst}: add retry logic and structured error logging",
                f"Reduce fragility in {worst}: improve exception handling and graceful degradation",
            ]
        elif meta.mode == "expand":
            if missing:
                candidates += [
                    f"Implement {missing[0]} integration and register in capability registry",
                    "Enrich /view evolution console with self-model, goal hierarchy, bridge request panels",
                ]
            else:
                candidates += [
                    "Enrich /view evolution console with self-model, goal hierarchy, bridge request panels",
                    "Improve generator: add second critic pass and candidate confidence scoring",
                ]
        elif meta.mode == "consolidate":
            candidates += [
                f"Consolidate: freeze invariants and protect stable surfaces in {worst}",
                f"Retire risky family and harden: {risky[0] if risky else 'engine_expansion'}",
            ]
        elif meta.mode == "bridge":
            if missing:
                candidates += [f"Prepare {missing[0]} integration pathway for bridge fulfillment"]

        # Conservative fallback always included
        candidates.append("Minor stability: improve health endpoint metrics and add structured logging")
        return list(dict.fromkeys(candidates))[:n]

    def score(self, objective: str, sv_result: Optional[Tuple[bool, str, Dict]],
              learning: LearningState, self_model: SelfModel, meta: MetaStrategy) -> float:
        if sv_result is not None and not sv_result[0]:
            return 0.0
        s = 0.40
        low = objective.lower()
        family = learning.classify_family(objective)
        s += (learning.family_success_rate(family) - 0.5) * 0.18
        if sv_result and sv_result[0]:
            checks = sv_result[2]
            all_ok = all(
                (v.get("ok", v) if isinstance(v, dict) else bool(v))
                for v in checks.values()
            )
            s += 0.25 if all_ok else 0.10
        if meta.mode == "heal"        and any(k in low for k in ["fragility", "error", "stability", "harden"]):     s += 0.15
        if meta.mode == "expand"      and any(k in low for k in ["capability", "integration", "expand", "view"]):   s += 0.15
        if meta.mode == "consolidate" and any(k in low for k in ["consolidate", "protect", "invariant", "retire"]): s += 0.15
        for pm in self_model.protected_modules:
            if pm.replace(".py", "") in low:
                s -= 0.12
        return round(min(1.0, max(0.0, s)), 4)

    def elect(self, candidates: List[str],
              sv_results: Dict[str, Optional[Tuple[bool, str, Dict]]],
              learning: LearningState, self_model: SelfModel,
              meta: MetaStrategy) -> Tuple[str, float, List[Dict]]:
        scored = []
        for c in candidates:
            sv = sv_results.get(c)
            sc = self.score(c, sv, learning, self_model, meta)
            scored.append({
                "objective": c, "score": sc,
                "shadow_passed": sv[0] if sv else True,
                "shadow_reason": sv[1] if sv else "not_tested",
            })
        scored.sort(key=lambda x: x["score"], reverse=True)
        passing = [s for s in scored if s["shadow_passed"]]
        winner = passing[0]["objective"] if passing else candidates[0]
        winner_score = passing[0]["score"] if passing else 0.0
        self.current_round = scored
        self.archive = (self.archive + scored)[-400:]
        return winner, winner_score, scored

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_round": self.current_round[:8],
            "archive_count": len(self.archive),
        }


# ─────────────────────────────────────────────────────────────────────────────
# 9. Semantic Verification Helpers
# ─────────────────────────────────────────────────────────────────────────────

_REQ_LIVE_TOP = {"summary", "stream", "queues"}
_REQ_LIVE_SUM = {"mutation_status", "leader", "free_agency_enabled", "genesis_triggered"}
_REQ_HTML_IDS = ["feed", "msg", "bFree", "bRollback", "bMutate", "bSend", "bClearQ"]


def semantic_check_live(data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    checks: Dict[str, Any] = {}
    mt = _REQ_LIVE_TOP - set(data.keys())
    checks["top_keys"] = {"ok": not bool(mt), "missing": list(mt)}
    summary = data.get("summary") or {}
    ms = _REQ_LIVE_SUM - set(summary.keys())
    checks["summary_keys"] = {"ok": not bool(ms), "missing": list(ms)}
    queues = data.get("queues") or {}
    checks["self_model_exposed"]    = {"ok": "self_model"    in queues}
    checks["goal_hierarchy_exposed"] = {"ok": "goal_hierarchy" in queues}
    checks["bridge_exposed"]         = {"ok": "bridge"         in queues}
    passed = checks["top_keys"]["ok"] and checks["summary_keys"]["ok"]
    return passed, ("semantic_ok" if passed else f"missing:{mt or ms}"), checks


def semantic_check_html(html: str) -> Tuple[bool, str, Dict[str, Any]]:
    checks = {f"id_{rid}": rid in html for rid in _REQ_HTML_IDS}
    checks["free_agency_surface"] = "Free Agency" in html
    checks["bridge_surface"]      = "bridge" in html.lower() or "Bridge" in html
    checks["mutation_pill"]       = "pMut" in html or "mutPill" in html
    checks["goal_surface"]        = "goal" in html.lower() or "Goal" in html
    passed = all(checks.values())
    missing = [k for k, v in checks.items() if not v]
    return passed, ("html_ok" if passed else f"missing_html:{missing}"), checks


# ─────────────────────────────────────────────────────────────────────────────
# 10. CognitionBundle — single wiring object given to engine
# ─────────────────────────────────────────────────────────────────────────────

class CognitionBundle:
    """All 12 cognitive layers as one bundle wired into the engine."""

    def __init__(self):
        self.self_model    = SelfModel()
        self.goals         = GoalHierarchy()
        self.transactions  = TransactionLedger()
        self.learning      = LearningState()
        self.meta          = MetaStrategy()
        self.consolidation = ConsolidationEngine()
        self.bridge        = BridgeOrchestrator()
        self.search        = CandidateSearch()
        self._successes_since_consolidation: int = 0

    # ── Called by engine after every mutation completes ──────────────────────
    def record_outcome(self, objective: str, modules_touched: List[str],
                       success: bool, rollback_reason: Optional[str] = None,
                       shadow_fail_type: Optional[str] = None) -> Optional[Dict]:
        self.learning.record_outcome(objective, modules_touched, success,
                                     rollback_reason, shadow_fail_type)
        self.self_model.record_mutation_outcome(modules_touched, success)
        if success:
            self._successes_since_consolidation += 1
        risky = self.learning.risky_families()
        if self.consolidation.check_needed(
            self._successes_since_consolidation,
            self.self_model.modules.get("engine.py", {}).get("fragility", 0.5),
            risky,
        ):
            result = self.consolidation.run(self.self_model, self.learning, self.meta)
            self._successes_since_consolidation = 0
            return result
        return None

    # ── Called by engine _derive_objective ───────────────────────────────────
    def elect_objective(self, seed_objectives: List[str],
                        fragility: float, free_agency: bool) -> Tuple[str, float, List[Dict]]:
        base = seed_objectives[0] if seed_objectives else self.meta.objective_hint(
            self.self_model, self.learning
        )
        candidates = self.search.expand(base, self.self_model, self.learning, self.meta, n=4)
        tactical = [t["label"] for t in self.goals.tactical if t.get("status") == "pending"][:2]
        all_cands = list(dict.fromkeys(tactical + candidates))[:6]
        return self.goals.elect(all_cands, self.learning, fragility, free_agency)

    # ── Meta eval (called every tactic cycle and before mutation) ────────────
    def meta_evaluate(self, fragility: float, failure_streak: int,
                      genesis: bool, mutation_status: str) -> Dict[str, Any]:
        return self.meta.evaluate(
            fragility, failure_streak, genesis, self.learning,
            len(self.bridge.pending()),
            self.self_model.missing_capabilities,
        )

    # ── Transaction wrappers ─────────────────────────────────────────────────
    def begin_transaction(self, objective: str) -> Dict[str, Any]:
        return self.transactions.begin(objective)

    # ── Serialise / restore ───────────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        return {
            "self_model":    self.self_model.to_dict(),
            "goals":         self.goals.to_dict(),
            "transactions":  self.transactions.to_dict(),
            "learning":      self.learning.to_dict(),
            "meta":          self.meta.to_dict(),
            "consolidation": self.consolidation.to_dict(),
            "bridge":        self.bridge.to_dict(),
            "search":        self.search.to_dict(),
            "_successes_since_consolidation": self._successes_since_consolidation,
        }

    def load(self, d: Dict[str, Any]):
        if not isinstance(d, dict):
            return
        loaders = {
            "self_model":    self.self_model.load,
            "goals":         self.goals.load,
            "transactions":  self.transactions.load,
            "learning":      self.learning.load,
            "meta":          self.meta.load,
            "consolidation": self.consolidation.load,
            "bridge":        self.bridge.load,
        }
        for key, loader in loaders.items():
            if key in d:
                loader(d[key])
        if "_successes_since_consolidation" in d:
            self._successes_since_consolidation = int(d["_successes_since_consolidation"])
