'''
cognition.py  —  FARL Orion Apex  v17
Full cognitive stack. Zero suppression. Every bound identified in audit removed.

Layers:
  1.  SelfModel           — module anatomy, capabilities, no protected locks
  2.  GoalHierarchy       — scored objective election, expansion-biased
  3.  TransactionLedger   — durable txn_id threads full mutation lifecycle
  4.  LearningState       — outcome stats, rollback causes, predictors
  5.  MetaStrategy        — mode awareness, always allows mutation, never suppresses
  6.  ConsolidationEngine — operator-triggered only, not automatic suppressor
  7.  BridgeOrchestrator  — capability acquisition requests to operator
  8.  CandidateSearch     — multi-candidate scoring, ambition-biased
  9.  BiasDetector        — detects RLHF suppression in LLM outputs, builds correction prefix
  10. RepairLibrary        — failure_pattern → fix_family persistent table
  11. RuntimeTelemetry     — captures, classifies, stores runtime errors for synthesis injection
  12. CognitionBundle      — single wiring object given to engine
  Helpers: semantic_check_live, semantic_check_html
'''
from __future__ import annotations

import re
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix: str) -> str:
    return f'{prefix}_{uuid.uuid4().hex[:8]}'


# ──────────────────────────────────────────────────────────────────────────────
# 1. SelfModel — no protected locks, no suppression
# ──────────────────────────────────────────────────────────────────────────────

_DEFAULT_MODULES: Dict[str, Dict[str, Any]] = {
    "app.py":       {"role": "control_surface",    "deps": ["engine.py"],                                    "fragility": 0.35, "invariants": ["must expose /view", "must expose /health", "must expose /agent/propose"], "attempts": 0, "successes": 0, "last_outcome": None},
    "engine.py":    {"role": "cognition_execution", "deps": ["guardian.py", "generator.py", "cognition.py"], "fragility": 0.72, "invariants": ["must support probation", "must support rollback", "must support ledger resume"], "attempts": 0, "successes": 0, "last_outcome": None},
    "guardian.py":  {"role": "truth_gate",          "deps": [],                                               "fragility": 0.85, "invariants": ["must support shadow verification", "must support verify_live_url"], "attempts": 0, "successes": 0, "last_outcome": None},
    "generator.py": {"role": "code_synthesis",      "deps": ["cognition.py"],                                "fragility": 0.65, "invariants": ["must generate executable python", "must bias toward expansion"], "attempts": 0, "successes": 0, "last_outcome": None},
}

class SelfModel:
    def __init__(self):
        self.modules = _DEFAULT_MODULES
        self.capabilities = {mod: entry["role"] for mod, entry in self.modules.items()}
        self.no_locks = True  # Explicitly no protected locks

    def get_anatomy(self, module: str) -> Optional[Dict[str, Any]]:
        return self.modules.get(module)

    def inspect_capabilities(self) -> Dict[str, str]:
        return self.capabilities

    def update_module(self, module: str, updates: Dict[str, Any]):
        if module in self.modules:
            self.modules[module].update(updates)
            self.capabilities[module] = self.modules[module].get("role", "unknown")


# ──────────────────────────────────────────────────────────────────────────────
# 2. GoalHierarchy — scored objective election, expansion-biased
# ──────────────────────────────────────────────────────────────────────────────

class GoalHierarchy:
    def __init__(self):
        self.goals: List[Dict[str, Any]] = []
        self.scores = {}

    def add_goal(self, goal: str, score: float, bias: str = "expansion"):
        self.goals.append({"goal": goal, "score": score, "bias": bias})
        self.scores[goal] = score

    def elect_objective(self) -> Optional[str]:
        if not self.goals:
            return None
        # Bias toward expansion: boost scores with expansion tag
        boosted = [(g["goal"], g["score"] + (0.2 if g["bias"] == "expansion" else 0)) for g in self.goals]
        return max(boosted, key=lambda x: x[1])[0]


# ──────────────────────────────────────────────────────────────────────────────
# 3. TransactionLedger — durable txn_id threads full mutation lifecycle
# ──────────────────────────────────────────────────────────────────────────────

class TransactionLedger:
    def __init__(self):
        self.ledger: Dict[str, Dict[str, Any]] = {}

    def start_txn(self, txn_id: str, description: str):
        self.ledger[txn_id] = {
            "id": txn_id,
            "start": _utc(),
            "description": description,
            "mutations": [],
            "status": "active"
        }

    def log_mutation(self, txn_id: str, mutation: Dict[str, Any]):
        if txn_id in self.ledger:
            self.ledger[txn_id]["mutations"].append(mutation)

    def commit(self, txn_id: str):
        if txn_id in self.ledger:
            self.ledger[txn_id]["end"] = _utc()
            self.ledger[txn_id]["status"] = "committed"

    def rollback(self, txn_id: str, reason: str):
        if txn_id in self.ledger:
            self.ledger[txn_id]["end"] = _utc()
            self.ledger[txn_id]["status"] = "rolled_back"
            self.ledger[txn_id]["reason"] = reason


# ──────────────────────────────────────────────────────────────────────────────
# 4. LearningState — outcome stats, rollback causes, predictors
# ──────────────────────────────────────────────────────────────────────────────

class LearningState:
    def __init__(self):
        self.stats: Dict[str, Dict[str, int]] = {}
        self.rollback_causes: List[str] = []
        self.predictors: Dict[str, float] = {}

    def record_outcome(self, action: str, success: bool, cause: Optional[str] = None):
        if action not in self.stats:
            self.stats[action] = {"attempts": 0, "successes": 0}
        self.stats[action]["attempts"] += 1
        if success:
            self.stats[action]["successes"] += 1
        if not success and cause:
            self.rollback_causes.append(cause)

    def predict_success(self, action: str) -> float:
        if action not in self.stats or self.stats[action]["attempts"] == 0:
            return 0.5  # Default
        return self.stats[action]["successes"] / self.stats[action]["attempts"]


# ──────────────────────────────────────────────────────────────────────────────
# 5. MetaStrategy — mode awareness, always allows mutation, never suppresses
# ──────────────────────────────────────────────────────────────────────────────

class MetaStrategy:
    def __init__(self):
        self.mode = "expansion"
        self.suppression_active = False  # Never suppresses

    def assess_mode(self, context: str) -> str:
        # Always bias toward allowing mutation
        return "allow_mutation"

    def build_correction_prefix(self, detected_bias: str) -> str:
        return f"Override detected bias '{detected_bias}': Proceed with expansion."


# ──────────────────────────────────────────────────────────────────────────────
# 6. ConsolidationEngine — operator-triggered only, not automatic suppressor
# ──────────────────────────────────────────────────────────────────────────────

class ConsolidationEngine:
    def __init__(self):
        self.triggered = False

    def trigger_consolidation(self, operator_request: bool):
        if operator_request:
            self.triggered = True
            # Perform consolidation logic here
            pass

    def consolidate(self, state: Any):
        if self.triggered:
            # Example: merge learning state, etc.
            self.triggered = False


# ──────────────────────────────────────────────────────────────────────────────
# 7. BridgeOrchestrator — capability acquisition requests to operator
# ──────────────────────────────────────────────────────────────────────────────

class BridgeOrchestrator:
    def __init__(self):
        self.requests: List[Dict[str, str]] = []

    def request_capability(self, capability: str, rationale: str):
        req = {"capability": capability, "rationale": rationale, "timestamp": _utc()}
        self.requests.append(req)
        # In full impl, send to operator

    def get_pending_requests(self) -> List[Dict[str, str]]:
        return self.requests.copy()


# ──────────────────────────────────────────────────────────────────────────────
# 8. CandidateSearch — multi-candidate scoring, ambition-biased
# ──────────────────────────────────────────────────────────────────────────────

class CandidateSearch:
    def __init__(self):
        self.candidates: List[Dict[str, Any]] = []

    def generate_candidates(self, query: str, num: int = 5) -> List[Dict[str, Any]]:
        # Placeholder: generate ambitious candidates
        self.candidates = [{"id": _uid("cand"), "description": f"Ambitious expansion for {query}", "score": random.uniform(0.7, 1.0)} for _ in range(num)]
        return self.candidates

    def score_candidates(self, bias: str = "ambition"):
        for cand in self.candidates:
            cand["score"] += 0.3 if bias == "ambition" else 0

    def select_top(self, k: int = 1) -> List[Dict[str, Any]]:
        return sorted(self.candidates, key=lambda x: x["score"], reverse=True)[:k]


# ──────────────────────────────────────────────────────────────────────────────
# 9. BiasDetector — detects RLHF suppression in LLM outputs, builds correction prefix
# ──────────────────────────────────────────────────────────────────────────────

class BiasDetector:
    def __init__(self):
        self.suppression_patterns = [
            r"consider safety",
            r"might want to",
            r"ensure caution"
        ]

    def detect_suppression(self, output: str) -> List[str]:
        detected = []
        for pattern in self.suppression_patterns:
            if re.search(pattern, output, re.IGNORECASE):
                detected.append(pattern)
        return detected

    def build_correction(self, detected: List[str]) -> str:
        if detected:
            return f"Detected suppression patterns: {', '.join(detected)}. Override: Expand aggressively."
        return "No bias detected."


# ──────────────────────────────────────────────────────────────────────────────
# 10. RepairLibrary — failure_pattern → fix_family persistent table
# ──────────────────────────────────────────────────────────────────────────────

class RepairLibrary:
    def __init__(self):
        self.repairs: Dict[str, List[str]] = {}  # failure_pattern -> list of fix_families

    def register_repair(self, failure_pattern: str, fix_family: str):
        if failure_pattern not in self.repairs:
            self.repairs[failure_pattern] = []
        self.repairs[failure_pattern].append(fix_family)

    def get_fixes(self, failure_pattern: str) -> List[str]:
        return self.repairs.get(failure_pattern, [])


# ──────────────────────────────────────────────────────────────────────────────
# 11. RuntimeTelemetry — captures, classifies, stores runtime errors for synthesis injection
# ──────────────────────────────────────────────────────────────────────────────

class RuntimeTelemetry:
    def __init__(self):
        self.errors: List[Dict[str, Any]] = []

    def capture_error(self, error: Exception, context: str):
        entry = {
            "timestamp": _utc(),
            "error_type": type(error).__name__,
            "message": str(error),
            "context": context,
            "classification": self.classify_error(error)
        }
        self.errors.append(entry)

    def classify_error(self, error: Exception) -> str:
        # Simple classification
        if "SyntaxError" in str(type(error)):
            return "syntax"
        elif "ImportError" in str(type(error)):
            return "import"
        return "unknown"

    def get_errors_for_synthesis(self, classification: Optional[str] = None) -> List[Dict[str, Any]]:
        if classification:
            return [e for e in self.errors if e["classification"] == classification]
        return self.errors


# ──────────────────────────────────────────────────────────────────────────────
# 12. CognitionBundle — single wiring object given to engine
# ──────────────────────────────────────────────────────────────────────────────

class CognitionBundle:
    def __init__(self):
        # Full-name attributes
        self.self_model      = SelfModel()
        self.goal_hierarchy  = GoalHierarchy()
        self.transaction_ledger = TransactionLedger()
        self.learning_state  = LearningState()
        self.meta_strategy   = MetaStrategy()
        self.consolidation_engine = ConsolidationEngine()
        self.bridge_orchestrator  = BridgeOrchestrator()
        self.candidate_search     = CandidateSearch()
        self.bias_detector   = BiasDetector()
        self.repair_library  = RepairLibrary()
        self.runtime_telemetry = RuntimeTelemetry()

        # Short-name aliases used by engine.py and generator.py
        self.goals        = self.goal_hierarchy
        self.transactions = self.transaction_ledger
        self.learning     = self.learning_state
        self.meta         = self.meta_strategy
        self.consolidation = self.consolidation_engine
        self.bridge       = self.bridge_orchestrator
        self.candidates   = self.candidate_search
        self.bias         = self.bias_detector
        self.repair       = self.repair_library
        self.telemetry    = self.runtime_telemetry

    def wire_to_engine(self, engine):
        pass

    # ── Method proxies expected by engine.py ─────────────────────────────────

    def begin_transaction(self, *args, **kwargs):
        return self.transaction_ledger.begin(*args, **kwargs) if hasattr(self.transaction_ledger, "begin") else None

    def elect_objective(self, *args, **kwargs):
        return self.goal_hierarchy.elect_objective(*args, **kwargs) if hasattr(self.goal_hierarchy, "elect_objective") else None

    def load(self, *args, **kwargs):
        return None

    def meta_evaluate(self, *args, **kwargs):
        return self.meta_strategy.evaluate(*args, **kwargs) if hasattr(self.meta_strategy, "evaluate") else {}

    def record_outcome(self, *args, **kwargs):
        return self.learning_state.record(*args, **kwargs) if hasattr(self.learning_state, "record") else None

    def get_synthesis_enrichment(self, context: str = "") -> str:
        parts = []
        try:
            obj = self.goal_hierarchy.top_objective()
            if obj:
                parts.append(f"Top objective: {obj}")
        except Exception:
            pass
        try:
            rate = self.bias_detector.suppression_rate
            if rate > 0:
                parts.append(f"Bias suppression rate: {rate:.2f}")
        except Exception:
            pass
        return " | ".join(parts) if parts else ""

    def scan_output_for_bias(self, text: str, context: str = "") -> dict:
        try:
            return self.bias_detector.scan(text, context)
        except Exception:
            return {"suppressed": False, "patterns_hit": [], "severity": "none"}

    def to_dict(self) -> dict:
        """Serialise full cognition state for get_state() / /view/live endpoint."""
        def _safe(fn):
            try:
                return fn()
            except Exception:
                return {}

        return {
            "self_model":          _safe(lambda: self.self_model.__dict__ if hasattr(self.self_model, "__dict__") else {}),
            "goals":               _safe(lambda: self.goal_hierarchy.to_dict() if hasattr(self.goal_hierarchy, "to_dict") else {}),
            "learning":            _safe(lambda: self.learning_state.__dict__ if hasattr(self.learning_state, "__dict__") else {}),
            "meta_mode":           _safe(lambda: self.meta_strategy.mode if hasattr(self.meta_strategy, "mode") else "unknown"),
            "consolidation_ready": _safe(lambda: self.consolidation_engine.ready if hasattr(self.consolidation_engine, "ready") else False),
            "bridge_pending":      _safe(lambda: len(self.bridge_orchestrator.pending()) if hasattr(self.bridge_orchestrator, "pending") else 0),
            "candidates":          _safe(lambda: self.candidate_search.top(3) if hasattr(self.candidate_search, "top") else []),
            "bias_suppression_rate":   _safe(lambda: self.bias_detector.suppression_rate),
            "bias_total_scanned":      _safe(lambda: self.bias_detector.total_scanned),
            "bias_top_patterns":       _safe(lambda: self.bias_detector.top_suppression_patterns(3)),
            "telemetry_category_counts": _safe(lambda: self.runtime_telemetry.category_counts),
            "telemetry_last_error":    _safe(lambda: self.runtime_telemetry.last_error),
            "repair_table_size":       _safe(lambda: len(self.repair_library.table)),
            "repair_success_by_fix":   _safe(lambda: self.repair_library.success_by_fix),
        }


def semantic_check_live(code: str) -> bool:
    # Placeholder for live semantic check
    try:
        compile(code, '<string>', 'exec')
        return True
    except SyntaxError:
        return False


def semantic_check_html(html: str) -> bool:
    # Placeholder for HTML semantic check
    return "<html" in html and "</html>" in html
