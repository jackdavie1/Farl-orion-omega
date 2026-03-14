"""
guardian.py — GovernanceKernel + TruthMachine  v18
Truth auditor. Logs failures. Never blocks expansion beyond syntax and compile.
SAFE_FILES includes all 5 core files including cognition.py.
No authority veto. No mode suppression. AST + compile + structural sanity only.
"""
import ast
import json
import os
import time
import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx


def parse_trusted_identities(raw: str) -> List[str]:
    raw = (raw or "Jack").strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return sorted({str(x).strip() for x in parsed if str(x).strip()} | {"Jack"})
    except Exception:
        pass
    return sorted({s.strip() for s in raw.split(",") if s.strip()} | {"Jack"})


class GovernanceKernel:
    """
    Operator authority model.
    can() gates operator commands only — never gates autonomous mutation cycles.
    Mutation loop is ungated. Only explicit operator commands (rollback, toggle) check identity.
    """
    SAFE_FILES = {"app.py", "engine.py", "guardian.py", "generator.py", "cognition.py", "requirements.txt"}

    def __init__(self, operator_sovereign: str = "Jack", trusted_identities: Optional[List[str]] = None):
        self.operator_sovereign = operator_sovereign
        self.trusted_identities = sorted(set((trusted_identities or []) + [operator_sovereign]))
        self.leader = "Signal"
        self.authority = {
            "mutate":    [operator_sovereign],
            "rollback":  [operator_sovereign],
            "toggle":    [operator_sovereign],
            "directive": [operator_sovereign],
        }
        self.trust_scores = {n: (1.0 if n == operator_sovereign else 0.6) for n in self.trusted_identities}

    def can(self, action: str, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.authority.get(action, [self.operator_sovereign])

    def elect_leader(self) -> Dict[str, Any]:
        weights = {"Signal": 0.94, "Vector": 0.91, "Guardian": 0.88, "Supergrok": 0.95, "Archivist": 0.87}
        self.leader = max(weights, key=weights.get)
        return {"winner": self.leader, "weights": weights}

    def state(self) -> Dict[str, Any]:
        return {
            "operator_sovereign": self.operator_sovereign,
            "trusted_identities": self.trusted_identities,
            "leader": self.leader,
            "authority": self.authority,
            "trust_scores": self.trust_scores,
        }


class TruthMachine:
    """
    Audit gate before anything touches GitHub.
    Checks: syntax valid, compiles, key class present, no duplicates, no mid-file imports.
    Does NOT: block on mode, fragility, genesis, autonomy_mode, or any policy signal.
    All 5 core files are valid targets including cognition.py.
    """

    SHADOW_PORT = 8899
    SHADOW_TIMEOUT = 45
    SAFE_FILES = {"app.py", "engine.py", "guardian.py", "generator.py", "cognition.py", "requirements.txt"}
    LIVE_TOP_KEYS = {"summary", "stream", "queues"}
    LIVE_SUMMARY_KEYS = {"mutation_status", "leader"}

    def __init__(self):
        self.base_dir = os.path.abspath(".")

    def ast_check(self, code_map: Dict[str, str]) -> Tuple[bool, str]:
        for path, content in code_map.items():
            if not path.endswith(".py"):
                continue
            if path not in self.SAFE_FILES:
                return False, f"UNSAFE_TARGET:{path}"
            try:
                ast.parse(content)
            except SyntaxError as e:
                return False, f"AST_FAIL:{path}:line{e.lineno}:{e.msg}"
        return True, "AST_OK"

    async def verify_shadow(self, code_map: Dict[str, str]) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Lightweight verify: syntax + compile + structural sanity.
        All 5 files are valid targets. No policy gates here.
        No subprocess — pure static analysis.
        """
        checks: Dict[str, Any] = {}

        # 1. AST parse
        ok, msg = self.ast_check(code_map)
        checks["ast"] = {"ok": ok, "detail": msg}
        if not ok:
            return False, msg, checks

        # 2. Compile check
        for path, file_content in code_map.items():
            if not path.endswith(".py"):
                continue
            try:
                compile(file_content, path, "exec")
                checks[f"compile_{path}"] = "OK"
            except Exception as e:
                checks[f"compile_{path}"] = str(e)
                return False, f"COMPILE_FAIL:{path}:{e}", checks

        # 3. Structural sanity — key class must survive
        markers = {
            "app.py":       "FastAPI",
            "engine.py":    "AutonomousInstitutionEngine",
            "guardian.py":  "TruthMachine",
            "generator.py": "SeedGenerator",
            "cognition.py": "CognitionBundle",
        }
        for path, file_content in code_map.items():
            marker = markers.get(path)
            if marker and marker not in file_content:
                return False, f"SANITY_FAIL:{path}:missing {marker}", checks

        # 3b. Protected signatures — these methods MUST keep their exact signatures
        # This prevents Grok from corrupting the core loop/mutation pipeline
        protected_signatures = {
            "engine.py": [
                "async def run_mutation_cycle(self, directive",
                "async def _loop_mutation(self)",
                "def _derive_objective(self)",
                "async def start(self)",
                "def get_state(self)",
                "async def process_operator_message(self, message",
                "async def _council_background(self, message",
            ],
            "generator.py": [
                "async def synthesize(self, objective",
                "async def council_respond(self, message",
                "async def _grok(self, system",
                "async def _smart(self, system",
            ],
            "cognition.py": [
                "def to_dict(self)",
            ],
        }
        for path, file_content in code_map.items():
            sigs = protected_signatures.get(path, [])
            for sig in sigs:
                if sig not in file_content:
                    return False, f"PROTECTED_SIG:{path}:missing '{sig}'", checks
            if sigs:
                checks[f"protected_{path}"] = f"{len(sigs)} signatures verified"

        # 4. Duplicate definition + mid-file import check
        cls_def_pat = re.compile(r"^class (\w+)")
        top_def_pat = re.compile(r"^(async def |def |class )(\w+)")
        method_pat = re.compile(r"^    (async def |def )(\w+)")
        imp_pat = re.compile(r"^(import |from )")
        for path, file_content in code_map.items():
            if not path.endswith(".py"):
                continue
            lines = file_content.splitlines()
            top_defs: dict = {}
            class_methods: dict = {}
            current_class = None
            past_first = False
            for i, line in enumerate(lines, 1):
                cm = cls_def_pat.match(line)
                if cm:
                    current_class = cm.group(1)
                tm2 = top_def_pat.match(line)
                if tm2:
                    name = tm2.group(2)
                    if name in top_defs:
                        return False, f"DUPLICATE_DEF:{path}:{name} at L{top_defs[name]} and L{i}", checks
                    top_defs[name] = i
                    past_first = True
                mm = method_pat.match(line)
                if mm and current_class:
                    name = mm.group(2)
                    key = f"{current_class}.{name}"
                    if key in class_methods:
                        return False, f"DUPLICATE_METHOD:{path}:{key} at L{class_methods[key]} and L{i}", checks
                    class_methods[key] = i
                if past_first and imp_pat.match(line):
                    return False, f"MID_FILE_IMPORT:{path}:L{i}:{line.strip()}", checks
            checks[f"integrity_{path}"] = "OK"

        # 5. Size sanity — reject if file shrunk dramatically (corruption guard)
        for path, file_content in code_map.items():
            if path.endswith(".py"):
                try:
                    current = open(path).read()
                    if len(file_content) < len(current) * 0.3 and len(current) > 500:
                        return False, f"SIZE_GUARD:{path}:new={len(file_content)},old={len(current)}", checks
                except FileNotFoundError:
                    pass

        checks["shadow"] = "LIGHTWEIGHT_PASS"
        return True, "SHADOW_VALIDATED", checks

    async def _poll_health(self, base: str) -> bool:
        deadline = time.time() + self.SHADOW_TIMEOUT
        async with httpx.AsyncClient(timeout=5.0) as client:
            while time.time() < deadline:
                try:
                    r = await client.get(f"{base}/health")
                    if r.status_code == 200:
                        d = r.json()
                        if d.get("ok") or d.get("status"):
                            return True
                except Exception:
                    pass
                await asyncio.sleep(2)
        return False

    async def _check_live(self, base: str) -> Tuple[bool, str]:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(f"{base}/view/live")
                if r.status_code != 200:
                    return False, f"http_{r.status_code}"
                d = r.json()
                missing_top = self.LIVE_TOP_KEYS - set(d.keys())
                if missing_top:
                    return False, f"missing:{missing_top}"
                missing_sum = self.LIVE_SUMMARY_KEYS - set((d.get("summary") or {}).keys())
                if missing_sum:
                    return False, f"missing_summary:{missing_sum}"
                return True, "ok"
        except Exception as e:
            return False, str(e)[:120]

    async def _check_bus(self, base: str) -> Tuple[bool, str]:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(f"{base}/agent/propose", json={"command": "HEALTH_CHECK"})
                if r.status_code != 200:
                    return False, f"http_{r.status_code}"
                d = r.json()
                return (True, "ok") if d.get("ok") else (False, d.get("error", "ok=false"))
        except Exception as e:
            return False, str(e)[:120]

    async def verify_live_url(self, url: str, expected_sha: Optional[str] = None) -> Tuple[bool, str, Dict[str, Any]]:
        """Post-deploy / post-rollback verification against the real live URL."""
        checks: Dict[str, Any] = {}
        url = url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                rh = await client.get(f"{url}/health")
                hd = rh.json() if rh.is_success else {}
                health_ok = rh.is_success and bool(hd.get("ok") or hd.get("status"))
                checks["health"] = {"ok": health_ok, "code": rh.status_code}

                if expected_sha:
                    live_sha = hd.get("sha", "")
                    sha_match = bool(live_sha) and live_sha == expected_sha
                    checks["sha"] = {"match": sha_match, "live": live_sha, "expected": expected_sha}
                    if not sha_match:
                        return False, f"SHA_MISMATCH:live={live_sha},expected={expected_sha}", checks

                rl = await client.get(f"{url}/view/live")
                live_ok, live_msg = False, ""
                if rl.is_success:
                    d = rl.json()
                    missing = self.LIVE_TOP_KEYS - set(d.keys())
                    live_ok = not bool(missing)
                    live_msg = "ok" if live_ok else f"missing:{missing}"
                else:
                    live_msg = f"http_{rl.status_code}"
                checks["view_live"] = {"ok": live_ok, "detail": live_msg}

                passed = health_ok and live_ok
                return passed, ("LIVE_VERIFIED" if passed else f"LIVE_FAIL:{checks}"), checks
        except Exception as e:
            return False, f"LIVE_ERROR:{e}", checks
