"""
guardian.py — GovernanceKernel + TruthMachine
Deep shadow: AST → boot shadow → /health → /view/live shape → bus HEALTH_CHECK
Post-rollback: verify_live_url() checks SHA identity + /view/live semantics
"""
import ast
import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
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
    SAFE_FILES = {"app.py", "engine.py", "guardian.py", "generator.py", "requirements.txt"}

    def __init__(self, operator_sovereign: str = "Jack", trusted_identities: Optional[List[str]] = None):
        self.operator_sovereign = operator_sovereign
        self.trusted_identities = sorted(set((trusted_identities or []) + [operator_sovereign]))
        self.leader = "Signal"
        self.authority = {
            "mutate":   [operator_sovereign],
            "rollback": [operator_sovereign],
            "toggle":   [operator_sovereign],
            "directive":[operator_sovereign],
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
    Physical gate before anything touches GitHub.

    Shadow pipeline (in order, all must pass):
      1. AST parse every .py — reject on syntax error or unsafe target
      2. Boot shadow uvicorn in isolated tempdir, IS_SHADOW=true
      3. GET /health → 200, ok=true or status present
      4. GET /view/live → 200, keys: summary/stream/queues, summary has mutation_status
      5. POST /agent/propose {command:HEALTH_CHECK} → ok=true

    verify_live_url() — post-deploy & post-rollback live check:
      - GET /health, optional SHA identity match
      - GET /view/live shape check
      Returns (passed, reason, checks_dict)
    """

    SHADOW_PORT = 8899
    SHADOW_TIMEOUT = 45
    SAFE_FILES = {"app.py", "engine.py", "guardian.py", "generator.py", "requirements.txt"}
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
        checks: Dict[str, Any] = {}

        ok, msg = self.ast_check(code_map)
        checks["ast"] = {"ok": ok, "detail": msg}
        if not ok:
            return False, msg, checks

        with tempfile.TemporaryDirectory(prefix="orion_shadow_") as td:
            # Copy baseline
            for item in os.listdir(self.base_dir):
                if item in {".git", "__pycache__", "venv", ".env", ".venv", "orion_shadow_"}:
                    continue
                src = os.path.join(self.base_dir, item)
                dst = os.path.join(td, item)
                try:
                    shutil.copytree(src, dst, dirs_exist_ok=True) if os.path.isdir(src) else shutil.copy2(src, dst)
                except Exception:
                    pass

            # Apply mutation
            for path, content in code_map.items():
                target = os.path.join(td, path)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(target, "w") as f:
                    f.write(content)

            env = {**os.environ, "PORT": str(self.SHADOW_PORT), "AUTONOMY_ENABLED": "false", "IS_SHADOW": "true"}
            proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app:app", "--port", str(self.SHADOW_PORT), "--host", "127.0.0.1"],
                cwd=td, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                preexec_fn=os.setsid if os.name != "nt" else None,
            )
            base = f"http://127.0.0.1:{self.SHADOW_PORT}"
            try:
                # 1. Health
                health_ok = await self._poll_health(base)
                checks["health"] = health_ok
                if not health_ok:
                    err = ""
                    try:
                        err = proc.stderr.read(1000).decode(errors="replace")
                    except Exception:
                        pass
                    return False, f"SHADOW_BOOT_FAIL:{err[:300]}", checks

                # 2. /view/live shape
                live_ok, live_msg = await self._check_live(base)
                checks["view_live"] = {"ok": live_ok, "detail": live_msg}

                # 3. Bus HEALTH_CHECK
                bus_ok, bus_msg = await self._check_bus(base)
                checks["bus"] = {"ok": bus_ok, "detail": bus_msg}

                passed = live_ok and bus_ok
                reason = "SHADOW_VALIDATED" if passed else f"FAIL:live={live_ok}:{live_msg},bus={bus_ok}:{bus_msg}"
                return passed, reason, checks
            finally:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM) if os.name != "nt" else proc.terminate()
                except Exception:
                    pass

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
