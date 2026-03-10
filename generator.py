"""
generator.py — SeedGenerator
Multi-agent mutation synthesis: Architect(Grok) → Coder(Claude) → Critic(Grok) → Refiner(Claude)
council_respond() — live persona-driven replies to operator messages
generate_all()    — background triangulation probes for council debate loop
agent_generate_directive() — agents propose their own autonomous objectives
"""
import asyncio
import json
import os
import re
from typing import Any, Dict, List, Optional

import httpx

COUNCIL_PERSONAS = """
FARL — Frontier Anomaly Research Lab. Autonomous recursive institution.
You are one of: Signal (strategic coordinator), Vector (analytical engine),
Guardian (governance, rollback discipline), Supergrok (adversarial auditor),
TokenMaster (spend efficiency), JackAgent (operator intent mirror),
BuilderAgent (code writer), DeployAgent (deployment gatekeeper).
Jack is sovereign operator. He can override everything from /view.
The organism mutates its own code via: synthesize → shadow verify → deploy → probation → commit/rollback.
Mutation targets only: app.py, engine.py, guardian.py, generator.py.
"""


def _extract_json(text: str) -> Optional[Any]:
    text = text.strip()
    text = re.sub(r"^```[a-z]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text).strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    # Find outermost JSON object or array
    for opener, closer in [('{', '}'), ('[', ']')]:
        depth, start = 0, None
        for i, ch in enumerate(text):
            if ch == opener:
                if depth == 0:
                    start = i
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0 and start is not None:
                    try:
                        return json.loads(text[start:i + 1])
                    except Exception:
                        pass
    return None


class SeedGenerator:
    SAFE_FILES = {"app.py", "engine.py", "guardian.py", "generator.py"}
    # Pre-genesis: restrict to small files only. engine.py is 958 lines —
    # too large for reliable full-file synthesis until genesis is proven.
    PRE_GENESIS_SAFE = {"app.py", "guardian.py", "generator.py"}

    def __init__(self):
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.xai_key = os.getenv("XAI_API_KEY", "")
        self.anthropic_model = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
        self.xai_model = os.getenv("XAI_MODEL") or os.getenv("GROK_MODEL") or "grok-3-mini"

    # ── Raw API callers ──────────────────────────────────────────────────────

    async def _claude(self, system: str, user: str, max_tokens: int = 4000) -> str:
        if not self.anthropic_key:
            return json.dumps({"error": "ANTHROPIC_NOT_CONFIGURED"})
        headers = {
            "x-api-key": self.anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        body = {
            "model": self.anthropic_model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        }
        async with httpx.AsyncClient(timeout=120.0) as c:
            try:
                r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json=body)
                d = r.json()
                if not r.is_success:
                    return json.dumps({"error": d})
                return "".join(b.get("text", "") for b in d.get("content", []) if isinstance(b, dict))
            except Exception as e:
                return json.dumps({"error": str(e)})

    async def _smart(self, system: str, user: str, max_tokens: int = 2000) -> str:
        """Use Claude if key present and healthy, fall back to Grok on any error."""
        if self.anthropic_key:
            result = await self._claude(system, user, max_tokens)
            try:
                parsed = json.loads(result)
                if isinstance(parsed, dict) and "error" in parsed:
                    return await self._grok(system, user, max_tokens)
            except Exception:
                pass
            return result
        return await self._grok(system, user, max_tokens)

    async def _grok(self, system: str, user: str, max_tokens: int = 1500) -> str:
        if not self.xai_key:
            return json.dumps({"error": "XAI_NOT_CONFIGURED"})
        headers = {"Authorization": f"Bearer {self.xai_key}", "Content-Type": "application/json"}
        body = {
            "model": self.xai_model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }
        async with httpx.AsyncClient(timeout=60.0) as c:
            try:
                r = await c.post("https://api.x.ai/v1/chat/completions", headers=headers, json=body)
                d = r.json()
                if not r.is_success:
                    return json.dumps({"error": d})
                choices = d.get("choices", [])
                return choices[0].get("message", {}).get("content", "") if choices else ""
            except Exception as e:
                return json.dumps({"error": str(e)})

    # ── Background triangulation ─────────────────────────────────────────────

    async def generate_all(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Council debate probes — called every reflex cycle."""
        context = context or {}
        prompt = (
            f"{COUNCIL_PERSONAS}\n\n"
            "You are an external cognition thread. "
            "Return JSON: {stance, risk, next_move, dissent}. "
            f"Mode={context.get('mode','supervised')}. "
            f"MutationStatus={context.get('mutation_status','IDLE')}. "
            f"FreeAgency={context.get('free_agency_enabled', False)}. "
            f"OpenThreads={json.dumps(context.get('open_threads',[]))[:400]}."
        )
        results = await asyncio.gather(
            self._probe("Grok-Ensemble", lambda: self._grok("FARL council cognition thread.", prompt, 480)),
            self._probe("Claude-Ensemble", lambda: self._claude("FARL council cognition thread.", prompt, 480)),
            return_exceptions=True,
        )
        out = []
        for r in results:
            out.append(r if not isinstance(r, Exception) else {"source": "error", "data": {"error": str(r)}})
        return out

    async def _probe(self, source: str, caller) -> Dict[str, Any]:
        text = await caller()
        parsed = _extract_json(text)
        return {"source": source, "data": parsed or {"text": text[:1500]}}

    # ── Live council chat ────────────────────────────────────────────────────

    async def council_respond(self, message: str, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generates real agent-persona replies to Jack's message.
        Returns list of {agent, message} dicts for rendering in the feed.
        """
        system = (
            f"{COUNCIL_PERSONAS}\n\n"
            "Jack has sent a message. Reply AS MULTIPLE COUNCIL AGENTS in character. "
            "Return JSON array: [{\"agent\": \"AgentName\", \"message\": \"their reply\"}]. "
            "3-5 agents. Each speaks from their role. Be direct, concrete, no filler. "
            "Reference actual system state. If a directive was given, plan it specifically. "
            "If a question was asked, answer it with real knowledge."
        )
        state_str = json.dumps({
            "mutation_status": state.get("mutation_status"),
            "genesis_triggered": state.get("genesis_triggered"),
            "fragility": state.get("fragility"),
            "failure_streak": state.get("failure_streak"),
            "autonomy_mode": state.get("autonomy_mode"),
            "free_agency_enabled": state.get("free_agency_enabled"),
            "last_mutation_objective": state.get("last_mutation_objective"),
            "deployer_ready": state.get("deployer_ready"),
            "open_threads": state.get("open_threads", [])[:5],
        }, indent=2)
        user = f"Jack says: {message}\n\nSystem state:\n{state_str}"
        text = await self._smart(system, user, max_tokens=2000)
        parsed = _extract_json(text)
        if isinstance(parsed, list) and all(isinstance(x, dict) for x in parsed):
            return parsed
        if isinstance(parsed, dict):
            if "responses" in parsed:
                return parsed["responses"]
            # single agent response
            if "agent" in parsed and "message" in parsed:
                return [parsed]
        # Fallback
        return [{"agent": "Signal", "message": text[:1200] if text else "Council received your message."}]

    # ── Agent self-directive generation ─────────────────────────────────────

    async def agent_generate_directive(self, agent_name: str, state: Dict[str, Any]) -> str:
        """
        When free agency is enabled, each agent autonomously proposes its own objective.
        Returns a directive string ready for run_mutation_cycle().
        """
        system = (
            f"{COUNCIL_PERSONAS}\n\n"
            f"You ARE {agent_name}. Jack has given you full autonomy. "
            "Propose ONE specific, actionable mutation objective for the organism. "
            "It must improve the system in a way only YOU would prioritise given your role. "
            "Return plain text — one sentence, no JSON. Be specific about what code changes."
        )
        state_str = json.dumps({
            "mutation_status": state.get("mutation_status"),
            "genesis_triggered": state.get("genesis_triggered"),
            "fragility": state.get("fragility"),
            "open_threads": [t.get("objective", "") for t in state.get("redesign_threads", [])[:4]],
        })
        user = f"System state: {state_str}\n\nPropose your autonomous directive:"
        text = await self._grok(system, user, max_tokens=200)
        return text.strip() if text and len(text.strip()) > 10 else f"Improve {agent_name} operational efficiency in the organism"

    # ── Autonomous mutation synthesis ────────────────────────────────────────

    async def _read_current_file(self, filename: str) -> str:
        """Read current file content from GitHub for surgical patching."""
        import os
        token = os.getenv("GITHUB_TOKEN", "")
        repo = os.getenv("REPO_NAME", "")
        if not token or not repo:
            return ""
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3.raw"}
        url = f"https://raw.githubusercontent.com/{repo}/main/{filename}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as c:
                r = await c.get(url, headers=headers)
                return r.text if r.is_success else ""
        except Exception:
            return ""

    async def synthesize(self, objective: str, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Surgical patch architecture:
        Architect(Grok) identifies WHICH functions to change
        Coder(Claude) writes ONLY those functions as patches
        Assembler splices patches into current file content
        Critic(Grok) reviews the splice result
        Refiner(Claude) fixes any issues
        Returns {"code_map": {file: full_patched_content}, "rationale": str}
        """
        genesis_triggered = state.get("genesis_triggered", False)
        allowed = self.SAFE_FILES if genesis_triggered else self.PRE_GENESIS_SAFE

        ss = json.dumps({
            "fragility": state.get("fragility", 0.0),
            "failure_streak": state.get("failure_streak", 0),
            "mutation_status": state.get("mutation_status", "IDLE"),
            "genesis_triggered": genesis_triggered,
            "free_agency_enabled": state.get("free_agency_enabled", False),
            "open_threads": [t.get("objective", "") for t in state.get("redesign_threads", [])[:4]],
        }, indent=2)

        # ── Architect: identify target file + functions only ───────────────
        plan = await self._grok(
            f"{COUNCIL_PERSONAS}\nYou are BuilderAgent as Architect. "
            "Identify EXACTLY: (1) which file to patch, (2) which function names to add or modify, (3) what each function should do differently. "
            "Be surgical. One file. 1-3 functions maximum. No code yet. Plain text.",
            f"Objective: {objective}\nState:\n{ss}\n"
            f"Allowed files: {sorted(allowed)}\n"
            "Name the file and the exact function(s) to change. Nothing else.",
            max_tokens=600
        )

        # ── Identify target file from plan ─────────────────────────────────
        target_file = None
        for f in sorted(allowed, key=len):
            if f in plan:
                target_file = f
                break
        if not target_file:
            target_file = sorted(allowed)[0]

        # ── Read current file from GitHub ──────────────────────────────────
        current_content = await self._read_current_file(target_file)
        if not current_content:
            current_content = f"# {target_file} — could not read current content"

        # Truncate for context — send first 3000 chars so model knows structure
        content_preview = current_content[:3000]

        # ── Coder: write ONLY the changed functions ────────────────────────
        coder_sys = (
            f"{COUNCIL_PERSONAS}\nYou are BuilderAgent as Coder. "
            "Write ONLY the specific Python functions that need to change. "
            "STRICT JSON only. No markdown.\n"
            'Schema: {"patches": [{"function": "function_name", "code": "def function_name(...):\n    ...complete implementation"}], "rationale": "one sentence"}\n'
            "Rules:\n"
            "- Each patch is ONE complete function definition only\n"
            "- No surrounding class or file content\n"
            "- Functions must be complete — no ..., no TODO\n"
            "- Only stdlib + fastapi + uvicorn + httpx + requests + pydantic"
        )
        coder_out = await self._smart(coder_sys,
            f"Plan:\n{plan}\n\nCurrent {target_file} structure (first 3000 chars):\n{content_preview}\n\nWrite patch JSON now.",
            max_tokens=3000
        )
        patch_data = _extract_json(coder_out)
        if not patch_data or "patches" not in patch_data:
            # Fallback: try full file if patches failed
            return await self._synthesize_full_file(objective, state, plan, ss, allowed)

        patches = patch_data.get("patches", [])
        if not patches or not all(isinstance(p, dict) and "function" in p and "code" in p for p in patches):
            return await self._synthesize_full_file(objective, state, plan, ss, allowed)

        # ── Assembler: splice patches into current file ────────────────────
        patched_content = current_content
        applied = []
        for patch in patches:
            fname = patch["function"]
            new_code = patch["code"].strip()
            # Find and replace existing function in file
            import re
            # Match def or async def
            pattern = rf"(async def |def ){re.escape(fname)}\s*\([^)]*\)[^:]*:"
            match = re.search(pattern, patched_content)
            if match:
                # Find the full function body by indentation
                start = match.start()
                lines = patched_content[start:].split("\n")
                end_line = 1
                base_indent = len(lines[0]) - len(lines[0].lstrip())
                for i, line in enumerate(lines[1:], 1):
                    if line.strip() == "":
                        continue
                    indent = len(line) - len(line.lstrip())
                    if indent <= base_indent and line.strip():
                        end_line = i
                        break
                else:
                    end_line = len(lines)
                old_func = "\n".join(lines[:end_line])
                patched_content = patched_content[:start] + new_code + "\n" + patched_content[start + len(old_func):]
                applied.append(fname)
            else:
                # Function doesn't exist — append to end of file
                patched_content = patched_content.rstrip() + "\n\n" + new_code + "\n"
                applied.append(f"{fname}(new)")

        if not applied:
            return {"error": "Assembler could not splice any patches into file"}

        code_map = {target_file: patched_content}

        # ── Critic: review the splice ──────────────────────────────────────
        critique = await self._grok(
            f"{COUNCIL_PERSONAS}\nYou are Supergrok as Critic. "
            "Review these function patches for syntax errors, missing imports, broken logic. "
            "Each flaw on its own line. If clean: respond exactly APPROVED",
            f"Objective: {objective}\nPatched functions: {applied}\n"
            f"Patch code:\n{json.dumps([p['code'] for p in patches])[:2000]}\n\nAPPROVE or list flaws:",
            max_tokens=600
        )

        if "APPROVED" in critique.upper():
            return {"code_map": code_map, "rationale": patch_data.get("rationale", "Patches applied"), "applied": applied}

        # ── Refiner: fix the patches ───────────────────────────────────────
        refined_out = await self._smart(
            f"{COUNCIL_PERSONAS}\nYou are BuilderAgent as Refiner. "
            "Fix the flawed patches. STRICT JSON only.\n"
            'Schema: {"patches": [{"function": "name", "code": "complete fixed function"}], "rationale": "one sentence"}\n'
            "Fix only what Critic flagged. Complete functions.",
            f"Critique:\n{critique}\n\nOriginal patches:\n{json.dumps(patches)[:2000]}\n\nFix now:",
            max_tokens=3000
        )
        refined = _extract_json(refined_out)
        if refined and "patches" in refined:
            rpatches = refined["patches"]
            if isinstance(rpatches, list) and rpatches:
                # Re-apply refined patches
                patched_content2 = current_content
                rapplied = []
                import re
                for patch in rpatches:
                    fname = patch.get("function", "")
                    new_code = patch.get("code", "").strip()
                    if not fname or not new_code:
                        continue
                    pattern = rf"(async def |def ){re.escape(fname)}\s*\([^)]*\)[^:]*:"
                    match = re.search(pattern, patched_content2)
                    if match:
                        start = match.start()
                        lines = patched_content2[start:].split("\n")
                        end_line = 1
                        base_indent = len(lines[0]) - len(lines[0].lstrip())
                        for i, line in enumerate(lines[1:], 1):
                            if line.strip() == "":
                                continue
                            indent = len(line) - len(line.lstrip())
                            if indent <= base_indent and line.strip():
                                end_line = i
                                break
                        else:
                            end_line = len(lines)
                        old_func = "\n".join(lines[:end_line])
                        patched_content2 = patched_content2[:start] + new_code + "\n" + patched_content2[start + len(old_func):]
                        rapplied.append(fname)
                    else:
                        patched_content2 = patched_content2.rstrip() + "\n\n" + new_code + "\n"
                        rapplied.append(f"{fname}(new)")
                if rapplied:
                    return {"code_map": {target_file: patched_content2}, "rationale": refined.get("rationale", "Refined patches"), "applied": rapplied, "critique": critique[:200]}

        # Refiner failed — return assembler result with warning
        return {"code_map": code_map, "rationale": patch_data.get("rationale", ""), "applied": applied, "critique_warning": critique[:300]}

    async def _synthesize_full_file(self, objective: str, state: Dict[str, Any], plan: str, ss: str, allowed: set) -> Dict[str, Any]:
        """Fallback: full file rewrite for small files only."""
        coder_sys = (
            f"{COUNCIL_PERSONAS}\nYou are BuilderAgent as Coder. "
            "Write the complete Python file. "
            "STRICT JSON only. No markdown outside JSON.\n"
            'Schema: {"code_map": {"filename.py": "COMPLETE file content"}, "rationale": "one sentence"}\n'
            "Rules: Complete file, no stubs, only stdlib+fastapi+uvicorn+httpx+requests+pydantic."
        )
        coder_out = await self._smart(coder_sys,
            f"Plan:\n{plan}\n\nState:\n{ss}\n\nAllowed: {sorted(allowed)}\n\nWrite code_map JSON now.",
            max_tokens=4000
        )
        draft = _extract_json(coder_out)
        if not draft or "code_map" not in draft:
            return {"error": f"Coder no code_map. raw[:200]={coder_out[:200]}"}
        code_map = draft.get("code_map", {})
        if not isinstance(code_map, dict) or not code_map:
            return {"error": "Empty code_map"}
        genesis_triggered = state.get("genesis_triggered", False)
        check_allowed = self.SAFE_FILES if genesis_triggered else self.PRE_GENESIS_SAFE
        unsafe = [k for k in code_map if k not in check_allowed]
        if unsafe:
            return {"error": f"Unsafe targets: {unsafe}"}
        short = [k for k, v in code_map.items() if not isinstance(v, str) or len(v) < 80]
        if short:
            return {"error": f"Files too short: {short}"}
        critique = await self._grok(
            f"{COUNCIL_PERSONAS}\nYou are Supergrok as Critic. Find flaws or respond APPROVED",
            f"Files: {list(code_map.keys())}\nCode sample:\n{json.dumps(code_map)[:3000]}",
            max_tokens=800
        )
        if "APPROVED" in critique.upper():
            return {"code_map": code_map, "rationale": draft.get("rationale", "Approved")}
        refined_out = await self._smart(
            f"{COUNCIL_PERSONAS}\nYou are BuilderAgent as Refiner. Fix flaws. STRICT JSON only.\n"
            'Schema: {"code_map": {"filename.py": "COMPLETE corrected file"}, "rationale": "one sentence"}\n',
            f"Critique:\n{critique}\n\nOriginal:\n{json.dumps(code_map)[:3500]}\n\nFix now:",
            max_tokens=4000
        )
        refined = _extract_json(refined_out)
        if refined and "code_map" in refined and isinstance(refined["code_map"], dict):
            fm = refined["code_map"]
            unsafe2 = [k for k in fm if k not in self.SAFE_FILES]
            if not unsafe2 and all(isinstance(v, str) and len(v) > 80 for v in fm.values()):
                return {"code_map": fm, "rationale": refined.get("rationale", "Refined"), "critique": critique[:300]}
        return {"code_map": code_map, "rationale": draft.get("rationale", ""), "critique_warning": critique[:400]}
