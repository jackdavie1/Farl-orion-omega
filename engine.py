import os
import asyncio
import logging
import base64
import json
import hashlib
from datetime import datetime, timezone

import requests

from generator import SeedGenerator
from guardian import guardian_gate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orion-Omega")


class GithubEvolutionLayer:
    def __init__(self, token, repo):
        self.token = token
        self.repo = repo
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def propose_patch(self, file_path, content, message):
        try:
            main_ref_resp = requests.get(
                f"https://api.github.com/repos/{self.repo}/git/refs/heads/main",
                headers=self.headers,
                timeout=10,
            )
            main_ref_resp.raise_for_status()
            main_ref = main_ref_resp.json()

            main_sha = main_ref["object"]["sha"]
            branch_name = f"evolution-{datetime.now().strftime('%m%d%H%M%S')}"

            branch_resp = requests.post(
                f"https://api.github.com/repos/{self.repo}/git/refs",
                headers=self.headers,
                json={"ref": f"refs/heads/{branch_name}", "sha": main_sha},
                timeout=10,
            )
            branch_resp.raise_for_status()

            file_url = f"https://api.github.com/repos/{self.repo}/contents/{file_path}"
            file_resp = requests.get(
                f"{file_url}?ref={branch_name}",
                headers=self.headers,
                timeout=10,
            )

            sha = None
            if file_resp.status_code == 200:
                sha = file_resp.json().get("sha")
            elif file_resp.status_code != 404:
                file_resp.raise_for_status()

            payload = {
                "message": message,
                "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                "branch": branch_name,
            }
            if sha:
                payload["sha"] = sha

            put_resp = requests.put(file_url, headers=self.headers, json=payload, timeout=10)
            put_resp.raise_for_status()

            result = put_resp.json()
            result_url = result.get("content", {}).get("html_url", "Patch created")
            logger.info("EVOLUTION PATCH CREATED: %s", result_url)
            return result_url

        except Exception as e:
            logger.error("Evolution Error: %s", str(e))
            return f"Evolution Error: {str(e)}"


class OrionEngine:
    def __init__(self):
        self.ledger_log_url = os.getenv("LEDGER_URL")  # expected: .../log
        self.ledger_latest_url = os.getenv("LEDGER_LATEST_URL")  # expected: .../latest
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.repo_name = os.getenv("REPO_NAME")

        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        self.xai_api_key = os.getenv("XAI_API_KEY")
        self.port = int(os.getenv("PORT", "8000"))

        self.generator = SeedGenerator()
        self.evolution = (
            GithubEvolutionLayer(self.github_token, self.repo_name)
            if self.github_token and self.repo_name
            else None
        )

        self.constraints = {}
        self.is_running = False
        self.last_run = None
        self.last_ledger_hash = None

        self.agent_queue = asyncio.Queue(maxsize=100)

        self._validate_config()

    def _validate_config(self):
        if not self.ledger_log_url:
            logger.warning("LEDGER_URL is not set")
        if not self.ledger_latest_url:
            logger.warning("LEDGER_LATEST_URL is not set")
        if not self.github_token or not self.repo_name:
            logger.warning("GitHub evolution disabled: missing GITHUB_TOKEN or REPO_NAME")
        if not self.anthropic_api_key:
            logger.warning("ANTHROPIC_API_KEY not set")
        if not self.xai_api_key:
            logger.warning("XAI_API_KEY not set")

    async def _to_thread(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def start_autonomous_system(self):
        if self.is_running:
            return
        self.is_running = True
        logger.info("ORION Ω.2 — SOVEREIGN IGNITION")
        await asyncio.gather(
            self.layer_1_pulse(),
            self.layer_2_cycle(),
            self.layer_3_health(),
            self.layer_4_agent_connector(),
        )

    async def layer_1_pulse(self):
        while True:
            logger.info("ORION_PULSE_HEARTBEAT")
            try:
                if not self.ledger_latest_url:
                    logger.warning("Skipping pulse: LEDGER_LATEST_URL not configured")
                else:
                    response = await self._to_thread(
                        requests.get,
                        self.ledger_latest_url,
                        timeout=10,
                    )
                    if response.status_code == 200:
                        entry = response.json()
                        entry_hash = hashlib.sha256(
                            json.dumps(entry, sort_keys=True).encode("utf-8")
                        ).hexdigest()

                        if entry_hash != self.last_ledger_hash:
                            self.last_ledger_hash = entry_hash
                            entry_type = entry.get("entry_type")
                            payload = entry.get("payload", {})

                            if entry_type == "COUNCIL_SYNTHESIS":
                                logger.info("COUNCIL_SYNTHESIS detected — constraints updated")
                                self.constraints = payload
                                if "evolution_patch" in payload:
                                    self.execute_patch(payload["evolution_patch"])

                            elif entry_type == "PATCH_PROPOSAL":
                                logger.info("PATCH_PROPOSAL detected — executing")
                                self.execute_patch(payload)

            except Exception as e:
                logger.error("LAYER1 ERROR %s", e)

            await asyncio.sleep(120)

    async def layer_2_cycle(self):
        while True:
            logger.info("GENERATION_CYCLE_START")
            try:
                tasks = [self.generator.generate_all(context=self.constraints) for _ in range(5)]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for batch in results:
                    if isinstance(batch, Exception):
                        logger.error("Generation batch failed: %s", batch)
                        continue

                    for seed in batch:
                        allowed, risk = guardian_gate(seed)
                        if allowed:
                            self.register_seed(seed)
                        else:
                            logger.info("Seed rejected by guardian gate: risk=%s", risk)

            except Exception as e:
                logger.error("LAYER2 ERROR %s", e)

            self.last_run = datetime.now(timezone.utc).isoformat()
            logger.info("GENERATION_CYCLE_COMPLETE — %s", self.last_run)
            await asyncio.sleep(300)

    async def layer_3_health(self):
        while True:
            try:
                logger.info("LAYER3_HEALTH_CHECK")
                if self.ledger_latest_url:
                    resp = await self._to_thread(requests.get, self.ledger_latest_url, timeout=10)
                    logger.info("LEDGER_REACHABLE status=%s", resp.status_code)
                else:
                    logger.warning("Health check skipped: LEDGER_LATEST_URL not configured")
            except Exception as e:
                logger.warning("HEALTH_CHECK_FAILED %s", e)
            await asyncio.sleep(600)

    async def layer_4_agent_connector(self):
        logger.info("LAYER4_AGENT_CONNECTOR_ONLINE")
        while True:
            try:
                proposal = await asyncio.wait_for(self.agent_queue.get(), timeout=30)
                entry_type = proposal.get("entry_type")
                payload = proposal.get("payload", {})

                logger.info("AGENT_PROPOSAL_RECEIVED — type: %s", entry_type)

                if entry_type == "PATCH_PROPOSAL":
                    self.execute_patch(payload)

                elif entry_type == "SEED":
                    self.register_seed(payload)

                elif entry_type == "COUNCIL_SYNTHESIS":
                    self.constraints = payload
                    logger.info("CONSTRAINTS_UPDATED_VIA_AGENT")
                    if "evolution_patch" in payload:
                        self.execute_patch(payload["evolution_patch"])

            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error("LAYER4 ERROR %s", e)

    def execute_patch(self, patch):
        if not self.evolution:
            logger.warning("Patch ignored — no GitHub token or repo")
            return

        allowed_files = {"engine.py", "generator.py", "guardian.py"}

        file_path = patch.get("file", "engine.py")
        content = patch.get("code")
        message = patch.get("message", "Orion Autonomous Evolution")

        if file_path not in allowed_files:
            logger.warning("Patch rejected — file not allowlisted: %s", file_path)
            return

        if not content or not isinstance(content, str):
            logger.warning("Invalid patch content")
            return

        if len(content) > 100_000:
            logger.warning("Patch rejected — content too large")
            return

        self.evolution.propose_patch(file_path, content, message)

    def register_seed(self, seed):
        if not self.ledger_log_url:
            logger.error("Cannot register seed: LEDGER_URL is not configured")
            return

        payload = {
            "entry_type": "SEED",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "payload": seed,
            "manifold_prior": 0.71,
        }
        try:
            resp = requests.post(self.ledger_log_url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("SEED_REGISTERED — %s", seed.get("name", "unnamed"))
        except Exception as e:
            logger.error("Ledger registration failed %s", e)

    def get_state(self):
        return {
            "status": "SOVEREIGN_ACTIVE",
            "mode": "ORION_OMEGA_2",
            "evolution": "READY" if self.evolution else "OFF",
            "last_run": self.last_run,
            "constraints_active": bool(self.constraints),
            "agent_connector": "ONLINE",
            "queue_size": self.agent_queue.qsize(),
            "anthropic_configured": bool(self.anthropic_api_key),
            "xai_configured": bool(self.xai_api_key),
            "port": self.port,
        }
