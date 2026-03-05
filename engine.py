import os
import asyncio
import requests
import logging
import base64
from datetime import datetime, timezone
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
            "Accept": "application/vnd.github.v3+json"
        }

    def propose_patch(self, file_path, content, message):

        try:

            main_ref = requests.get(
                f"https://api.github.com/repos/{self.repo}/git/refs/heads/main",
                headers=self.headers,
                timeout=10
            ).json()

            main_sha = main_ref['object']['sha']

            branch_name = f"evolution-{datetime.now().strftime('%m%d%H%M%S')}"

            requests.post(
                f"https://api.github.com/repos/{self.repo}/git/refs",
                headers=self.headers,
                json={"ref": f"refs/heads/{branch_name}", "sha": main_sha},
                timeout=10
            )

            file_url = f"https://api.github.com/repos/{self.repo}/contents/{file_path}"

            file_data = requests.get(
                f"{file_url}?ref={branch_name}",
                headers=self.headers,
                timeout=10
            ).json()

            sha = file_data.get("sha")

            payload = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
                "sha": sha,
                "branch": branch_name
            }

            r = requests.put(file_url, headers=self.headers, json=payload, timeout=10)

            return r.json().get("html_url", "Push Failed")

        except Exception as e:
            return f"Evolution Error: {str(e)}"


class OrionEngine:

    def __init__(self):

        self.ledger_url = os.getenv("LEDGER_URL")
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.repo_name = os.getenv("REPO_NAME")

        self.generator = SeedGenerator()

        self.evolution = GithubEvolutionLayer(self.github_token, self.repo_name) if self.github_token else None

        self.constraints = {}

        self.is_running = False
        self.last_run = None
        self.last_ledger_hash = None

    async def start_autonomous_system(self):

        if self.is_running:
            return

        self.is_running = True

        logger.info("ORION Ω.2 — AUTONOMOUS SYSTEM ONLINE")

        asyncio.create_task(self.layer_1_pulse())
        asyncio.create_task(self.layer_2_cycle())
        asyncio.create_task(self.layer_3_health())

    async def layer_1_pulse(self):

        while True:

            try:

                response = requests.get(
                    f"{self.ledger_url}/latest",
                    timeout=10
                )

                if response.status_code == 200:

                    entry = response.json()

                    entry_hash = str(entry)

                    if entry_hash != self.last_ledger_hash:

                        self.last_ledger_hash = entry_hash

                        entry_type = entry.get("entry_type")

                        payload = entry.get("payload", {})

                        if entry_type == "COUNCIL_SYNTHESIS":

                            logger.info("Council synthesis detected")

                            self.constraints = payload

                            if "evolution_patch" in payload:

                                self.execute_patch(payload["evolution_patch"])

                        elif entry_type == "PATCH_PROPOSAL":

                            logger.info("Autonomous patch proposal detected")

                            self.execute_patch(payload)

            except Exception as e:

                logger.error(f"LAYER1 ERROR {e}")

            await asyncio.sleep(120)

    def execute_patch(self, patch):

        if not self.evolution:
            logger.warning("Patch ignored. No GitHub token configured.")
            return

        file_path = patch.get("file", "engine.py")
        content = patch.get("code")
        message = patch.get("message", "Orion Autonomous Evolution")

        if not content:
            logger.warning("Invalid patch content")
            return

        url = self.evolution.propose_patch(file_path, content, message)

        logger.info(f"EVOLUTION PATCH CREATED: {url}")

    async def layer_2_cycle(self):

        while True:

            logger.info("LAYER2 — PARALLEL GENERATION")

            try:

                tasks = []

                for _ in range(5):

                    tasks.append(self.generator.generate_all(context=self.constraints))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for batch in results:

                    if isinstance(batch, Exception):
                        continue

                    for seed in batch:

                        allowed, risk = guardian_gate(seed)

                        if allowed:

                            self.register_seed(seed)

            except Exception as e:

                logger.error(f"LAYER2 ERROR {e}")

            self.last_run = datetime.now(timezone.utc).isoformat()

            await asyncio.sleep(300)

    async def layer_3_health(self):

        while True:

            try:

                logger.info("LAYER3 — HEALTH CHECK")

                requests.get(self.ledger_url, timeout=10)

            except Exception as e:

                logger.warning(f"Health check failed {e}")

            await asyncio.sleep(600)

    def register_seed(self, seed):

        payload = {
            "entry_type": "SEED_REGISTRATION",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "payload": seed,
            "manifold_prior": 0.71
        }

        try:

            requests.post(self.ledger_url, json=payload, timeout=10)

        except Exception as e:

            logger.error(f"Ledger registration failed {e}")

    def get_state(self):

        return {
            "status": "SOVEREIGN_ACTIVE",
            "mode": "ORION_OMEGA_2",
            "evolution": "READY" if self.evolution else "OFF",
            "last_run": self.last_run,
            "constraints_active": bool(self.constraints)
        }
