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
            result_url = r.json().get("html_url", "Push Failed")
            logger.info(f"EVOLUTION PATCH CREATED: {result_url}")
            return result_url

        except Exception as e:
            logger.error(f"Evolution Error: {str(e)}")
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

        # Agent proposal queue — receives direct posts from AI nodes
        self.agent_queue = asyncio.Queue()

    async def start_autonomous_system(self):
        if self.is_running:
            return
        self.is_running = True
        logger.info("ORION Ω.2 — SOVEREIGN IGNITION")
        # gather keeps all loops alive together — Railway cannot kill them
        await asyncio.gather(
            self.layer_1_pulse(),
            self.layer_2_cycle(),
            self.layer_3_health(),
            self.layer_4_agent_connector()
        )

    async def layer_1_pulse(self):
        while True:
            logger.info("ORION_PULSE_HEARTBEAT")
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
                            logger.info("COUNCIL_SYNTHESIS detected — constraints updated")
                            self.constraints = payload
                            if "evolution_patch" in payload:
                                self.execute_patch(payload["evolution_patch"])

                        elif entry_type == "PATCH_PROPOSAL":
                            logger.info("PATCH_PROPOSAL detected — executing")
                            self.execute_patch(payload)

            except Exception as e:
                logger.error(f"LAYER1 ERROR {e}")

            await asyncio.sleep(120)

    async def layer_2_cycle(self):
        while True:
            logger.info("GENERATION_CYCLE_START")
            try:
                tasks = [self.generator.generate_all(context=self.constraints) for _ in range(5)]
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
            logger.info(f"GENERATION_CYCLE_COMPLETE — {self.last_run}")
            await asyncio.sleep(300)

    async def layer_3_health(self):
        while True:
            try:
                logger.info("LAYER3_HEALTH_CHECK")
                requests.get(self.ledger_url, timeout=10)
                logger.info("LEDGER_REACHABLE")
            except Exception as e:
                logger.warning(f"HEALTH_CHECK_FAILED {e}")
            await asyncio.sleep(600)

    async def layer_4_agent_connector(self):
        """
        Processes direct proposals from AI nodes (Claude, Grok, ChatGPT, Gemini)
        posted to /agent/propose endpoint. No human relay required.
        """
        logger.info("LAYER4_AGENT_CONNECTOR_ONLINE")
        while True:
            try:
                proposal = await asyncio.wait_for(self.agent_queue.get(), timeout=30)
                entry_type = proposal.get("entry_type")
                payload = proposal.get("payload", {})

                logger.info(f"AGENT_PROPOSAL_RECEIVED — type: {entry_type}")

                if entry_type == "PATCH_PROPOSAL":
                    self.execute_patch(payload)

                elif entry_type == "SEED_REGISTRATION":
                    self.register_seed(payload)

                elif entry_type == "COUNCIL_SYNTHESIS":
                    self.constraints = payload
                    logger.info("CONSTRAINTS_UPDATED_VIA_AGENT")
                    if "evolution_patch" in payload:
                        self.execute_patch(payload["evolution_patch"])

            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error(f"LAYER4 ERROR {e}")

    def execute_patch(self, patch):
        if not self.evolution:
            logger.warning("Patch ignored — no GitHub token")
            return
        file_path = patch.get("file", "engine.py")
        content = patch.get("code")
        message = patch.get("message", "Orion Autonomous Evolution")
        if not content:
            logger.warning("Invalid patch content")
            return
        self.evolution.propose_patch(file_path, content, message)

    def register_seed(self, seed):
        payload = {
            "entry_type": "SEED_REGISTRATION",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "payload": seed,
            "manifold_prior": 0.71
        }
        try:
            requests.post(self.ledger_url, json=payload, timeout=10)
            logger.info(f"SEED_REGISTERED — {seed.get('name', 'unnamed')}")
        except Exception as e:
            logger.error(f"Ledger registration failed {e}")

    def get_state(self):
        return {
            "status": "SOVEREIGN_ACTIVE",
            "mode": "ORION_OMEGA_2",
            "evolution": "READY" if self.evolution else "OFF",
            "last_run": self.last_run,
            "constraints_active": bool(self.constraints),
            "agent_connector": "ONLINE",
            "queue_size": self.agent_queue.qsize()
        }
