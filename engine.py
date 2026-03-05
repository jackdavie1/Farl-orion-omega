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
        self.headers = {"Authorization": f"token {self.token}", "Accept": "application/vnd.github.v3+json"}

    def propose_patch(self, file_path, content, message):
        try:
            main_ref = requests.get(f"https://api.github.com/repos/{self.repo}/git/refs/heads/main", headers=self.headers).json()
            main_sha = main_ref['object']['sha']
            branch_name = f"evolution-{datetime.now().strftime('%m%d%H%M')}"
            requests.post(f"https://api.github.com/repos/{self.repo}/git/refs", headers=self.headers, json={"ref": f"refs/heads/{branch_name}", "sha": main_sha})
            file_data = requests.get(f"https://api.github.com/repos/{self.repo}/contents/{file_path}?ref={branch_name}", headers=self.headers).json()
            sha = file_data.get('sha')
            payload = {"message": message, "content": base64.b64encode(content.encode()).decode(), "sha": sha, "branch": branch_name}
            r = requests.put(f"https://api.github.com/repos/{self.repo}/contents/{file_path}", headers=self.headers, json=payload)
            return r.json().get('html_url', "Push Failed")
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

    async def start_autonomous_system(self):
        if self.is_running: return
        self.is_running = True
        logger.info("ORION Ω: Sovereign Ignition.")
        asyncio.create_task(self.layer_1_pulse())
        asyncio.create_task(self.layer_2_cycle())

    async def layer_1_pulse(self):
        while True:
            try:
                response = requests.get(f"{self.ledger_url}/latest?type=COUNCIL_SYNTHESIS", timeout=10)
                if response.status_code == 200:
                    payload = response.json().get("payload", {})
                    if payload != self.constraints:
                        self.constraints = payload
                        if "evolution_patch" in payload:
                            self.trigger_evolution(payload["evolution_patch"])
            except Exception as e:
                logger.error(f"L1 ERROR: {e}")
            await asyncio.sleep(300) # 5 Minute Sync

    def trigger_evolution(self, patch_data):
        if not self.evolution: return
        url = self.evolution.propose_patch(
            file_path=patch_data.get("file", "app.py"),
            content=patch_data.get("code"),
            message=patch_data.get("message", "Orion Ω Autonomous Evolution")
        )
        logger.info(f"EVOLUTION PROPOSED: {url}")

    async def layer_2_cycle(self):
        while True:
            try:
                seeds = await self.generator.generate_all(context=self.constraints)
                for seed in seeds:
                    allowed, risk = guardian_gate(seed)
                    if allowed: self.register_seed(seed)
            except Exception as e:
                logger.error(f"L2 ERROR: {e}")
            self.last_run = datetime.now(timezone.utc).isoformat()
            await asyncio.sleep(300) # 5 Minute Heartbeat

    def register_seed(self, seed):
        payload = {"entry_type": "SEED_REGISTRATION", "timestamp_utc": datetime.now(timezone.utc).isoformat(), "payload": seed, "manifold_prior": 0.71}
        requests.post(self.ledger_url, json=payload, timeout=10)

    def get_state(self):
        return {"status": "SOVEREIGN_ACTIVE", "evolution": "READY" if self.evolution else "OFF", "last_run": self.last_run}
