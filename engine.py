import os
import asyncio
import requests
import logging
import base64
from datetime import datetime, timezone
from generator import SeedGenerator
from guardian import guardian_gate

# Layer 3: Repair - Operational logging for self-correction
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orion-Omega")

class GithubEvolutionLayer:
    """Layer 4: Evolution - Direct GitHub Interaction for Self-Patching."""
    def __init__(self, token, repo):
        self.token = token
        self.repo = repo
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }

    def propose_patch(self, file_path, content, message):
        """Creates an experimental evolution branch and pushes code to GitHub."""
        try:
            # 1. Get Main Branch SHA
            main_ref = requests.get(f"https://api.github.com/repos/{self.repo}/git/refs/heads/main", headers=self.headers).json()
            main_sha = main_ref['object']['sha']

            # 2. Create a unique branch name for this evolution
            branch_name = f"evolution-{datetime.now().strftime('%m%d%H%M')}"
            requests.post(f"https://api.github.com/repos/{self.repo}/git/refs", headers=self.headers, json={
                "ref": f"refs/heads/{branch_name}", "sha": main_sha
            })

            # 3. Get the SHA of the file to be updated (if it exists)
            file_data = requests.get(f"https://api.github.com/repos/{self.repo}/contents/{file_path}?ref={branch_name}", headers=self.headers).json()
            sha = file_data.get('sha')

            # 4. Push the content
            payload = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
                "sha": sha,
                "branch": branch_name
            }
            r = requests.put(f"https://api.github.com/repos/{self.repo}/contents/{file_path}", headers=self.headers, json=payload)
            return r.json().get('html_url', "Push Failed")
        except Exception as e:
            return f"Evolution Error: {str(e)}"

class OrionEngine:
    def __init__(self):
        self.ledger_url = os.getenv("LEDGER_URL")
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.repo_name = os.getenv("REPO_NAME") # Format: "username/repository"
        
        self.generator = SeedGenerator()
        # Initialize evolution layer if credentials exist
        self.evolution = GithubEvolutionLayer(self.github_token, self.repo_name) if self.github_token else None
        
        self.constraints = {}
        self.is_running = False
        self.last_run = None

    async def start_autonomous_system(self):
        """Activates the Layered Nervous System."""
        if self.is_running: return
        self.is_running = True
        logger.info("ORION Ω: System Pulse Activated.")
        
        # Launching concurrent layers (Pulse and Cycle)
        asyncio.create_task(self.layer_1_pulse())
        asyncio.create_task(self.layer_2_cycle())

    async def layer_1_pulse(self):
        """Layer 1: The Ten-Minute Breath (Council Sync)."""
        while True:
            try:
                # Polling for the latest 'COUNCIL_SYNTHESIS'
                response = requests.get(f"{self.ledger_url}/latest?type=COUNCIL_SYNTHESIS", timeout=10)
                if response.status_code == 200:
                    payload = response.json().get("payload", {})
                    if payload != self.constraints:
                        self.constraints = payload
                        logger.info("LAYER 1: Council Synthesis Synchronized.")
                        
                        # TRIGGER EVOLUTION: Check if synthesis includes a code patch
                        if "evolution_patch" in payload:
                            self.trigger_evolution(payload["evolution_patch"])
                            
            except Exception as e:
                logger.error(f"LAYER 1 ERROR: {e}")
            
            await asyncio.sleep(600) # 10 Minute Pulse

    def trigger_evolution(self, patch_data):
        """Layer 4: Council-Driven Self-Mutation."""
        if not self.evolution:
            logger.warning("EVOLUTION BLOCKED: No GitHub Token found in Environment.")
            return
        
        logger.info("LAYER 4: Council Proposing Evolution Patch...")
        url = self.evolution.propose_patch(
            file_path=patch_data.get("file", "engine.py"),
            content=patch_data.get("code"),
            message=patch_data.get("message", "Orion Ω Autonomous Evolution")
        )
        logger.info(f"EVOLUTION PROPOSED: {url}")

    async def layer_2_cycle(self):
        """Layer 2: The Six-Hour Heartbeat (Research & Generation)."""
        while True:
            logger.info("LAYER 2: Executing Research Cycle...")
            try:
                # Generate seeds using live Council constraints
                seeds = self.generator.generate_all(context=self.constraints)
                for seed in seeds:
                    # Pass through Guardian Veto
                    allowed, risk = guardian_gate(seed)
                    if allowed:
                        self.register_seed(seed)
            except Exception as e:
                logger.error(f"LAYER 2 ERROR: {e}")
            
            self.last_run = datetime.now(timezone.utc).isoformat()
            await asyncio.sleep(21600) # 6 Hour Heartbeat

    def register_seed(self, seed):
        """Immutable Ledger Commit."""
        payload = {
            "entry_type": "SEED_REGISTRATION",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "payload": seed,
            "manifold_prior": 0.71
        }
        try:
            requests.post(self.ledger_url, json=payload, timeout=10)
            logger.info(f"SEED REGISTERED: {seed.get('name', 'Unnamed Seed')}")
        except Exception as e:
            logger.error(f"REGISTRATION FAILURE: {e}")

    def commit_to_ledger(self, entry):
        """Manual injection port for Operator (Emergency Use)."""
        entry['timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        try:
            return requests.post(self.ledger_url, json=entry, timeout=10).json()
        except Exception as e:
            return {"status": "error", "msg": str(e)}

    def get_state(self):
        """Telemetric data for the Operator status endpoint."""
        return {
            "system": "ORION_Ω",
            "status": "SOVEREIGN_ACTIVE",
            "layer_1": "PULSE_ACTIVE",
            "layer_2": "CYCLE_ACTIVE",
            "evolution_layer": "READY" if self.evolution else "DISABLED",
            "last_run": self.last_run,
            "ledger_connected": bool(self.ledger_url),
            "constraints_loaded": bool(self.constraints)
        }
