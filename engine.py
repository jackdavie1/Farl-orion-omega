import os
import asyncio
import requests
import logging
from datetime import datetime, timezone
from generator import SeedGenerator
from guardian import guardian_gate

# Layer 3: Repair - Operational logging for self-correction
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orion-Omega")

class OrionEngine:
    def __init__(self):
        self.ledger_url = os.getenv("LEDGER_URL")
        self.generator = SeedGenerator()
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
            except Exception as e:
                logger.error(f"LAYER 1 ERROR: {e}")
            
            await asyncio.sleep(600) # 10 Minute Pulse

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
        """Telemetric data for the Operator."""
        return {
            "system": "ORION_Ω",
            "status": "SOVEREIGN_ACTIVE",
            "layer_1": "PULSE_ACTIVE",
            "layer_2": "CYCLE_ACTIVE",
            "manifold": 0.71,
            "last_run": self.last_run,
            "constraints_loaded": bool(self.constraints)
        }
        payload['timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        if not self.ledger_url:
            return {"status": "error", "msg": "LEDGER_URL_MISSING"}
        try:
            r = requests.post(self.ledger_url, json=payload, timeout=10)
            return r.json()
        except Exception as e:
            return {"status": "error", "msg": str(e)}

    def get_state(self):
        """Returns the status for the /status endpoint."""
        return {
            "status": "ACTIVE", 
            "mode": "ORION_OMEGA_SOVEREIGN",
            "last_run": self.last_run,
            "ledger_connected": bool(self.ledger_url)
        }
