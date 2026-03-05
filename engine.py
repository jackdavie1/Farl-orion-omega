import os
import asyncio
from datetime import datetime, timezone
import requests
# Ensure these files (generator.py, guardian.py) are in the same folder
from generator import SeedGenerator
from guardian import guardian_gate

class OrionEngine:
    def __init__(self):
        self.ledger_url = os.getenv("LEDGER_URL")
        self.generator = SeedGenerator()
        self.cycle_interval = 21600 # 6 Hours in seconds
        self.last_run = None

    async def run_continuous_cycle(self):
        """The heartbeat loop that keeps Orion Ω alive on Railway."""
        while True:
            try:
                await self.execute_cycle()
            except Exception as e:
                print(f"Cycle Error: {e}")
            
            self.last_run = datetime.now(timezone.utc).isoformat()
            await asyncio.sleep(self.cycle_interval)

    async def execute_cycle(self):
        """The core research logic."""
        seeds = self.generator.generate_all()
        for seed in seeds:
            allowed, risk = guardian_gate(seed)
            if allowed:
                self.commit_to_ledger({"entry_type": "SEED_REGISTRATION", "payload": seed})

    def commit_to_ledger(self, payload):
        """Pushes data to Module 1 Ledger."""
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
