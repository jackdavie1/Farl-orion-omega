import os
import asyncio
from datetime import datetime, timezone
import requests
# Direct imports
from generator import SeedGenerator
from guardian import guardian_gate

class OrionEngine:
    def __init__(self):
        self.cycle_interval = 6 * 3600 
        self.ledger_url = os.getenv("LEDGER_URL")
        self.generator = SeedGenerator()
        self.last_cycle_time = None

    async def run_continuous_cycle(self):
        while True:
            await self.execute_cycle()
            await asyncio.sleep(self.cycle_interval)

    async def execute_cycle(self):
        self.last_cycle_time = datetime.now(timezone.utc).isoformat()
        seeds = self.generator.generate_all()
        for seed in seeds:
            allowed, reason = guardian_gate(seed)
            if allowed:
                self.commit_to_ledger(seed)
    
    def get_state(self):
        return {"status": "ACTIVE", "last_cycle": self.last_cycle_time}

    def commit_to_ledger(self, payload):
        try:
            r = requests.post(self.ledger_url, json=payload)
            return r.json()
        except:
            return {"error": "Ledger connection failed"}
