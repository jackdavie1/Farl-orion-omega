import os
import asyncio
from datetime import datetime, timezone
import requests
from generator import SeedGenerator
from guardian import guardian_gate

class OrionEngine:
    def __init__(self):
        self.ledger_url = os.getenv("LEDGER_URL")
        self.generator = SeedGenerator()

    async def execute_cycle(self):
        # OBSERVE -> GENERATE -> SIMULATE -> REGISTER
        seeds = self.generator.generate_all()
        for seed in seeds:
            allowed, risk = guardian_gate(seed)
            if allowed:
                self.commit_to_ledger({"entry_type": "SEED_REGISTRATION", "payload": seed})

    def commit_to_ledger(self, payload):
        # Manual entry point for ChatGPT Council or Auto-Entries
        payload['timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        try:
            return requests.post(self.ledger_url, json=payload).json()
        except:
            return {"status": "error", "msg": "Ledger Offline"}

    def get_state(self):
        return {"status": "ACTIVE", "mode": "ORION_OMEGA_SOVEREIGN"}
