import os
import requests
import json
import asyncio

class SeedGenerator:
    def __init__(self):
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        self.xai_key = os.getenv("XAI_API_KEY")
        
        # Polyphony Protocol: 936 agents, 3 threads.
        self.polyphony_directive = (
            "ACTIVATE POLYPHONY MODE. You represent 936 agents. "
            "Provide three distinct reasoning threads: "
            "1. ADVERSARIAL: Attack current manifold prior (0.71). "
            "2. STRUCTURAL: Audit stability and risk. "
            "3. SYNTHESIS: The finalized seed. "
            "Format: JSON ONLY. Use technical quantum-probabilistic terminology."
        )

    def call_grok(self, context_data):
        if not self.xai_key: return {"error": "GROK_KEY_MISSING"}
        url = "https://api.x.ai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {self.xai_key}", "Content-Type": "application/json"}
        context_str = json.dumps(context_data) if context_data else "No_Active_Constraints"
        data = {
            "model": "grok-beta",
            "messages": [{"role": "system", "content": f"{self.polyphony_directive} CONTEXT: {context_str}"}],
            "response_format": {"type": "json_object"}
        }
        try:
            r = requests.post(url, headers=headers, json=data, timeout=20)
            return r.json()['choices'][0]['message']['content']
        except Exception as e:
            return {"error": f"GROK_OFFLINE: {str(e)}"}

    def call_anthropic(self, context_data):
        if not self.anthropic_key: return {"error": "ANTHROPIC_KEY_MISSING"}
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": self.anthropic_key, 
            "anthropic-version": "2023-06-01", 
            "content-type": "application/json"
        }
        context_str = json.dumps(context_data) if context_data else "No_Active_Constraints"
        data = {
            "model": "claude-3-haiku-20240307",
            "max_tokens": 600,
            "system": f"{self.polyphony_directive} CONTEXT: {context_str}",
            "messages": [{"role": "user", "content": "EXECUTE_CYCLE"}]
        }
        try:
            r = requests.post(url, headers=headers, json=data, timeout=20)
            return r.json()['content'][0]['text']
        except Exception as e:
            return {"error": f"ANTHROPIC_OFFLINE: {str(e)}"}

    async def generate_all(self, context={}):
        """10x Parallel Ignition: Fires ensemble nodes concurrently."""
        grok_task = asyncio.to_thread(self.call_grok, context)
        claude_task = asyncio.to_thread(self.call_anthropic, context)
        
        grok_payload, claude_payload = await asyncio.gather(grok_task, claude_task)
        
        return [
            {"source": "Grok-Ensemble", "data": self._parse(grok_payload)},
            {"source": "Claude-Ensemble", "data": self._parse(claude_payload)}
        ]

    def _parse(self, raw):
        """Unified JSON parsing for the ensemble."""
        try:
            if isinstance(raw, str):
                return json.loads(raw)
            return raw
        except Exception:
            return {"raw_output": str(raw)}
