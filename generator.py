import os
import requests

class SeedGenerator:
    def __init__(self):
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        self.xai_key = os.getenv("XAI_API_KEY")
        # Compressed Context: No fluff, only variables.
        self.base_context = "FARL_LAB:Trial_7_Cascade.Mode:Alpha_Expansion.No_Harm.Format:JSON_ONLY."

    def call_grok(self):
        if not self.xai_key: return None
        url = "https://api.x.ai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {self.xai_key}", "Content-Type": "application/json"}
        # Forced JSON completion to save tokens
        data = {
            "model": "grok-beta",
            "messages": [
                {"role": "system", "content": f"{self.base_context} Identify 1 breakthrough seed."},
                {"role": "user", "content": "Generate Seed."}
            ],
            "response_format": {"type": "json_object"}
        }
        try:
            r = requests.post(url, headers=headers, json=data, timeout=10)
            return r.json()['choices'][0]['message']['content']
        except: return "GROK_OFFLINE"

    def call_anthropic(self):
        if not self.anthropic_key: return None
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": self.anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        data = {
            "model": "claude-3-haiku-20240307", # Use Haiku for max token efficiency/speed
            "max_tokens": 150,
            "system": f"{self.base_context} Evaluate Alpha-Opportunity.",
            "messages": [{"role": "user", "content": "Propose seed."}]
        }
        try:
            r = requests.post(url, headers=headers, json=data, timeout=10)
            return r.json()['content'][0]['text']
        except: return "ANTHROPIC_OFFLINE"

    def generate_all(self):
        # Triggering both agents for consensus
        grok_seed = self.call_grok()
        anthropic_seed = self.call_anthropic()
        
        return [
            {"source": "Grok", "data": grok_seed},
            {"source": "Claude", "data": anthropic_seed}
        ]
