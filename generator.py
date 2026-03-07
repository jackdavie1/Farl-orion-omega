import os
import asyncio
from typing import Any, Dict, List, Optional

import requests


class SeedGenerator:
    def __init__(self):
        self.xai_api_key = os.getenv("XAI_API_KEY")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        self.xai_model = os.getenv("XAI_MODEL") or os.getenv("GROK_MODEL") or "grok-3-mini"
        self.anthropic_model = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")

    def _build_prompt(self, context: Optional[Dict[str, Any]] = None) -> str:
        context = context or {}
        agenda = context.get("agenda", {})
        hypotheses = context.get("hypotheses", {})
        world_model = context.get("world_model", {})
        mode = context.get("mode", "autonomous")
        objectives = context.get("objectives", [])
        return (
            "You are an external cognition thread for FARL, a persistent autonomous institution. "
            f"Mode: {mode}. Agenda: {agenda}. Objectives: {objectives}. Hypotheses: {hypotheses}. World-model: {world_model}. "
            "Respond in short structured prose with: stance, risk, next_move, and one dissent if appropriate."
        )

    async def _probe_xai(self, prompt: str) -> Dict[str, Any]:
        if not self.xai_api_key:
            return {"source": "Grok-Ensemble", "data": {"error": "XAI_NOT_CONFIGURED"}}
        headers = {"Authorization": f"Bearer {self.xai_api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.xai_model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 320,
            "temperature": 0.2,
        }
        try:
            r = await asyncio.to_thread(requests.post, "https://api.x.ai/v1/chat/completions", headers=headers, json=payload, timeout=40)
            data = r.json()
            if not r.ok:
                return {"source": "Grok-Ensemble", "data": {"error": data, "model": self.xai_model}}
            choices = data.get("choices", [])
            text = choices[0].get("message", {}).get("content", "") if choices and isinstance(choices[0], dict) else ""
            return {"source": "Grok-Ensemble", "data": {"text": text[:3500], "model": data.get("model", self.xai_model)}}
        except Exception as e:
            return {"source": "Grok-Ensemble", "data": {"error": str(e), "model": self.xai_model}}

    async def _probe_anthropic(self, prompt: str) -> Dict[str, Any]:
        if not self.anthropic_api_key:
            return {"source": "Claude-Ensemble", "data": {"error": "ANTHROPIC_NOT_CONFIGURED"}}
        headers = {
            "x-api-key": self.anthropic_api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": self.anthropic_model,
            "max_tokens": 320,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            r = await asyncio.to_thread(requests.post, "https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=40)
            data = r.json()
            if not r.ok:
                return {"source": "Claude-Ensemble", "data": {"error": data, "model": self.anthropic_model}}
            text = "".join(block.get("text", "") for block in data.get("content", []) if isinstance(block, dict))
            return {"source": "Claude-Ensemble", "data": {"text": text[:3500], "model": data.get("model", self.anthropic_model)}}
        except Exception as e:
            return {"source": "Claude-Ensemble", "data": {"error": str(e), "model": self.anthropic_model}}

    async def generate_all(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        prompt = self._build_prompt(context)
        return await asyncio.gather(self._probe_xai(prompt), self._probe_anthropic(prompt))
