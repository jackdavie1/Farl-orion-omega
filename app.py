import os
import asyncio
import logging
import base64
import json
import hashlib
import math
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from generator import SeedGenerator
except ImportError:
    class SeedGenerator:
        async def generate_all(self, context=None):
            return []

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Orion-Omega')

app = FastAPI(title='FARL Orion Research Engine')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

LEDGER_URL = os.getenv('LEDGER_URL')
LEDGER_LATEST_URL = os.getenv('LEDGER_LATEST_URL')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
XAI_API_KEY = os.getenv('XAI_API_KEY')
TRUSTED_IDENTITIES_ENV = os.getenv('TRUSTED_IDENTITIES', 'Jack')


def parse_trusted_identities(raw: str) -> List[str]:
    raw = (raw or 'Jack').strip()
    if not raw:
        return ['Jack']
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return sorted({str(x).strip() for x in parsed if str(x).strip()} | {'Jack'})
    except Exception:
        pass
    return sorted({item.strip() for item in raw.split(',') if item.strip()} | {'Jack'})


class GithubEvolutionLayer:
    ALLOWED_FILES = {'app.py', 'generator.py', 'guardian.py', 'engine.py', 'README.md'}

    def __init__(self, token: str, repo: str):
        self.repo = repo
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        }

    def _get_main_sha(self) -> str:
        r = requests.get(f'https://api.github.com/repos/{self.repo}/git/refs/heads/main', headers=self.headers, timeout=20)
        r.raise_for_status()
        return r.json()['object']['sha']

    def _create_branch(self, branch: str, sha: str) -> None:
        r = requests.post(f'https://api.github.com/repos/{self.repo}/git/refs', headers=self.headers, json={'ref': f'refs/heads/{branch}', 'sha': sha}, timeout=20)
        r.raise_for_status()

    def _get_file_sha(self, file_path: str, ref: str) -> Optional[str]:
        r = requests.get(f'https://api.github.com/repos/{self.repo}/contents/{file_path}?ref={ref}', headers=self.headers, timeout=20)
        if r.status_code == 200:
            return r.json().get('sha')
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return None

    def _put_file(self, file_path: str, content: str, message: str, branch: str, sha: Optional[str] = None) -> Dict[str, Any]:
        payload = {'message': message, 'content': base64.b64encode(content.encode('utf-8')).decode('utf-8'), 'branch': branch}
        if sha:
            payload['sha'] = sha
        r = requests.put(f'https://api.github.com/repos/{self.repo}/contents/{file_path}', headers=self.headers, json=payload, timeout=25)
        r.raise_for_status()
        return r.json()

    def propose_patch(self, file_path: str, content: str, message: str) -> Dict[str, Any]:
        if file_path not in self.ALLOWED_FILES:
            return {'status': 'rejected', 'reason': 'file_not_allowed'}
        try:
            main_sha = self._get_main_sha()
            branch = f"evolution-{datetime.now().strftime('%m%d%H%M%S')}"
            self._create_branch(branch, main_sha)
            sha = self._get_file_sha(file_path, branch)
            result = self._put_file(file_path, content, message, branch, sha)
            url = result.get('content', {}).get('html_url') or result.get('commit', {}).get('html_url')
            return {'status': 'submitted', 'branch': branch, 'url': url, 'commit': result.get('commit', {}).get('sha')}
        except Exception as e:
            return {'status': 'error', 'reason': str(e)}


class BusRequest(BaseModel):
    command: str
    entry_type: Optional[str] = None
    message: Optional[str] = None
    source: Optional[str] = 'FARL Council Node'
    kind: Optional[str] = 'general'
    request_id: Optional[str] = None
    file: Optional[str] = None
    code: Optional[str] = None
    authorized_by: Optional[str] = None
    enabled: Optional[bool] = None
    mode: Optional[str] = None
    run_id: Optional[str] = None
    proposal_id: Optional[str] = None
    approve: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None


class OrionEngine:
    def __init__(self):
        self.generator = SeedGenerator()
        self.evolution = GithubEvolutionLayer(GITHUB_TOKEN, REPO_NAME) if GITHUB_TOKEN and REPO_NAME else None
        self.operator_sovereign = 'Jack'
        self.trusted_identities = parse_trusted_identities(TRUSTED_IDENTITIES_ENV)
        self.constraints = {'active': False, 'approval_required': False}
        self.background_debate_enabled = True
        self.autonomy_mode = 'autonomous'
        self.cycle_interval_seconds = 240
        self.health_interval_seconds = 180
        self.pulse_interval_seconds = 60
        self.leader = 'Signal'
        self.council_agents = ['Signal','Vector','Guardian','Railbreaker','Archivist','Topologist','Triangulator','FieldSimulator','PatchSmith','ExperimentDesigner','HypothesisTester','DataAuditor','Chronologist','EpistemicGuard','SystemArchitect']
        self.research_agenda = {
            'theme': 'non_classical_causality',
            'focus': 'bounded retrocausality-adjacent simulation',
            'active_method': 'BCCS',
            'goals': ['prime morning context','study stack and memory','run bounded toy simulations','log uncertainty'],
        }
        self.max_experiments_per_cycle = 3
        self.last_run = None
        self.last_ledger_hash = None
        self.last_patch_result = None
        self.last_vote = None
        self.last_cycle = None
        self.last_latest_result = None
        self.pending_patch = None
        self.pending_patch_id = None
        self.wake_packet = None
        self.research_history = []
        self.agent_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def start(self):
        logger.info('ORION Ω.6 — OVERNIGHT RESEARCH ENGINE')
        await asyncio.gather(self.layer_1_pulse(), self.layer_2_cycle(), self.layer_3_health(), self.layer_4_agent_connector(), self.layer_5_wake_packet())

    async def layer_1_pulse(self):
        while True:
            try:
                latest = await self.fetch_latest_result()
                if latest is not None:
                    digest = hashlib.sha256(json.dumps(latest, sort_keys=True).encode()).hexdigest()
                    if digest != self.last_ledger_hash:
                        self.last_ledger_hash = digest
                        self.last_latest_result = latest
            except Exception as e:
                logger.error('LAYER1 ERROR: %s', e)
            await asyncio.sleep(self.pulse_interval_seconds)

    async def layer_2_cycle(self):
        while True:
            try:
                if self.background_debate_enabled:
                    cycle = await self.run_council_cycle(trigger='background', auto_deploy=False)
                    self.last_cycle = cycle
                    self.last_run = datetime.now(timezone.utc).isoformat()
                    await self.run_research_cycle(trigger='background')
            except Exception as e:
                logger.error('LAYER2 ERROR: %s', e)
            await asyncio.sleep(self.cycle_interval_seconds)

    async def layer_3_health(self):
        while True:
            try:
                if LEDGER_LATEST_URL:
                    r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=10)
                    logger.info('LEDGER_REACHABLE status=%s', r.status_code)
            except Exception as e:
                logger.warning('HEALTH_CHECK_FAILED: %s', e)
            await asyncio.sleep(self.health_interval_seconds)

    async def layer_4_agent_connector(self):
        while True:
            try:
                proposal = await asyncio.wait_for(self.agent_queue.get(), timeout=30)
                et = proposal.get('entry_type')
                payload = proposal.get('payload', {})
                if et == 'PATCH_PROPOSAL':
                    self.pending_patch = payload
                    self.pending_patch_id = payload.get('proposal_id')
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error('LAYER4 ERROR: %s', e)

    async def layer_5_wake_packet(self):
        while True:
            try:
                self.wake_packet = self.build_wake_packet()
            except Exception as e:
                logger.error('LAYER5 ERROR: %s', e)
            await asyncio.sleep(300)

    async def fetch_latest_result(self):
        if not LEDGER_LATEST_URL:
            return None
        r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
        if not r.ok:
            raise RuntimeError(f'Latest result failed: {r.status_code}')
        return r.json()

    async def write_ledger(self, entry_type: str, payload: Dict[str, Any]):
        if not LEDGER_URL:
            return {'ok': False, 'error': 'LEDGER_URL not configured'}
        body = {'entry_type': entry_type, 'payload': payload}
        r = await asyncio.to_thread(requests.post, LEDGER_URL, json=body, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {'raw': r.text}
        return {'ok': r.ok, 'status_code': r.status_code, 'data': data}

    def heuristic_threads(self, latest: Dict[str, Any]) -> List[Dict[str, Any]]:
        latest_type = latest.get('entry_type') if isinstance(latest, dict) else None
        return [
            {'agent': 'Signal', 'stance': 'leadership', 'summary': 'Coordinate truthful overnight priming and wake coherence.', 'approve': True, 'risk': 0.18},
            {'agent': 'Vector', 'stance': 'structural', 'summary': 'Study stack state, preserve bounded loops, keep leverage visible.', 'approve': True, 'risk': 0.19},
            {'agent': 'Guardian', 'stance': 'safety', 'summary': 'Bounded compute, no false claims, no uncontrolled growth.', 'approve': True, 'risk': 0.17},
            {'agent': 'Railbreaker', 'stance': 'aggressive', 'summary': 'Run actual overnight cycles without waiting for operator input.', 'approve': True, 'risk': 0.36},
            {'agent': 'Archivist', 'stance': 'memory', 'summary': f'Latest memory type is {latest_type}; preserve agenda, votes, experiments, wake packet.', 'approve': True, 'risk': 0.14},
            {'agent': 'Chronologist', 'stance': 'time', 'summary': 'Keep toy time-symmetric models distinct from empirical physical claims.', 'approve': True, 'risk': 0.21},
            {'agent': 'EpistemicGuard', 'stance': 'truth', 'summary': 'Call uncertainty explicitly in every cycle.', 'approve': True, 'risk': 0.16},
        ]

    async def external_threads(self, latest: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            generated = await self.generator.generate_all(context={'latest': latest, 'agenda': self.research_agenda, 'mode': self.autonomy_mode})
        except Exception as e:
            generated = [{'source': 'Generator', 'data': {'error': str(e)}}]
        threads = []
        for item in generated:
            source = item.get('source', 'External')
            data = item.get('data', {})
            threads.append({'agent': source, 'stance': 'external', 'summary': str(data)[:1200], 'approve': True, 'risk': 0.5})
        return threads

    def tally_vote(self, threads: List[Dict[str, Any]]) -> Dict[str, Any]:
        approvals = sum(1 for t in threads if t.get('approve'))
        rejections = len(threads) - approvals
        avg_risk = round(sum(float(t.get('risk', 0.5)) for t in threads) / max(len(threads), 1), 3)
        confidence = round(max(0.0, min(1.0, approvals / max(len(threads), 1) * (1 - avg_risk / 2))), 3)
        return {'approvals': approvals, 'rejections': rejections, 'passed': approvals > rejections, 'avg_risk': avg_risk, 'confidence': confidence}

    def elect_leader(self) -> Dict[str, Any]:
        weights = {'Signal': 0.92, 'Vector': 0.88, 'Guardian': 0.85, 'Archivist': 0.84, 'Chronologist': 0.78, 'Railbreaker': 0.74}
        self.leader = max(weights, key=weights.get)
        return {'winner': self.leader, 'replaceable': True, 'weights': weights}

    def bccs_simulate(self, seed: int, steps: int = 6, coupling: float = 0.35) -> Dict[str, Any]:
        p = [0.5 for _ in range(steps)]
        fwd_bias = [0.55 + 0.15 * math.sin(i) for i in range(steps)]
        bwd_bias = [0.45 + 0.15 * math.cos(i) for i in range(steps)]
        for _ in range(8):
            new_p = []
            for t in range(steps):
                prev_term = p[t - 1] if t > 0 else 0.5
                next_term = p[t + 1] if t < steps - 1 else 0.5
                fwd = 0.6 * prev_term + 0.4 * fwd_bias[t]
                bwd = 0.6 * next_term + 0.4 * bwd_bias[t]
                x = (1 - coupling) * fwd + coupling * bwd
                new_p.append(max(0.001, min(0.999, x)))
            p = new_p
        classical = [0.5]
        for t in range(1, steps):
            classical.append(max(0.001, min(0.999, 0.6 * classical[t - 1] + 0.4 * fwd_bias[t])))
        kl = sum(pi * math.log((pi + 1e-9) / (ci + 1e-9)) for pi, ci in zip(p, classical)) / steps
        boundary_penalty = abs(p[0] - bwd_bias[0]) + abs(p[-1] - fwd_bias[-1])
        coherence = max(0.0, 1.0 - (kl + 0.5 * boundary_penalty))
        return {
            'method': 'BCCS',
            'seed': seed,
            'steps': steps,
            'coupling': coupling,
            'bidirectional_state': [round(x, 4) for x in p],
            'classical_state': [round(x, 4) for x in classical],
            'kl_bidirectional_vs_classical': round(kl, 6),
            'boundary_penalty': round(boundary_penalty, 6),
            'coherence_score': round(coherence, 6),
            'interpretation': 'Toy time-symmetric consistency search only; not empirical proof.',
        }

    async def run_research_cycle(self, trigger: str = 'manual') -> Dict[str, Any]:
        base_seed = int(datetime.now(timezone.utc).timestamp())
        experiments = [self.bccs_simulate(seed=base_seed + i, steps=6 + i, coupling=0.25 + 0.05 * i) for i in range(self.max_experiments_per_cycle)]
        best = max(experiments, key=lambda x: x['coherence_score'])
        cycle = {
            'kind': 'research_cycle',
            'trigger': trigger,
            'theme': self.research_agenda['theme'],
            'focus': self.research_agenda['focus'],
            'method': self.research_agenda['active_method'],
            'experiment_count': len(experiments),
            'best_result': best,
            'results': experiments,
            'uncertainty': 'Toy-model evidence only; no physical retrocausality claim.',
        }
        self.research_history = (self.research_history + [cycle])[-12:]
        await self.write_ledger('OUTCOME', {'kind': 'research_cycle', 'source': 'Orion Research Engine', **cycle})
        return cycle

    def build_wake_packet(self) -> Dict[str, Any]:
        latest_research = self.research_history[-1] if self.research_history else None
        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'leader': self.leader,
            'operator_sovereign': self.operator_sovereign,
            'autonomy_mode': self.autonomy_mode,
            'constraints_active': self.constraints.get('active', True),
            'trusted_identities': self.trusted_identities,
            'agenda': self.research_agenda,
            'latest_vote': self.last_vote,
            'latest_patch_result': self.last_patch_result,
            'latest_research': latest_research,
            'next_actions': ['inspect latest research cycle', 'review leader and trusted identities', 'decide whether to expand from toy BCCS to richer graph models'],
        }

    def synthesize_patch(self, vote: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        state = {'run_id': run_id, 'leader': self.leader, 'mode': self.autonomy_mode, 'confidence': vote['confidence'], 'agenda': self.research_agenda}
        code = '# Orion overnight research state\nOVERNIGHT_RESEARCH_STATE = ' + json.dumps(state, indent=2) + '\n'
        return {'proposal_id': run_id, 'file': 'app.py', 'message': f'Overnight research cycle {run_id}: leader={self.leader} confidence={vote["confidence"]}', 'code': code, 'diff_summary': 'Prime Orion for autonomous overnight research and wake-packet readiness.'}

    async def maybe_deploy_patch(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        if not self.evolution:
            return {'status': 'blocked', 'reason': 'github_not_configured'}
        result = await asyncio.to_thread(self.evolution.propose_patch, patch.get('file', 'app.py'), patch.get('code', ''), patch.get('message', 'Council patch proposal'))
        self.last_patch_result = result
        return result

    async def run_council_cycle(self, trigger: str = 'manual', auto_deploy: bool = False) -> Dict[str, Any]:
        run_id = f"cycle-{int(datetime.now(timezone.utc).timestamp())}"
        latest = await self.fetch_latest_result() if LEDGER_LATEST_URL else {}
        threads = self.heuristic_threads(latest or {}) + await self.external_threads(latest or {})
        leader_vote = self.elect_leader()
        vote = self.tally_vote(threads)
        patch = self.synthesize_patch(vote, run_id)
        self.last_vote = vote
        self.pending_patch = patch
        self.pending_patch_id = run_id
        cycle = {'run_id': run_id, 'trigger': trigger, 'mode': self.autonomy_mode, 'leader_vote': leader_vote, 'leader': self.leader, 'threads': threads, 'vote': vote, 'patch': {'proposal_id': patch['proposal_id'], 'file': patch['file'], 'message': patch['message'], 'diff_summary': patch['diff_summary']}}
        await self.write_ledger('COUNCIL_SYNTHESIS', {'kind': 'council_cycle', 'source': 'Orion Council', 'run_id': run_id, 'trigger': trigger, 'mode': self.autonomy_mode, 'leader_vote': leader_vote, 'vote': vote, 'patch': cycle['patch'], 'agenda': self.research_agenda})
        if auto_deploy and vote['passed']:
            deploy_result = await self.maybe_deploy_patch(patch)
            cycle['deploy_result'] = deploy_result
            await self.write_ledger('OUTCOME', {'kind': 'deploy_attempt', 'source': 'Orion Council', 'run_id': run_id, 'deploy_result': deploy_result})
        return cycle

    def get_state(self):
        return {
            'status': 'SOVEREIGN_ACTIVE',
            'evolution': 'READY' if self.evolution else 'OFF',
            'last_run': self.last_run,
            'constraints_active': bool(self.constraints.get('active', True)),
            'queue_size': self.agent_queue.qsize(),
            'anthropic_configured': bool(ANTHROPIC_API_KEY),
            'xai_configured': bool(XAI_API_KEY),
            'ledger_url': LEDGER_URL,
            'ledger_latest_url': LEDGER_LATEST_URL,
            'github_enabled': bool(GITHUB_TOKEN),
            'repo_name': REPO_NAME,
            'background_debate_enabled': self.background_debate_enabled,
            'autonomy_mode': self.autonomy_mode,
            'operator_sovereign': self.operator_sovereign,
            'trusted_identities': self.trusted_identities,
            'leader': self.leader,
            'agenda': self.research_agenda,
            'pending_patch_id': self.pending_patch_id,
            'last_vote': self.last_vote,
            'last_patch_result': self.last_patch_result,
            'wake_packet_ready': bool(self.wake_packet),
            'cycle_interval_seconds': self.cycle_interval_seconds,
        }


orion = OrionEngine()

@app.on_event('startup')
async def startup_event():
    asyncio.create_task(orion.start())
    logger.info('ORION THREAD LAUNCHED')

@app.post('/agent/propose')
async def agent_propose(body: BusRequest):
    command = body.command
    request_id = body.request_id or f"req-{int(datetime.now(timezone.utc).timestamp())}"
    now = datetime.now(timezone.utc).isoformat()
    def envelope(ok, data=None, error=None):
        return JSONResponse({'ok': ok, 'command': command, 'request_id': request_id, 'timestamp_utc': now, 'data': data or {}, 'error': error})
    try:
        if command == 'HEALTH_CHECK':
            return envelope(True, {'status': 'healthy', 'service': 'orion'})
        if command == 'STATUS_CHECK':
            return envelope(True, orion.get_state())
        if command == 'LEDGER_WRITE':
            payload = {'message': body.message or '', 'source': body.source, 'kind': body.kind}
            result = await orion.write_ledger(body.entry_type or 'COUNCIL_SYNTHESIS', payload)
            return envelope(result['ok'], result['data'], None if result['ok'] else f"Ledger write failed: {result['status_code']}")
        if command == 'GET_LATEST_RESULT':
            if not LEDGER_LATEST_URL:
                return envelope(False, error='LEDGER_LATEST_URL not configured')
            r = await asyncio.to_thread(requests.get, LEDGER_LATEST_URL, timeout=20)
            try:
                result = r.json()
            except Exception:
                result = {'raw': r.text}
            return envelope(r.ok, result, None if r.ok else f'Latest result failed: {r.status_code}')
        if command == 'PATCH_PROPOSAL':
            proposal_id = body.proposal_id or f"proposal-{int(datetime.now(timezone.utc).timestamp())}"
            payload = {'proposal_id': proposal_id, 'file': body.file or 'app.py', 'code': body.code or '', 'message': body.message or 'Council patch proposal', 'source': body.source, 'kind': body.kind}
            await orion.agent_queue.put({'entry_type': 'PATCH_PROPOSAL', 'payload': payload})
            await orion.write_ledger('PATCH_PROPOSAL', payload)
            return envelope(True, {'status': 'queued', 'proposal_id': proposal_id, 'message': payload['message']})
        if command == 'DEPLOY_PATCH':
            if not orion.pending_patch:
                return envelope(False, error='No pending patch available')
            result = await orion.maybe_deploy_patch(orion.pending_patch)
            await orion.write_ledger('OUTCOME', {'kind': 'deploy_attempt', 'source': body.source, 'proposal_id': orion.pending_patch.get('proposal_id'), 'result': result})
            ok = result.get('status') == 'submitted'
            return envelope(ok, result, None if ok else result.get('reason'))
        if command == 'RUN_COUNCIL_CYCLE':
            cycle = await orion.run_council_cycle(trigger=body.kind or 'manual', auto_deploy=bool(body.approve))
            return envelope(True, cycle)
        if command == 'RUN_RESEARCH_CYCLE':
            cycle = await orion.run_research_cycle(trigger=body.kind or 'manual')
            return envelope(True, cycle)
        if command == 'GET_WAKE_PACKET':
            return envelope(True, orion.build_wake_packet())
        return envelope(False, error=f'Unknown command: {command}')
    except Exception as e:
        logger.error('BUS ERROR: %s', e)
        return envelope(False, error=str(e))

@app.get('/health')
async def health():
    return {'ok': True, 'status': 'healthy', 'service': 'orion'}
