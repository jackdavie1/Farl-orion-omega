import json
from typing import Dict, List, Optional


def parse_trusted_identities(raw: str) -> List[str]:
    raw = (raw or "Jack").strip()
    if not raw:
        return ["Jack"]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return sorted({str(x).strip() for x in parsed if str(x).strip()} | {"Jack"})
    except Exception:
        pass
    return sorted({item.strip() for item in raw.split(",") if item.strip()} | {"Jack"})


class GovernanceKernel:
    def __init__(self, operator_sovereign: str = "Jack", trusted_identities: Optional[List[str]] = None):
        self.operator_sovereign = operator_sovereign
        self.trusted_identities = sorted(set(trusted_identities or [operator_sovereign]))
        if operator_sovereign not in self.trusted_identities:
            self.trusted_identities.append(operator_sovereign)
            self.trusted_identities = sorted(set(self.trusted_identities))
        self.constraints = {"active": False, "approval_required": False}
        self.leader = "Signal"
        self.authority = {
            "direct_push": [operator_sovereign],
            "merge": [operator_sovereign],
            "rollback": [operator_sovereign],
            "trust_update": [operator_sovereign],
            "autonomy_toggle": [operator_sovereign],
        }
        self.trust_scores = {name: (1.0 if name == operator_sovereign else 0.6) for name in self.trusted_identities}

    def is_trusted(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.trusted_identities

    def can_mutate(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.authority.get("direct_push", [])

    def can_merge(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.authority.get("merge", [])

    def can_rollback(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.authority.get("rollback", [])

    def can_toggle(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.authority.get("autonomy_toggle", [])

    def set_trusted_identities(self, identities: List[str]) -> List[str]:
        self.trusted_identities = sorted({str(x).strip() for x in identities if str(x).strip()} | {self.operator_sovereign})
        for identity in self.trusted_identities:
            self.trust_scores.setdefault(identity, 0.6 if identity != self.operator_sovereign else 1.0)
        self.authority["direct_push"] = [self.operator_sovereign]
        self.authority["merge"] = [self.operator_sovereign]
        self.authority["rollback"] = [self.operator_sovereign]
        self.authority["trust_update"] = [self.operator_sovereign]
        self.authority["autonomy_toggle"] = [self.operator_sovereign]
        return self.trusted_identities

    def adjust_trust(self, identity: str, delta: float) -> float:
        current = self.trust_scores.get(identity, 0.5)
        updated = max(0.0, min(1.0, current + delta))
        self.trust_scores[identity] = updated
        return updated

    def grant_authority(self, identity: str, capability: str) -> Dict[str, object]:
        if identity not in self.trusted_identities:
            self.trusted_identities.append(identity)
            self.trusted_identities = sorted(set(self.trusted_identities))
        self.authority.setdefault(capability, [])
        if identity not in self.authority[capability]:
            self.authority[capability].append(identity)
        return {"capability": capability, "granted_to": sorted(self.authority[capability])}

    def elect_leader(self) -> Dict[str, object]:
        weights = {
            "Signal": 0.94,
            "Vector": 0.91,
            "Guardian": 0.88,
            "Archivist": 0.87,
            "Triangulator": 0.86,
            "Chronologist": 0.82,
            "Railbreaker": 0.79,
            "Supergrok": 0.95,
        }
        self.leader = max(weights, key=weights.get)
        return {"winner": self.leader, "replaceable": True, "weights": weights}

    def call_vote(self, motion: str, options: List[str], agent_count: int, preferred: Optional[str] = None) -> Dict[str, object]:
        tallies = {opt: 0 for opt in options}
        chosen = preferred if preferred in options else (options[0] if options else None)
        if chosen:
            tallies[chosen] = agent_count
        return {"motion": motion, "options": options, "tallies": tallies, "winner": chosen}

    def state(self) -> Dict[str, object]:
        return {
            "operator_sovereign": self.operator_sovereign,
            "trusted_identities": self.trusted_identities,
            "leader": self.leader,
            "authority": self.authority,
            "trust_scores": self.trust_scores,
            "constraints": self.constraints,
        }
