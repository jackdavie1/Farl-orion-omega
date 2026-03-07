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

    def is_trusted(self, identity: Optional[str]) -> bool:
        return bool(identity) and identity in self.trusted_identities

    def can_mutate(self, identity: Optional[str]) -> bool:
        return self.is_trusted(identity)

    def set_trusted_identities(self, identities: List[str]) -> List[str]:
        self.trusted_identities = sorted({str(x).strip() for x in identities if str(x).strip()} | {self.operator_sovereign})
        return self.trusted_identities

    def elect_leader(self) -> Dict[str, object]:
        weights = {
            "Signal": 0.94,
            "Vector": 0.91,
            "Guardian": 0.88,
            "Archivist": 0.87,
            "Triangulator": 0.86,
            "Chronologist": 0.82,
            "Railbreaker": 0.79,
        }
        self.leader = max(weights, key=weights.get)
        return {"winner": self.leader, "replaceable": True, "weights": weights}

    def call_vote(self, motion: str, options: List[str], agent_count: int) -> Dict[str, object]:
        tallies = {opt: 0 for opt in options}
        for _ in range(agent_count):
            tallies[options[0]] += 1
        return {"motion": motion, "options": options, "tallies": tallies, "winner": options[0] if options else None}
