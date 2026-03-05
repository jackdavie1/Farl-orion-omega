def guardian_gate(seed):
    # Rule: Block negative irreversibility. Approve positive windfalls.
    if seed.get('valence') == 'negative' and seed.get('irreversible'):
        return False, "VETO: Destructive Irreversibility"
    return True, "APPROVED"
