from pydantic import BaseModel

class Seed(BaseModel):
    text: str
    category: str
    valence: str
    irreversible: bool
    risk_score: float
