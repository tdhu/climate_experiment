"""Risk ratio calculations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ProbabilityEstimate:
    probability: float


def compute_risk_ratio(all_prob: float, nat_prob: float, floor: float = 1e-6) -> float:
    """Compute risk ratio with a small floor to avoid division by zero."""
    nat_safe = max(nat_prob, floor)
    return float(all_prob / nat_safe)
