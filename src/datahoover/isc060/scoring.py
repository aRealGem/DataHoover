"""Mean pinball score, quantile coverage, interval coverage/width, median MAE,
moving-block bootstrap (fixed seed). No CRPS or PIT claim (FA-008 Metrics)."""

from __future__ import annotations

import math
import random
from collections import defaultdict

from . import spec

BOOT_B = 1000
BOOT_SEED = 60


def pinball(q: float, y: float, qhat: float) -> float:
    d = y - qhat
    return max(q * d, (q - 1) * d)


def mean_pinball(qs: dict, y: float) -> float:
    return sum(pinball(q, y, qs[q]) for q in spec.QUANTILES) / len(spec.QUANTILES)


def overlap_origins(cell) -> int:
    """Consecutive weekly origins whose target windows overlap: h for weekly targets
    (gas_w4 spans 4 weeks), 1 for daily cells (windows end before the next origin's)."""
    return cell.h if cell.kind == "weekly_logret" else 1


def block_bootstrap_ci(xs: list[float], *, min_block: int = 1, b: int = BOOT_B,
                       seed: int = BOOT_SEED, alpha: float = 0.10) -> tuple[float, float] | None:
    """Moving-block bootstrap of the mean in origin order. Block length is predeclared:
    max(ceil(n^(1/3)), min_block), with min_block = the cell's target overlap.
    Descriptive only: with few origins it does not repair limited evidence."""
    n = len(xs)
    if n < 2:
        return None
    blk = min(n, max(1, math.ceil(n ** (1 / 3)), min_block))
    starts = n - blk + 1
    rng = random.Random(seed)
    means = []
    for _ in range(b):
        sample: list[float] = []
        while len(sample) < n:
            s = rng.randrange(starts)
            sample.extend(xs[s:s + blk])
        means.append(sum(sample[:n]) / n)
    means.sort()
    lo = means[int(math.floor(alpha / 2 * (b - 1)))]
    hi = means[int(math.ceil((1 - alpha / 2) * (b - 1)))]
    return lo, hi


def summarize(records, *, value: str = "y_first") -> list[dict]:
    """One row per (group, sealed, cell, baseline) with counts and scores."""
    buckets = defaultdict(list)
    for r in records:
        buckets[(r.group, r.sealed, r.cell_id, r.baseline)].append(r)
    rows = []
    for (group, sealed, cell, base), rs in sorted(buckets.items(), key=lambda kv: tuple(map(str, kv[0]))):
        rs.sort(key=lambda r: r.origin)
        scored = [r for r in rs if r.status == "ok" and r.maturity == "mature"
                  and getattr(r, value) is not None]
        row = {
            "group": group, "sealed": sealed, "cell_id": cell, "baseline": base, "value": value,
            "n_origins": len(rs), "n_scored": len(scored),
            "n_excluded": sum(r.status == "excluded" for r in rs),
            "n_abstain": sum(r.status == "abstain" for r in rs),
            "n_immature": sum(r.status == "ok" and r.maturity == "immature" for r in rs),
            "n_unscorable": sum(r.status == "ok" and r.maturity == "unscorable" for r in rs),
            "n_missing": sum(r.status == "ok" and r.maturity == "missing" for r in rs),
            "n_timing_uncertain": sum(r.status == "ok" and r.maturity == "timing_uncertain" for r in rs),
            "n_sealed": sum(r.status == "ok" and r.maturity == "sealed" for r in rs),
        }
        if scored:
            ys = [getattr(r, value) for r in scored]
            mp = [mean_pinball(r.quantiles, y) for r, y in zip(scored, ys)]
            row["mean_pinball"] = sum(mp) / len(mp)
            cellspec = next(c for c in spec.CELLS if c.cell_id == cell)
            row["mean_pinball_ci90_descriptive"] = block_bootstrap_ci(
                mp, min_block=overlap_origins(cellspec))
            row["pinball_by_q"] = {q: sum(pinball(q, y, r.quantiles[q]) for r, y in zip(scored, ys)) / len(ys)
                                   for q in spec.QUANTILES}
            row["frac_below_q"] = {q: sum(y < r.quantiles[q] for r, y in zip(scored, ys)) / len(ys)
                                   for q in spec.QUANTILES}
            row["interval"] = {
                lvl: {"coverage": sum(r.quantiles[lo] <= y <= r.quantiles[hi] for r, y in zip(scored, ys)) / len(ys),
                      "mean_width": sum(r.quantiles[hi] - r.quantiles[lo] for r in scored) / len(scored)}
                for lvl, (lo, hi) in spec.INTERVALS.items()}
            row["mae_median"] = sum(abs(y - r.quantiles[0.5]) for r, y in zip(scored, ys)) / len(ys)
        rows.append(row)
    return rows
