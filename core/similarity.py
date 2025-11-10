#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/similarity.py — minimal cosine similarity for sparse dict vectors
"""

from __future__ import annotations
import math
from typing import Dict, Iterable, Tuple, List

def cosine(a: Dict[str, float], b: Dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    # dot
    dot = 0.0
    for k, v in a.items():
        bv = b.get(k)
        if bv is not None:
            dot += v * bv
    # norms
    na = math.sqrt(sum(v*v for v in a.values()))
    nb = math.sqrt(sum(v*v for v in b.values()))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na*nb)

def top_k(query: Dict[str, float],
          corpus: Iterable[Tuple[str, Dict[str, float]]],
          k: int = 5) -> List[Tuple[str, float]]:
    scored = []
    for key, vec in corpus:
        scored.append((key, cosine(query, vec)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:max(0, k)]
