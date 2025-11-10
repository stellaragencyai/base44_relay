#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/setup_ranker.py — online ranking of tagged setups

- Maintains per-setup prior (win-rate, avg R) and per-feature linear weights.
- Online updates on verdicts (pnl_r, win flag) with forgetting factor.
- Scores a feature dict into [-1, 1] using setup priors + feature weights.

Storage:
  logs/models/setup_ranker.json

Public:
  ranker = SetupRanker()
  score = ranker.score(features)               # features must include 'setup_tag'
  ranker.update(features, pnl_r, won)          # call after a trade closes
  ranker.save(), ranker.load()
"""

from __future__ import annotations
import json, math, time
from pathlib import Path
from typing import Dict, Any, Tuple

ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "logs" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = MODEL_DIR / "setup_ranker.json"

def _clip(x: float, a: float, b: float) -> float:
    return a if x < a else b if x > b else x

def _safelogit(p: float) -> float:
    p = _clip(p, 1e-4, 1-1e-4)
    return math.log(p/(1-p))

def _sigmoid(z: float) -> float:
    try:
        return 1.0/(1.0+math.exp(-z))
    except Exception:
        return 0.5

def _norm_num(x: Any) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return 0.0
        return v
    except Exception:
        return 0.0

def _extract_numeric(feats: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, v in feats.items():
        if k in ("setup_tag", "symbol", "link", "account", "timestamp_ms"):
            continue
        val = _norm_num(v)
        if val != 0.0:
            out[k] = val
    return out

class SetupRanker:
    def __init__(self,
                 lr_prior: float = 0.08,
                 lr_weights: float = 0.04,
                 decay: float = 0.995):
        self.lr_prior = float(lr_prior)
        self.lr_weights = float(lr_weights)
        self.decay = float(decay)

        # priors per setup: win_p, avg_r
        self.priors: Dict[str, Dict[str, float]] = {}
        # linear weights per setup: feature -> weight
        self.weights: Dict[str, Dict[str, float]] = {}
        # last updated
        self.meta = {"created": int(time.time()*1000), "updated": 0}

    # ---------- persistence ----------
    def save(self, path: Path = MODEL_PATH) -> None:
        obj = {
            "priors": self.priors,
            "weights": self.weights,
            "meta": self.meta,
            "lr_prior": self.lr_prior,
            "lr_weights": self.lr_weights,
            "decay": self.decay,
        }
        try:
            path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def load(self, path: Path = MODEL_PATH) -> None:
        if not path.exists():
            return
        try:
            js = json.loads(path.read_text(encoding="utf-8"))
            self.priors = js.get("priors", {})
            self.weights = js.get("weights", {})
            self.meta = js.get("meta", self.meta)
            self.lr_prior = float(js.get("lr_prior", self.lr_prior))
            self.lr_weights = float(js.get("lr_weights", self.lr_weights))
            self.decay = float(js.get("decay", self.decay))
        except Exception:
            pass

    # ---------- init helpers ----------
    def _ensure_setup(self, setup: str) -> None:
        if setup not in self.priors:
            self.priors[setup] = {"win_p": 0.55, "avg_r": 0.20}
        if setup not in self.weights:
            self.weights[setup] = {}

    # ---------- scoring ----------
    def score(self, features: Dict[str, Any]) -> float:
        """
        Blend prior quality of the setup with linear feature model.
        Returns a score in [-1, 1].
        """
        setup = str(features.get("setup_tag") or "Unclassified")
        self._ensure_setup(setup)
        x = _extract_numeric(features)

        # linear margin
        w = self.weights.get(setup, {})
        margin = 0.0
        for k, v in x.items():
            margin += w.get(k, 0.0) * v

        # convert margin to pseudo win-prob
        p_feat = _sigmoid(margin)

        # prior from historical performance
        prior = self.priors[setup]
        p_prior = _clip(prior.get("win_p", 0.55), 0.05, 0.95)
        # harmonic-ish blend in logit space
        p_blend = _sigmoid(0.5*_safelogit(p_prior) + 0.5*_safelogit(p_feat))

        # scale by avg expected R (map 0..0.5R to 0..1)
        avg_r = _clip(prior.get("avg_r", 0.2), -1.0, 1.0)
        scale = _clip((avg_r + 0.5) / 1.0, 0.0, 1.0)  # -0.5R->0, +0.5R->1

        raw = (p_blend*2.0 - 1.0) * scale
        return _clip(raw, -1.0, 1.0)

    # ---------- online update ----------
    def update(self, features: Dict[str, Any], pnl_r: float, won: bool) -> None:
        """
        Update priors and feature weights from outcome:
          pnl_r: realized R multiple (e.g. +0.8, -0.3)
          won  : True/False
        """
        setup = str(features.get("setup_tag") or "Unclassified")
        self._ensure_setup(setup)
        x = _extract_numeric(features)

        # decay old info
        #  - priors: n/a directly, we do nudged moving averages
        #  - weights: shrink
        w = self.weights[setup]
        if self.decay < 0.9999:
            for k in list(w.keys()):
                w[k] *= self.decay
                if abs(w[k]) < 1e-9:
                    del w[k]

        # update priors
        pr = self.priors[setup]
        # win rate
        pr["win_p"] = _clip(pr["win_p"] + self.lr_prior*((1.0 if won else 0.0) - pr["win_p"]), 0.05, 0.95)
        # average R (bounded)
        pnl_r = _clip(float(pnl_r), -1.5, 1.5)
        pr["avg_r"] = _clip(pr["avg_r"] + self.lr_prior*(pnl_r - pr["avg_r"]), -1.0, 1.0)

        # feature weights via logistic gradient towards won label
        target = 1.0 if won else 0.0
        margin = sum(w.get(k, 0.0)*v for k, v in x.items())
        pred = _sigmoid(margin)
        err = (target - pred)
        step = self.lr_weights * err
        for k, v in x.items():
            w[k] = _clip(w.get(k, 0.0) + step * v, -5.0, 5.0)

        self.weights[setup] = w
        self.meta["updated"] = int(time.time()*1000)
