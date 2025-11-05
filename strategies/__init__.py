from importlib import import_module

REGISTRY = {
    "A2_breakout": "strategies.breakout_impulse:Strategy",
    "A1_trend":    "strategies.trend_pullback:Strategy",
    "A7_canary":   "strategies.momentum_scalp:Strategy",
    "A0_manual":   "strategies.momentum_scalp:Strategy"  # placeholder
}
