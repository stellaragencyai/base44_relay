class Strategy:
    """
    Breakout-Impulse placeholder.
    generate_signals should read klines and tick to determine breakouts.
    For now it emits a NOOP to prove wiring.
    """
    def required_feeds(self) -> set[str]:
        return {"tickers","klines"}

    def generate_signals(self, klines=None, tick=None, pos=None, params=None):
        # TODO: implement real breakout logic
        return []  # e.g., [{"type":"LONG_BREAKOUT","strength":0.6,"symbol":"PUMPFUNUSDT"}]
