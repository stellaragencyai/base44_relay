class Strategy:
    """
    Momentum-Scalp placeholder used for canary/manual for now.
    """
    def required_feeds(self) -> set[str]:
        return {"tickers"}

    def generate_signals(self, klines=None, tick=None, pos=None, params=None):
        return []
