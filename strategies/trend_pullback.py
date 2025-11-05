class Strategy:
    """
    Trend-Pullback placeholder.
    """
    def required_feeds(self) -> set[str]:
        return {"tickers","klines"}

    def generate_signals(self, klines=None, tick=None, pos=None, params=None):
        return []
