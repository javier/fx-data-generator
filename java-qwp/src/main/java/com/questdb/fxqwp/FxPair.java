package com.questdb.fxqwp;

/**
 * One FX pair plus the bracket the simulation is allowed to roam within.
 *
 * <p>{@code low}/{@code high} are mutable because in real-time mode a background
 * Yahoo refresher periodically recentres the bracket on the live mid price. They
 * are declared {@code volatile} so the single ingest worker reads a consistent
 * snapshot without locking (mirrors the Python generator's lock-free
 * {@code multiprocessing.Manager().list()} approach).
 */
public final class FxPair {
    public final String symbol;
    public final int precision;
    public final double pip;
    /** Liquidity rank: 1 = most liquid, 10 = least. Drives trade-weighting. */
    public final int rank;

    public volatile double low;
    public volatile double high;

    public FxPair(String symbol, double low, double high, int precision, double pip, int rank) {
        this.symbol = symbol;
        this.low = low;
        this.high = high;
        this.precision = precision;
        this.pip = pip;
        this.rank = rank;
    }

    /** Yahoo ticker for an FX pair is the symbol with an "=X" suffix, e.g. EURUSD=X. */
    public String yahooTicker() {
        return symbol + "=X";
    }

    public double midOfBracket() {
        return (low + high) / 2.0;
    }
}
