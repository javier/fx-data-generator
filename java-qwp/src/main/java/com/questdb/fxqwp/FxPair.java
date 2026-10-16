package com.questdb.fxqwp;

import java.util.Arrays;

/**
 * One FX pair plus the bracket the price walk is allowed to roam within.
 *
 * <p><b>Bracket timeline.</b> The bracket can change over a run (the real-time
 * Yahoo refresher recentres it on the live mid). To keep the price walk identical
 * across the two worker pools, the bracket is not a single mutable value but a
 * small append-only timeline of {@code (effSec, low, high)} entries: the bracket
 * "as of data-second k" is the last entry with {@code effSec <= k}. Both pools
 * resolve the same bracket for the same second regardless of wall-clock timing, so
 * the clamp — and therefore the whole deterministic walk — stays consistent
 * between {@code qwp_trades} and {@code qwp_market_data}. The refresher stamps new
 * entries a few seconds in the future so every worker adopts them at the same
 * data-second.
 */
public final class FxPair {
    public final String symbol;
    public final int precision;
    public final double pip;
    /** Liquidity rank: 1 = most liquid, 10 = least. Drives trade-weighting. */
    public final int rank;

    // Market character. FX pairs keep the defaults (the original model exactly); the precious
    // metals override them, mirroring METAL_PARAMS in the Python generator.
    /** Multiplier on the per-second mid random walk and shocks, in pips. */
    public final double move;
    /** Probability of a shock in a given second. */
    public final double shockProb;
    /** Spread walk bounds in whole pips. */
    public final int spreadMinPips;
    public final int spreadMaxPips;
    /** Pips between consecutive order-book levels. */
    public final int levelPips;
    /** Scale on the volume ladder (metals: troy ounces). */
    public final double volScale;
    /** Scale on the trade size distribution (metals: troy ounces). */
    public final double sizeScale;
    /** Yahoo ticker, or null when the bracket is derived (see {@link #derivedNum}). */
    public final String yahoo;
    /** Derived bracket: mid = derivedNum / derivedDen (e.g. XAUEUR = XAUUSD / EURUSD), else null. */
    public final String derivedNum;
    public final String derivedDen;
    /** True when this pair overrides the FX defaults. */
    public final boolean metal;

    /** Immutable bracket timeline snapshot (parallel arrays, sorted by effSec ascending). */
    private static final class Brackets {
        final long[] effSec;
        final double[] low;
        final double[] high;

        Brackets(long[] effSec, double[] low, double[] high) {
            this.effSec = effSec;
            this.low = low;
            this.high = high;
        }
    }

    private volatile Brackets brackets;

    public FxPair(String symbol, double low, double high, int precision, double pip, int rank) {
        this(symbol, low, high, precision, pip, rank, 1.0, 0.010, 1, 8, 1, 1.0, 1.0,
                symbol + "=X", null, null, false);
    }

    /** A precious metal: the FX model with its own scales (see FxUniverse.metalPairs). */
    public static FxPair metal(String symbol, double low, double high, int precision, double pip, int rank,
                               double move, double shockProb, int spreadMinPips, int spreadMaxPips,
                               int levelPips, double volScale, double sizeScale,
                               String yahoo, String derivedNum, String derivedDen) {
        return new FxPair(symbol, low, high, precision, pip, rank, move, shockProb, spreadMinPips,
                spreadMaxPips, levelPips, volScale, sizeScale, yahoo, derivedNum, derivedDen, true);
    }

    private FxPair(String symbol, double low, double high, int precision, double pip, int rank,
                   double move, double shockProb, int spreadMinPips, int spreadMaxPips, int levelPips,
                   double volScale, double sizeScale, String yahoo, String derivedNum, String derivedDen,
                   boolean metal) {
        this.symbol = symbol;
        this.precision = precision;
        this.pip = pip;
        this.rank = rank;
        this.move = move;
        this.shockProb = shockProb;
        this.spreadMinPips = spreadMinPips;
        this.spreadMaxPips = spreadMaxPips;
        this.levelPips = levelPips;
        this.volScale = volScale;
        this.sizeScale = sizeScale;
        this.yahoo = yahoo;
        this.derivedNum = derivedNum;
        this.derivedDen = derivedDen;
        this.metal = metal;
        this.brackets = new Brackets(new long[]{Long.MIN_VALUE}, new double[]{low}, new double[]{high});
    }

    /**
     * Yahoo ticker: the symbol with an "=X" suffix for FX (e.g. EURUSD=X); the front-month
     * future for a metal (the XAUUSD=X style spot tickers do not resolve); null when derived.
     */
    public String yahooTicker() {
        return yahoo;
    }

    /** Price step between book levels: one pip for FX, {@link #levelPips} pips for a metal. */
    public double tick() {
        return levelPips * pip;
    }

    /**
     * Spread walk step in pips: 1 for FX, about an eighth of the band for a metal. Math.rint
     * rounds half to even like Python's round(), so gold's 2.5 gives 2 on both engines.
     */
    public int spreadStepPips() {
        return Math.max(1, (int) Math.rint((spreadMaxPips - spreadMinPips) / 8.0));
    }

    private static int indexAt(Brackets b, long sec) {
        int lo = 0, hi = b.effSec.length - 1, res = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (b.effSec[mid] <= sec) {
                res = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return res;
    }

    public double lowAt(long sec) {
        Brackets b = brackets;
        return b.low[indexAt(b, sec)];
    }

    public double highAt(long sec) {
        Brackets b = brackets;
        return b.high[indexAt(b, sec)];
    }

    public double midOfBracketAt(long sec) {
        Brackets b = brackets;
        int i = indexAt(b, sec);
        return (b.low[i] + b.high[i]) / 2.0;
    }

    /** Replace the whole timeline with a single bracket effective from the start (setup only). */
    public void resetBracket(double low, double high) {
        this.brackets = new Brackets(new long[]{Long.MIN_VALUE}, new double[]{low}, new double[]{high});
    }

    /**
     * Append a bracket that takes effect at data-second {@code effSec}. Stamp it in
     * the future so every worker adopts it at the same second. Append-only; ignored
     * if not strictly later than the last entry.
     */
    public synchronized void appendBracket(long effSec, double low, double high) {
        Brackets b = brackets;
        int n = b.effSec.length;
        if (effSec <= b.effSec[n - 1]) {
            return;
        }
        long[] e = Arrays.copyOf(b.effSec, n + 1);
        double[] l = Arrays.copyOf(b.low, n + 1);
        double[] h = Arrays.copyOf(b.high, n + 1);
        e[n] = effSec;
        l[n] = low;
        h[n] = high;
        this.brackets = new Brackets(e, l, h);
    }
}
