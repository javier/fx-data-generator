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
        this.symbol = symbol;
        this.precision = precision;
        this.pip = pip;
        this.rank = rank;
        this.brackets = new Brackets(new long[]{Long.MIN_VALUE}, new double[]{low}, new double[]{high});
    }

    /** Yahoo ticker for an FX pair is the symbol with an "=X" suffix, e.g. EURUSD=X. */
    public String yahooTicker() {
        return symbol + "=X";
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
