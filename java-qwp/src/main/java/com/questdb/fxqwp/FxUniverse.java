package com.questdb.fxqwp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The simulated FX market: the pair universe, the ECN/counterparty pools, and
 * the price-continuity model.
 *
 * <p>This is the simplified sibling of the Python generator. There is no order
 * book and no {@code market_data}/{@code core_price} table: trades are sampled
 * directly off an evolving mid/spread. The continuity guarantees that matter for
 * downstream OHLC still hold, because the mid follows the same controlled random
 * walk and {@code close(second t) == open(second t+1)}.
 */
public final class FxUniverse {

    /** Venues a trade can print on (matches the Python {@code ecn_pool}). */
    public static final String[] ECNS = {"LMAX", "EBS", "Hotspot", "Currenex"};

    private FxUniverse() {
    }

    /** The 30-pair default universe, identical to {@code FX_PAIRS} in the Python generator. */
    public static List<FxPair> defaultPairs() {
        List<FxPair> p = new ArrayList<>();
        // (symbol, low, high, precision, pip, rank)
        p.add(new FxPair("EURUSD", 1.05, 1.10, 5, 0.0001, 1));
        p.add(new FxPair("USDJPY", 150.0, 155.0, 3, 0.01, 2));
        p.add(new FxPair("GBPUSD", 1.25, 1.30, 5, 0.0001, 3));
        p.add(new FxPair("USDCHF", 0.90, 0.95, 4, 0.0001, 4));
        p.add(new FxPair("AUDUSD", 0.65, 0.70, 5, 0.0001, 5));
        p.add(new FxPair("USDCAD", 1.35, 1.40, 5, 0.0001, 6));
        p.add(new FxPair("EURGBP", 0.85, 0.88, 5, 0.0001, 7));
        p.add(new FxPair("EURJPY", 160.0, 165.0, 3, 0.01, 8));
        p.add(new FxPair("NZDUSD", 0.60, 0.65, 5, 0.0001, 9));
        p.add(new FxPair("GBPJPY", 180.0, 185.0, 3, 0.01, 10));
        p.add(new FxPair("EURCHF", 0.95, 1.00, 4, 0.0001, 10));
        p.add(new FxPair("EURAUD", 1.55, 1.60, 5, 0.0001, 10));
        p.add(new FxPair("GBPCHF", 1.10, 1.15, 4, 0.0001, 10));
        p.add(new FxPair("AUDJPY", 100.0, 105.0, 3, 0.01, 10));
        p.add(new FxPair("NZDJPY", 95.0, 100.0, 3, 0.01, 10));
        p.add(new FxPair("USDSEK", 10.0, 11.0, 4, 0.0001, 10));
        p.add(new FxPair("USDNOK", 10.0, 11.0, 4, 0.0001, 10));
        p.add(new FxPair("USDMXN", 17.0, 18.0, 4, 0.0001, 10));
        p.add(new FxPair("USDSGD", 1.35, 1.40, 5, 0.0001, 10));
        p.add(new FxPair("USDHKD", 7.75, 7.85, 4, 0.0001, 10));
        p.add(new FxPair("USDZAR", 18.0, 19.0, 4, 0.0001, 10));
        p.add(new FxPair("USDTRY", 27.0, 28.0, 4, 0.0001, 10));
        p.add(new FxPair("EURCAD", 1.45, 1.50, 5, 0.0001, 10));
        p.add(new FxPair("EURNZD", 1.70, 1.75, 5, 0.0001, 10));
        p.add(new FxPair("GBPAUD", 1.85, 1.90, 5, 0.0001, 10));
        p.add(new FxPair("GBPNZD", 2.00, 2.05, 5, 0.0001, 10));
        p.add(new FxPair("AUDCAD", 0.85, 0.90, 5, 0.0001, 10));
        p.add(new FxPair("AUDNZD", 1.05, 1.10, 5, 0.0001, 10));
        p.add(new FxPair("NZDCAD", 0.80, 0.85, 5, 0.0001, 10));
        p.add(new FxPair("CADJPY", 110.0, 115.0, 3, 0.01, 10));
        return p;
    }

    /**
     * Deterministic fake LEIs (20 alphanumeric chars), matching the Python
     * {@code generate_lei_pool}: "00" prefix + first 18 hex chars of
     * SHA-256("LEI_SEED_%010d"), upper-cased.
     */
    public static String[] generateLeiPool(int count) {
        String[] leis = new String[count];
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < count; i++) {
                md.reset();
                byte[] digest = md.digest(String.format("LEI_SEED_%010d", i).getBytes(StandardCharsets.UTF_8));
                StringBuilder hex = new StringBuilder(40);
                for (byte b : digest) {
                    hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                    hex.append(Character.forDigit(b & 0xF, 16));
                }
                leis[i] = "00" + hex.substring(0, 18).toUpperCase();
            }
        } catch (Exception e) {
            // SHA-256 is always available; fall back to a trivial scheme just in case.
            for (int i = 0; i < count; i++) {
                leis[i] = String.format("00%018d", i);
            }
        }
        return leis;
    }

    /**
     * Log-normal trade size: many small clips, few large ones.
     * Matches the Python {@code generate_trade_size_lognormal} defaults.
     */
    public static double tradeSizeLognormal() {
        double mu = Math.log(500_000.0);
        double sigma = 1.2;
        double size = Math.exp(mu + sigma * ThreadLocalRandom.current().nextGaussian());
        return Math.max(100_000.0, Math.min(100_000_000.0, Math.round(size)));
    }

    /** Round a price to the nearest pip and trim float noise to the pair's precision. */
    public static double quantizeToPip(double price, double pip, int precision) {
        double snapped = Math.round(price / pip) * pip;
        double scale = Math.pow(10, precision);
        return Math.round(snapped * scale) / scale;
    }

    /**
     * Controlled mid-price random walk: small drift each tick, with a rare shock.
     * Clamped to the pair's [low, high] bracket. Mirrors {@code evolve_mid_price}.
     */
    public static double evolveMid(double prevMid, FxPair pair, double driftPips) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        double change = rnd.nextDouble(-driftPips * pair.pip, driftPips * pair.pip);
        if (rnd.nextDouble() < 0.010) {
            change += rnd.nextDouble(-20 * pair.pip, 20 * pair.pip);
        }
        double newMid = prevMid + change;
        newMid = Math.max(pair.low, Math.min(pair.high, newMid));
        return quantizeToPip(newMid, pair.pip, pair.precision);
    }

    /**
     * Spread random walk expressed in whole pips, mostly flat with rare widening.
     * Kept within 1..8 pips. Mirrors the spread logic in {@code evolve_state_one_tick}.
     */
    public static int evolveSpreadPips(int prevSpreadPips) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int[] nudge = {-1, 0, 0, 0, 1};
        int sp = prevSpreadPips + nudge[rnd.nextInt(nudge.length)];
        if (rnd.nextDouble() < 0.0005) {
            sp += rnd.nextInt(3, 11); // 3..10
        }
        return Math.max(1, Math.min(8, sp));
    }
}
