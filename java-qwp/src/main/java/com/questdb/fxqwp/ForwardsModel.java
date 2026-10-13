package com.questdb.fxqwp;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Forward point ladders over spot at standard tenors ({@code --forwards}), a port of the Python
 * {@code ForwardsModel} with the same curves, conventions, cadence and planted event.
 *
 * <p>Points come from covered interest parity on per-currency short-rate curves (lease-rate
 * curves for the metals) plus a slow random walk, in pips. ON and TN follow market convention:
 * they hold the points of the short swaps today->tomorrow and tomorrow->spot as quoted, their
 * value date is today / tomorrow and their {@code tenor_days} is negative, so the outright for
 * those dates is spot MINUS the cumulative points. Crosses (EURJPY, XAUEUR) are derived from
 * their legs so the triangular consistency check holds by construction, except for the one
 * planted bad point. Every ladder records the spot it was priced on as {@code spot_ref}.
 *
 * <p>One full ladder per covered pair every 5 seconds: each pair has a fixed phase in the cycle
 * plus a random jitter, kept inside the cycle so the 1-minute view always pairs a cross with its
 * legs from the same rate step. The rate curves step once per cycle. Used by a single worker, so
 * it is not thread-safe.
 */
public final class ForwardsModel {

    public static final String[] TENORS = {"ON", "TN", "1W", "2W", "1M", "2M", "3M", "6M", "9M", "1Y"};
    public static final long CYCLE_NS = 5_000_000_000L;
    static final long JITTER_NS = 400_000_000L;
    public static final long DAY_NS = 86_400_000_000_000L;
    public static final String SOURCE = "FWD_FEED";

    static final double BAD_POINT_PIPS = 6.0;
    static final String BAD_POINT_TENOR = "3M";
    static final int BAD_POINT_MINUTES = 20;

    /** Half the bid/ask spread in pips per tenor for the majors; scaled per pair below. */
    static final double[] HALF_SPREAD_PIPS = {0.03, 0.03, 0.05, 0.08, 0.12, 0.20, 0.25, 0.50, 0.75, 1.00};
    static final Map<String, Double> SPREAD_MULT = Map.of(
            "EURJPY", 1.5, "XAUUSD", 50.0, "XAGUSD", 15.0, "XPTUSD", 100.0, "XPDUSD", 200.0, "XAUEUR", 60.0);

    /** Covered pairs as {symbol, base, quote}. */
    static final String[][] MAJORS = {
            {"EURUSD", "EUR", "USD"}, {"USDJPY", "USD", "JPY"}, {"GBPUSD", "GBP", "USD"},
            {"USDCHF", "USD", "CHF"}, {"AUDUSD", "AUD", "USD"}, {"USDCAD", "USD", "CAD"},
            {"NZDUSD", "NZD", "USD"}, {"EURJPY", "EUR", "JPY"}};
    static final String[][] METALS = {
            {"XAUUSD", "XAU", "USD"}, {"XAGUSD", "XAG", "USD"}, {"XPTUSD", "XPT", "USD"},
            {"XPDUSD", "XPD", "USD"}, {"XAUEUR", "XAU", "EUR"}};
    /** Cross -> {leg1, leg2, op}: EURJPY = EURUSD x USDJPY, XAUEUR = XAUUSD / EURUSD. */
    static final Map<String, String[]> CROSSES = Map.of(
            "EURJPY", new String[]{"EURUSD", "USDJPY", "mul"},
            "XAUEUR", new String[]{"XAUUSD", "EURUSD", "div"});

    /**
     * Short-rate curves in percent per tenor, identical to the Python {@code RATE_CURVES} (set
     * 2026-10-13; see the sources noted there). Overnight sits at the policy rate; metals hold
     * lease rates.
     */
    static final Map<String, double[]> RATE_CURVES = Map.ofEntries(
            Map.entry("USD", new double[]{3.88, 3.88, 3.88, 3.89, 3.90, 3.92, 3.93, 3.95, 3.95, 3.94}),
            Map.entry("EUR", new double[]{2.42, 2.42, 2.43, 2.44, 2.46, 2.49, 2.52, 2.58, 2.62, 2.65}),
            Map.entry("GBP", new double[]{3.73, 3.73, 3.72, 3.71, 3.70, 3.68, 3.66, 3.61, 3.57, 3.54}),
            Map.entry("JPY", new double[]{1.23, 1.23, 1.23, 1.24, 1.25, 1.26, 1.27, 1.30, 1.33, 1.35}),
            Map.entry("CHF", new double[]{-0.05, -0.05, -0.04, -0.04, -0.03, -0.02, -0.01, 0.01, 0.03, 0.05}),
            Map.entry("AUD", new double[]{4.58, 4.58, 4.59, 4.60, 4.61, 4.62, 4.63, 4.66, 4.68, 4.70}),
            Map.entry("CAD", new double[]{2.24, 2.24, 2.24, 2.23, 2.22, 2.21, 2.20, 2.18, 2.16, 2.15}),
            Map.entry("NZD", new double[]{2.73, 2.73, 2.74, 2.75, 2.77, 2.80, 2.83, 2.88, 2.92, 2.95}),
            Map.entry("XAU", new double[]{0.05, 0.05, 0.08, 0.10, 0.15, 0.25, 0.35, 0.55, 0.70, 0.85}),
            Map.entry("XAG", new double[]{6.20, 6.10, 6.00, 5.90, 5.70, 5.20, 4.80, 3.90, 3.30, 2.90}),
            Map.entry("XPT", new double[]{13.00, 13.00, 12.80, 12.60, 12.50, 11.50, 10.50, 8.00, 6.50, 5.50}),
            Map.entry("XPD", new double[]{6.00, 6.00, 5.90, 5.80, 5.60, 5.30, 5.00, 4.60, 4.30, 4.00}));
    /** Day count basis: Act/365 for GBP, Act/360 for every other currency and for lease rates. */
    static final Map<String, Double> DAY_COUNT = Map.of("GBP", 365.0);
    /** Random walk in basis points: daily vol of the parallel shift, stationary sd of tenor noise. */
    static final Map<String, Double> SHIFT_DAILY_BP = Map.of("XAU", 2.0, "XAG", 8.0, "XPT", 10.0, "XPD", 15.0);
    static final Map<String, Double> TENOR_NOISE_BP = Map.of("XAU", 1.0, "XAG", 3.0, "XPT", 5.0, "XPD", 8.0);

    /** One planned ladder. */
    public static final class Emission {
        public final long ts;
        public final String symbol;
        public final double[] premium;      // F/S - 1 per tenor
        public final long[] valueDateNs;    // midnight UTC per tenor
        public final int[] tenorDays;
        public final boolean bad;

        Emission(long ts, String symbol, double[] premium, long[] valueDateNs, int[] tenorDays, boolean bad) {
            this.ts = ts;
            this.symbol = symbol;
            this.premium = premium;
            this.valueDateNs = valueDateNs;
            this.tenorDays = tenorDays;
            this.bad = bad;
        }
    }

    private static final class Dates {
        final long[] valueDateNs = new long[TENORS.length];
        final int[] tenorDays = new int[TENORS.length];
        final double[] accrualDays = new double[TENORS.length];
    }

    private final List<String> symbols = new ArrayList<>();
    private final Map<String, String[]> legs = new HashMap<>();
    private final List<String> ccys = new ArrayList<>();
    private final Map<String, Double> shift = new HashMap<>();
    private final Map<String, double[]> noise = new HashMap<>();
    private final Map<String, Long> phase = new HashMap<>();
    private final SplittableRandom rng;
    private final double shiftTheta;
    private final double noiseTheta;
    private final Map<String, Double> shiftSigma = new HashMap<>();
    private final Map<String, Double> noiseSigma = new HashMap<>();
    private final Map<Long, Dates> datesCache = new LinkedHashMap<>();
    private final List<Emission> pending = new ArrayList<>();
    private long cycle;
    public final String badSymbol;
    private final int badTenorIdx = Arrays.asList(TENORS).indexOf(BAD_POINT_TENOR);
    private final long badFrom;
    private final long badTo;

    public ForwardsModel(boolean metals, SplittableRandom rng, long startNs, long badPointNsOfDay) {
        List<String[]> pairs = new ArrayList<>(Arrays.asList(MAJORS));
        if (metals) {
            pairs.addAll(Arrays.asList(METALS));
        }
        for (String[] p : pairs) {
            symbols.add(p[0]);
            legs.put(p[0], new String[]{p[1], p[2]});
            for (String c : new String[]{p[1], p[2]}) {
                if (!ccys.contains(c)) {
                    ccys.add(c);
                }
            }
        }
        ccys.sort(String::compareTo);
        this.rng = rng;
        long cyclesPerDay = DAY_NS / CYCLE_NS;
        this.shiftTheta = 1.0 / (7 * cyclesPerDay);
        this.noiseTheta = 1.0 / (3600L * 1_000_000_000L / CYCLE_NS);
        for (String c : ccys) {
            shift.put(c, 0.0);
            noise.put(c, new double[TENORS.length]);
            shiftSigma.put(c, SHIFT_DAILY_BP.getOrDefault(c, 3.0) / Math.sqrt(cyclesPerDay));
            noiseSigma.put(c, TENOR_NOISE_BP.getOrDefault(c, 0.5) * Math.sqrt(2 * noiseTheta));
        }
        int n = symbols.size();
        for (int k = 0; k < n; k++) {
            phase.put(symbols.get(k), k * (CYCLE_NS - JITTER_NS) / n);
        }
        this.cycle = Math.floorDiv(startNs + CYCLE_NS - 1, CYCLE_NS);   // first cycle at or after start
        this.badSymbol = metals ? "XAUEUR" : "EURJPY";
        this.badFrom = badPointNsOfDay;
        this.badTo = badPointNsOfDay + BAD_POINT_MINUTES * 60L * 1_000_000_000L;
    }

    public List<String> symbols() {
        return symbols;
    }

    private void stepRates() {
        for (String c : ccys) {
            shift.put(c, shift.get(c) * (1 - shiftTheta) + shiftSigma.get(c) * rng.nextGaussian());
            double[] nz = noise.get(c);
            double s = noiseSigma.get(c);
            for (int t = 0; t < nz.length; t++) {
                nz[t] = nz[t] * (1 - noiseTheta) + s * rng.nextGaussian();
            }
        }
    }

    /** Percent per tenor: the static curve plus the walk (kept in basis points). */
    double rate(String ccy, int tenor) {
        return RATE_CURVES.get(ccy)[tenor] + (shift.get(ccy) + noise.get(ccy)[tenor]) / 100.0;
    }

    private double factor(String symbol, int tenor, double accrualDays) {
        String[] bq = legs.get(symbol);
        double rb = rate(bq[0], tenor) / 100.0;
        double rq = rate(bq[1], tenor) / 100.0;
        return (1 + rq * accrualDays / DAY_COUNT.getOrDefault(bq[1], 360.0))
                / (1 + rb * accrualDays / DAY_COUNT.getOrDefault(bq[0], 360.0));
    }

    /** Forward premium F/S - 1 per tenor (for ON and TN, over their own short period). */
    double[] premium(String symbol, double[] accrualDays) {
        double[] out = new double[TENORS.length];
        String[] cross = CROSSES.get(symbol);
        for (int t = 0; t < TENORS.length; t++) {
            if (cross == null) {
                out[t] = factor(symbol, t, accrualDays[t]) - 1;
            } else {
                double f1 = factor(cross[0], t, accrualDays[t]);
                double f2 = factor(cross[1], t, accrualDays[t]);
                out[t] = ("mul".equals(cross[2]) ? f1 * f2 : f1 / f2) - 1;
            }
        }
        return out;
    }

    boolean isBad(String symbol, long ts) {
        if (!symbol.equals(badSymbol)) {
            return false;
        }
        long tod = Math.floorMod(ts, DAY_NS);
        // A window starting late in the day wraps past midnight.
        return (badFrom <= tod && tod < badTo) || tod < badTo - DAY_NS;
    }

    /**
     * Ladders due in [t0, t1), sorted by timestamp. Cycles are processed whole and in order, so
     * the result does not depend on how the caller slices time. Ladders before t0 (e.g. after a
     * stall) are dropped, but their cycles still step the rate walk.
     */
    public List<Emission> emissions(long t0, long t1) {
        while (cycle * CYCLE_NS < t1) {
            long c0 = cycle * CYCLE_NS;
            stepRates();
            for (String s : symbols) {
                long ts = c0 + phase.get(s) + (long) (rng.nextDouble() * JITTER_NS);
                Dates d = datesFor(Math.floorDiv(ts, DAY_NS));
                pending.add(new Emission(ts, s, premium(s, d.accrualDays), d.valueDateNs, d.tenorDays, isBad(s, ts)));
            }
            cycle++;
        }
        pending.sort((a, b) -> Long.compare(a.ts, b.ts));
        List<Emission> out = new ArrayList<>();
        int k = 0;
        while (k < pending.size() && pending.get(k).ts < t1) {
            Emission e = pending.get(k++);
            if (e.ts >= t0) {
                out.add(e);
            }
        }
        pending.subList(0, k).clear();
        return out;
    }

    /** Points per tenor at the given spot, with the planted shift applied when the ladder is bad. */
    public double[] midPoints(Emission e, double spotRef, double pip) {
        double[] mid = new double[TENORS.length];
        for (int t = 0; t < mid.length; t++) {
            mid[t] = spotRef * e.premium[t] / pip;
        }
        if (e.bad) {
            mid[badTenorIdx] += BAD_POINT_PIPS;
        }
        return mid;
    }

    public static double halfSpread(String symbol, int tenor) {
        return HALF_SPREAD_PIPS[tenor] * SPREAD_MULT.getOrDefault(symbol, 1.0);
    }

    // ---------------------------------------------------------------- value dates

    private Dates datesFor(long dayIdx) {
        Dates d = datesCache.get(dayIdx);
        if (d == null) {
            if (datesCache.size() > 8) {
                datesCache.clear();
            }
            d = valueDates(LocalDate.ofEpochDay(dayIdx));
            datesCache.put(dayIdx, d);
        }
        return d;
    }

    private static boolean weekend(LocalDate d) {
        return d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY;
    }

    private static LocalDate nextBusinessDay(LocalDate d) {
        while (weekend(d)) {
            d = d.plusDays(1);
        }
        return d;
    }

    private static LocalDate addBusinessDays(LocalDate d, int n) {
        for (int i = 0; i < n; i++) {
            d = nextBusinessDay(d.plusDays(1));
        }
        return d;
    }

    private static LocalDate lastBusinessDayOfMonth(int year, int month) {
        LocalDate d = YearMonth.of(year, month).atEndOfMonth();
        while (weekend(d)) {
            d = d.minusDays(1);
        }
        return d;
    }

    private static LocalDate modifiedFollowing(LocalDate d) {
        LocalDate f = nextBusinessDay(d);
        if (f.getMonthValue() != d.getMonthValue()) {
            f = d;
            while (weekend(f)) {
                f = f.minusDays(1);
            }
        }
        return f;
    }

    /** Whole-month tenor from spot, with the end-of-month rule and modified following. */
    private static LocalDate addMonthsEom(LocalDate spot, int months) {
        YearMonth target = YearMonth.from(spot).plusMonths(months);
        if (spot.equals(lastBusinessDayOfMonth(spot.getYear(), spot.getMonthValue()))) {
            return lastBusinessDayOfMonth(target.getYear(), target.getMonthValue());
        }
        return modifiedFollowing(target.atDay(Math.min(spot.getDayOfMonth(), target.lengthOfMonth())));
    }

    /**
     * Value dates on a UTC trade date (weekday calendar, no holidays), as the Python
     * {@code forward_value_dates}: spot is T+2 business days, a weekend trade date rolls to
     * Monday, tenor_days counts calendar days from spot (negative for ON and TN), and accrual is
     * the period the points cover.
     */
    static Dates valueDates(LocalDate today) {
        LocalDate td = nextBusinessDay(today);
        LocalDate tom = addBusinessDays(td, 1);
        LocalDate spot = addBusinessDays(td, 2);
        LocalDate[] vd = new LocalDate[TENORS.length];
        double[] accrual = new double[TENORS.length];
        vd[0] = td;
        accrual[0] = ChronoUnit.DAYS.between(td, tom);
        vd[1] = tom;
        accrual[1] = ChronoUnit.DAYS.between(tom, spot);
        vd[2] = modifiedFollowing(spot.plusDays(7));
        vd[3] = modifiedFollowing(spot.plusDays(14));
        int[] months = {1, 2, 3, 6, 9, 12};
        for (int m = 0; m < months.length; m++) {
            vd[4 + m] = addMonthsEom(spot, months[m]);
        }
        Dates d = new Dates();
        for (int t = 0; t < TENORS.length; t++) {
            d.valueDateNs[t] = vd[t].toEpochDay() * DAY_NS;
            d.tenorDays[t] = (int) ChronoUnit.DAYS.between(spot, vd[t]);
            d.accrualDays[t] = t < 2 ? accrual[t] : d.tenorDays[t];
        }
        return d;
    }

    /** Parse --bad_point_utc (HH:MM, UTC) to nanoseconds into the day. */
    public static long parseBadPointUtc(String s) {
        String[] hm = s.split(":");
        if (hm.length != 2) {
            throw new IllegalArgumentException("--bad_point_utc must be HH:MM in UTC, got '" + s + "'");
        }
        int hh = Integer.parseInt(hm[0]);
        int mm = Integer.parseInt(hm[1]);
        if (hh < 0 || hh > 23 || mm < 0 || mm > 59) {
            throw new IllegalArgumentException("--bad_point_utc must be HH:MM in UTC, got '" + s + "'");
        }
        return (hh * 60L + mm) * 60L * 1_000_000_000L;
    }
}
