package com.questdb.fxqwp;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Synthetic FX generator that ingests into QuestDB over QWP (WebSocket), HA-aware.
 *
 * <p>Two independent worker pools, one per table:
 * <ul>
 *   <li>{@code --trades_processes} workers → {@code qwp_trades} (trade prints), and
 *   <li>{@code --market_data_processes} workers → {@code qwp_market_data} (order books).
 * </ul>
 * Each pool snake-drafts the symbols across its own threads, so you can give a table
 * its own degree of parallelism (e.g. 1 for trades, 2 for market_data). Separate
 * tables get separate WAL writers, and fewer workers per table means less data-clock
 * divergence, so out-of-order apply pressure stays low per table.
 *
 * <p><b>Cross-table price consistency.</b> Each symbol's mid/spread walk is
 * <b>deterministic</b> (seeded by the symbol), so a trades worker and a market_data
 * worker that both own a symbol compute the identical top-of-book for the same
 * (symbol, second) — with no shared state and no coordination. Trades then
 * <b>execute against the reconstructed book</b> (walk levels), so every trade prints
 * at a real book-level price consistent with the published snapshot. Per-row noise
 * (order side/size, book volumes, ids) stays fast {@link ThreadLocalRandom}.
 */
public final class QwpTradesGenerator {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final long NANOS_PER_SEC = 1_000_000_000L;
    private static final long WALK_SEED_BASE = 0xF1A7C0DEL;
    private static final long GOLDEN = 0x9E3779B97F4A7C15L;
    // Per-second event plan packs (offset << SYM_BITS | ownedIndex) into one long so a
    // single sort orders events by offset while carrying their symbol. 6 bits => <=64
    // symbols per worker (universe is 30), offset (<1e9) fits in the remaining bits.
    private static final int SYM_BITS = 6;
    private static final long SYM_MASK = (1L << SYM_BITS) - 1;
    private static final String ENTERPRISE_POLICY =
            "TO parquet 1 hour, DROP NATIVE 2 days, DROP LOCAL 3 months";

    private enum Kind { TRADES, MARKET_DATA, CORE_PRICE }

    private final Cli cfg;
    private final List<FxPair> pairs = FxUniverse.defaultPairs();
    private final String[] leiPool;
    private final long[] ladder = FxUniverse.makeVolumeLadder();

    // Read-only initial mid per global pair index (computed once from Yahoo/template/
    // incremental). Both pools seed their deterministic walk from the same values.
    private final double[] initialMid;
    private final int totalAllWeight; // sum of (11 - rank) across all pairs

    private final AtomicBoolean running = new AtomicBoolean(true);
    // Per-pool pause flags: each table's WAL lag pauses only its own pool, so a
    // struggling market_data table never freezes the trades pool (and vice versa).
    private final AtomicBoolean pausedTrades = new AtomicBoolean(false);
    private final AtomicBoolean pausedMd = new AtomicBoolean(false);
    private final AtomicBoolean pausedCore = new AtomicBoolean(false);
    private final AtomicLong tradesRows = new AtomicLong(0);
    private final AtomicLong mdRows = new AtomicLong(0);
    private final AtomicLong coreRows = new AtomicLong(0);
    // Wall-clock ms when each pool's last worker finished — pools finish at different
    // times (the light tables cover the window first), so per-pool rate is rows over
    // that pool's own active span, not the whole run.
    private final AtomicLong tradesFinishMs = new AtomicLong(0);
    private final AtomicLong mdFinishMs = new AtomicLong(0);
    private final AtomicLong coreFinishMs = new AtomicLong(0);

    private QwpTradesGenerator(Cli cfg) {
        this.cfg = cfg;
        this.leiPool = FxUniverse.generateLeiPool(cfg.leiPoolSize);
        int n = pairs.size();
        this.initialMid = new double[n];
        int acc = 0;
        for (int i = 0; i < n; i++) {
            acc += (11 - pairs.get(i).rank);
        }
        this.totalAllWeight = acc;
    }

    public static void main(String[] args) {
        Cli cfg;
        try {
            cfg = Cli.parse(args);
        } catch (IllegalArgumentException e) {
            System.exit(2);
            return;
        }
        try {
            new QwpTradesGenerator(cfg).run();
        } catch (Exception e) {
            System.err.printf("[FATAL] %s: %s%n", e.getClass().getSimpleName(), e.getMessage());
            for (Throwable c = e.getCause(); c != null && c != c.getCause(); c = c.getCause()) {
                System.err.printf("[FATAL]   caused by %s: %s%n", c.getClass().getSimpleName(), c.getMessage());
            }
            System.exit(1);
        }
    }

    private void run() throws Exception {
        System.out.println("=== qwp-fx generator ===");
        System.out.printf("mode=%s  hosts=%s  tls=%s  trades_processes=%d  market_data_processes=%d%n",
                cfg.mode, cfg.hosts, cfg.tls, cfg.tradesProcesses, cfg.marketDataProcesses);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        // 1) Reference data → initial mid per symbol (shared, read-only).
        if (cfg.incremental) {
            seedInitialMidFromDb();
        } else if (!cfg.noYahoo) {
            new YahooFinance().refreshBrackets(pairs, 1.0, Long.MIN_VALUE); // startup: reset timeline
        }
        for (int i = 0; i < pairs.size(); i++) {
            FxPair p = pairs.get(i);
            if (initialMid[i] <= 0) {
                initialMid[i] = FxUniverse.quantizeToPip(p.midOfBracketAt(0), p.pip, p.precision);
            }
        }

        // 2) Schema for each enabled table, plus optional materialized views.
        createTables();
        createViews();

        // 3) Start point (data clock). resolveStartNanos enforces the same overlap
        //    safety as Python: advance/abort in faster-than-life, wait in real-time.
        Long endNs = cfg.endTs == null ? null : isoToNanos(cfg.endTs);
        long startNs = resolveStartNanos(endNs);

        // 4) Build the two pools and run them.
        boolean realtime = "real-time".equals(cfg.mode);
        long wallStartMs = System.currentTimeMillis();
        long t0 = System.nanoTime();

        List<Long> perSecond = new ArrayList<>();
        Thread sampler = cfg.runSecs > 0 ? startThroughputSampler(perSecond) : null;
        Thread deadline = cfg.runSecs > 0 ? startDeadline(cfg.runSecs) : null;
        // Outside benchmark mode (no --run_secs), emit a lightweight heartbeat every
        // 10s so a long-running stream still shows it is alive and keeping pace.
        Thread heartbeat = cfg.runSecs == 0 ? startHeartbeat() : null;
        Thread walMonitor = startWalMonitor();
        Thread yahooRefresher = (realtime && !cfg.noYahoo && !cfg.incremental)
                ? startYahooRefresher(wallStartMs) : null;

        List<Thread> workers = new ArrayList<>();
        workers.addAll(spawnPool(Kind.TRADES, cfg.tradesProcesses, cfg.tradesMinPerSec,
                cfg.tradesMaxPerSec, startNs, endNs, wallStartMs));
        workers.addAll(spawnPool(Kind.MARKET_DATA, cfg.marketDataProcesses, cfg.marketDataMinEps,
                cfg.marketDataMaxEps, startNs, endNs, wallStartMs));
        workers.addAll(spawnPool(Kind.CORE_PRICE, cfg.coreProcesses, cfg.coreMinEps,
                cfg.coreMaxEps, startNs, endNs, wallStartMs));
        for (Thread th : workers) {
            th.join();
        }

        running.set(false);
        if (yahooRefresher != null) {
            yahooRefresher.interrupt();
        }
        if (deadline != null) {
            deadline.interrupt();
            deadline.join(1000);
        }
        if (sampler != null) {
            sampler.interrupt();
            sampler.join(1500);
        }
        if (heartbeat != null) {
            heartbeat.interrupt();
            heartbeat.join(1000);
        }
        if (walMonitor != null) {
            walMonitor.join(2000);
        }

        double secs = (System.nanoTime() - t0) / 1e9;
        printThroughputSummary(perSecond);
        long totalRows = tradesRows.get() + mdRows.get() + coreRows.get();
        System.out.printf("[DONE] in %.2fs wall — %,d rows total:%n", secs, totalRows);
        printPoolDone("trades", tradesRows.get(), tradesFinishMs.get(), wallStartMs);
        printPoolDone("market_data", mdRows.get(), mdFinishMs.get(), wallStartMs);
        printPoolDone("core_price", coreRows.get(), coreFinishMs.get(), wallStartMs);
    }

    private List<Thread> spawnPool(Kind kind, int processes, int poolMin, int poolMax,
                                   long startNs, Long endNs, long wallStartMs) {
        List<Thread> threads = new ArrayList<>();
        if (processes <= 0) {
            return threads;
        }
        List<List<Integer>> assignment = snakeDraft(processes);
        logAssignment(kind, assignment);
        for (int w = 0; w < processes; w++) {
            Worker worker = new Worker(kind, w, assignment.get(w), poolMin, poolMax,
                    startNs, endNs, wallStartMs);
            Thread th = new Thread(worker, "qwp-" + tag(kind) + "-" + w);
            threads.add(th);
            th.start();
        }
        return threads;
    }

    private static String tag(Kind kind) {
        switch (kind) {
            case TRADES: return "t";
            case MARKET_DATA: return "md";
            default: return "cp";
        }
    }

    // ---------------------------------------------------------------- worker fleet

    private List<List<Integer>> snakeDraft(int p) {
        int n = pairs.size();
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        Arrays.sort(order, Comparator.comparingInt((Integer i) -> pairs.get(i).rank)
                .thenComparingInt(i -> i));
        List<List<Integer>> res = new ArrayList<>();
        for (int w = 0; w < p; w++) {
            res.add(new ArrayList<>());
        }
        int top = 0, bottom = n - 1, k = 0;
        while (top <= bottom) {
            List<Integer> bucket = res.get(k % p);
            bucket.add(order[top++]);
            if (top <= bottom) {
                bucket.add(order[bottom--]);
            }
            k++;
        }
        return res;
    }

    private void logAssignment(Kind kind, List<List<Integer>> assignment) {
        if (assignment.size() == 1) {
            return;
        }
        for (int w = 0; w < assignment.size(); w++) {
            StringBuilder sb = new StringBuilder();
            for (int idx : assignment.get(w)) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(pairs.get(idx).symbol);
            }
            System.out.printf("[INFO] %s worker %d owns %d symbols: %s%n",
                    tag(kind), w, assignment.get(w).size(), sb);
        }
    }

    /** One ingest worker: a pool kind, a disjoint symbol set, its own QWP sender. */
    private final class Worker implements Runnable {
        private final Kind kind;
        private final int id;
        private final int[] owned;
        private final int[] cumW;
        private final int totalW;
        private final int wMin;
        private final int wMax;
        private final long startNs;
        private final Long endNs;
        private final long wallStartMs;

        // Deterministic per-owned-symbol walk state (indexed by position in `owned`).
        private final SplittableRandom[] walkRng;
        private final double[] walkMid;
        private final int[] walkSp;
        // per-second open/close snapshots
        private final double[] oMid;
        private final double[] cMid;
        private final int[] oSp;
        private final int[] cSp;
        // core_price: per-symbol indicator walk (separate RNG so it never perturbs the
        // mid/spread stream the other pools reproduce identically). Only used by CORE_PRICE.
        private final SplittableRandom[] indRng;
        private final double[] walkInd1;
        private final double[] walkInd2;
        private final double[] oInd1;
        private final double[] cInd1;
        private final double[] oInd2;
        private final double[] cInd2;
        // market_data: reusable DoubleArrays keyed by level count
        private final Map<Integer, DoubleArray> bidArrays = new HashMap<>();
        private final Map<Integer, DoubleArray> askArrays = new HashMap<>();
        private boolean stop = false;
        // Per-second event plan: evPlan[0..offsetCount) packs (offset<<SYM_BITS|ownedIdx),
        // sorted by offset, consumed across sub-second slices via offsetCursor.
        private long[] evPlan;
        private int offsetCount;
        private int offsetCursor;
        private int[] symCount;   // events per owned symbol this second
        private int[] symSeen;    // running per-symbol ordinal during emission
        // Exact open/close best bid/ask per owned symbol (Python interpolates bid/ask
        // directly by event ordinal and pins the first/last event to open/close).
        private double[] oBid;
        private double[] oAsk;
        private double[] cBid;
        private double[] cAsk;
        private final long tsLookaheadNs;

        Worker(Kind kind, int id, List<Integer> ownedList, int poolMin, int poolMax,
               long startNs, Long endNs, long wallStartMs) {
            this.kind = kind;
            this.id = id;
            this.startNs = startNs;
            this.endNs = endNs;
            this.wallStartMs = wallStartMs;
            this.owned = ownedList.stream().mapToInt(Integer::intValue).toArray();
            // Per-symbol selection weight = (11 - rank), so more liquid pairs get more
            // events. NOTE: deliberate divergence from Python, which weights only trades
            // and picks market_data/core_price symbols uniformly — weighting all three is
            // more realistic (majors quote/trade far more than exotics).
            this.cumW = new int[owned.length];
            int acc = 0;
            for (int j = 0; j < owned.length; j++) {
                acc += (11 - pairs.get(owned[j]).rank);
                cumW[j] = acc;
            }
            this.totalW = acc;
            double share = totalAllWeight > 0 ? (double) totalW / totalAllWeight : 1.0;
            this.wMin = Math.max(1, (int) Math.round(poolMin * share));
            this.wMax = Math.max(wMin, (int) Math.round(poolMax * share));
            this.walkRng = new SplittableRandom[owned.length];
            this.walkMid = new double[owned.length];
            this.walkSp = new int[owned.length];
            this.oMid = new double[owned.length];
            this.cMid = new double[owned.length];
            this.oSp = new int[owned.length];
            this.cSp = new int[owned.length];
            this.indRng = new SplittableRandom[owned.length];
            this.walkInd1 = new double[owned.length];
            this.walkInd2 = new double[owned.length];
            this.oInd1 = new double[owned.length];
            this.cInd1 = new double[owned.length];
            this.oInd2 = new double[owned.length];
            this.cInd2 = new double[owned.length];
            this.evPlan = new long[wMax + 1];
            this.symCount = new int[owned.length];
            this.symSeen = new int[owned.length];
            this.oBid = new double[owned.length];
            this.oAsk = new double[owned.length];
            this.cBid = new double[owned.length];
            this.cAsk = new double[owned.length];
            this.tsLookaheadNs = "real-time".equals(cfg.mode)
                    ? (long) cfg.realtimeLookaheadSecs * NANOS_PER_SEC : 0L;
            for (int j = 0; j < owned.length; j++) {
                int g = owned[j];
                walkRng[j] = new SplittableRandom(WALK_SEED_BASE + GOLDEN * g);
                walkMid[j] = initialMid[g];
                walkSp[j] = 4;
                // Distinct seed from walkRng so the indicator draws never shift the
                // mid/spread stream. Initial values match the Python template (0.2, 0.5).
                indRng[j] = new SplittableRandom((WALK_SEED_BASE * 31 + 0x5DEECE66DL) + GOLDEN * g);
                walkInd1[j] = 0.2;
                walkInd2[j] = 0.5;
            }
        }

        @Override
        public void run() {
            String table = kind == Kind.TRADES ? cfg.tradesTable()
                    : kind == Kind.MARKET_DATA ? cfg.marketDataTable()
                    : cfg.corePriceTable();
            int commitMs = kind == Kind.TRADES ? cfg.tradesCommitIntervalMs()
                    : kind == Kind.MARKET_DATA ? cfg.marketDataCommitIntervalMs()
                    : cfg.coreCommitIntervalMs();
            boolean realtime = "real-time".equals(cfg.mode);
            try (Sender sender = buildSender(kind, id)) {
                long base = alignToSecond(startNs);
                long secondStartNs = base;
                if (realtime) {
                    // Real-time: commit cadence can be sub-second. Split each data-second
                    // into slices of `commitMs`, emitting + flushing + pacing per slice so
                    // the data trickles across the second (and the WAL commits at commitMs).
                    long sliceNs = Math.min(NANOS_PER_SEC, Math.max(1L, (long) commitMs) * 1_000_000L);
                    while (running.get() && !stop && !capReached() && !pastEnd(secondStartNs, endNs)) {
                        waitWhilePaused();
                        if (!running.get()) {
                            break;
                        }
                        // Data-second index, identical across pools for the same
                        // secondStartNs -> deterministic bracket lookup -> consistent walk.
                        long k = (secondStartNs - base) / NANOS_PER_SEC;
                        beginSecond(k, 5.0);
                        long sliceEnd = sliceNs;
                        while (true) {
                            emitSlice(sender, table, secondStartNs, sliceEnd);
                            sender.flush();
                            long deadlineMs = wallStartMs + k * 1000L + sliceEnd / 1_000_000L;
                            long sleepMs = deadlineMs - System.currentTimeMillis();
                            if (sleepMs > 0) {
                                Thread.sleep(sleepMs);
                            }
                            if (stop || !running.get() || sliceEnd >= NANOS_PER_SEC) {
                                break;
                            }
                            sliceEnd = Math.min(NANOS_PER_SEC, sliceEnd + sliceNs);
                        }
                        endSecond();
                        secondStartNs += NANOS_PER_SEC;
                    }
                } else {
                    // Faster-than-life: no wall pacing; emit whole seconds and commit on a
                    // wall-clock cadence so many data-seconds squash into one transaction.
                    long lastCommitMs = System.currentTimeMillis();
                    while (running.get() && !stop && !capReached() && !pastEnd(secondStartNs, endNs)) {
                        waitWhilePaused();
                        if (!running.get()) {
                            break;
                        }
                        long k = (secondStartNs - base) / NANOS_PER_SEC;
                        beginSecond(k, 5.0);
                        emitSlice(sender, table, secondStartNs, NANOS_PER_SEC);
                        endSecond();
                        secondStartNs += NANOS_PER_SEC;
                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastCommitMs >= commitMs) {
                            sender.flush();
                            lastCommitMs = nowMs;
                        }
                    }
                }
                sender.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.printf("[%s worker %d] FATAL: %s%n", tag(kind), id, e.getMessage());
            } finally {
                finishMsFor(kind).accumulateAndGet(System.currentTimeMillis(), Math::max);
                bidArrays.values().forEach(DoubleArray::close);
                askArrays.values().forEach(DoubleArray::close);
            }
        }

        /**
         * Advance the deterministic walk once for data-second {@code k} (open/close per
         * owned symbol, plus core_price indicators) and lay out this second's event
         * offsets. Done once per second regardless of how many sub-second commit slices
         * follow, so price continuity is unaffected by the commit cadence.
         */
        private void beginSecond(long k, double driftPips) {
            for (int j = 0; j < owned.length; j++) {
                FxPair p = pairs.get(owned[j]);
                oMid[j] = walkMid[j];
                oSp[j] = walkSp[j];
                double lo = p.lowAt(k);
                double hi = p.highAt(k);
                cMid[j] = FxUniverse.evolveMid(oMid[j], p, driftPips, lo, hi, walkRng[j]);
                cSp[j] = FxUniverse.evolveSpreadPips(oSp[j], walkRng[j]);
                // Exact open/close best bid/ask; intra-second events interpolate between
                // these by ordinal, with the first/last event pinned exactly to them so
                // close(t) == open(t+1) holds in the emitted rows (clean OHLC candles).
                oBid[j] = FxUniverse.quantizeToPip(oMid[j] - oSp[j] * p.pip / 2.0, p.pip, p.precision);
                oAsk[j] = FxUniverse.quantizeToPip(oMid[j] + oSp[j] * p.pip / 2.0, p.pip, p.precision);
                cBid[j] = FxUniverse.quantizeToPip(cMid[j] - cSp[j] * p.pip / 2.0, p.pip, p.precision);
                cAsk[j] = FxUniverse.quantizeToPip(cMid[j] + cSp[j] * p.pip / 2.0, p.pip, p.precision);
                symCount[j] = 0;
                symSeen[j] = 0;
            }
            // Indicators live only on core_price; advance their (separate) walk here.
            if (kind == Kind.CORE_PRICE) {
                for (int j = 0; j < owned.length; j++) {
                    oInd1[j] = walkInd1[j];
                    oInd2[j] = walkInd2[j];
                    cInd1[j] = FxUniverse.evolveIndicator(oInd1[j], indRng[j]);
                    cInd2[j] = FxUniverse.evolveIndicator(oInd2[j], indRng[j]);
                }
            }
            int nEvents = ThreadLocalRandom.current().nextInt(wMin, wMax + 1);
            for (int e = 0; e < nEvents; e++) {
                int idx = pickOwned();
                long offset = ThreadLocalRandom.current().nextLong(NANOS_PER_SEC);
                evPlan[e] = (offset << SYM_BITS) | idx;
                symCount[idx]++;
            }
            Arrays.sort(evPlan, 0, nEvents);  // sorts by offset (high bits), symbol follows
            offsetCount = nEvents;
            offsetCursor = 0;
        }

        /** Emit the prepared events whose offset is {@code < toNs}, continuing where the
         *  previous slice left off. Sets {@code stop} on cap/end-of-window. */
        private void emitSlice(Sender sender, String table, long secondStartNs, long toNs) {
            while (offsetCursor < offsetCount) {
                long packed = evPlan[offsetCursor];
                long offset = packed >>> SYM_BITS;
                if (offset >= toNs) {
                    return;
                }
                offsetCursor++;
                int idx = (int) (packed & SYM_MASK);
                long ts = secondStartNs + offset + tsLookaheadNs;
                if (endNs != null && ts >= endNs) {
                    stop = true;
                    return;
                }
                FxPair p = pairs.get(owned[idx]);
                int j = symSeen[idx]++;
                int n = symCount[idx];
                boolean last = j == n - 1;
                double bid;
                double ask;
                if (j == 0) {                       // first event of this symbol: exact open
                    bid = oBid[idx];
                    ask = oAsk[idx];
                } else if (last) {                  // last event: exact close
                    bid = cBid[idx];
                    ask = cAsk[idx];
                } else {                            // interpolate bid/ask by ordinal
                    double frac = (double) j / (n - 1);
                    bid = FxUniverse.quantizeToPip(oBid[idx] + frac * (cBid[idx] - oBid[idx]), p.pip, p.precision);
                    ask = FxUniverse.quantizeToPip(oAsk[idx] + frac * (cAsk[idx] - oAsk[idx]), p.pip, p.precision);
                }

                if (kind == Kind.TRADES) {
                    if (!emitOrder(sender, table, ts, p, bid, ask)) {
                        stop = true;
                        return;
                    }
                } else if (kind == Kind.MARKET_DATA) {
                    if (capReached()) {
                        stop = true;
                        return;
                    }
                    emitSnapshot(sender, table, ts, p, bid, ask);
                    mdRows.incrementAndGet();
                } else {
                    if (capReached()) {
                        stop = true;
                        return;
                    }
                    // Python uses open-state indicators for all but the last event (close).
                    double ind1 = last ? cInd1[idx] : oInd1[idx];
                    double ind2 = last ? cInd2[idx] : oInd2[idx];
                    emitCore(sender, table, ts, p, bid, ask, ind1, ind2);
                    coreRows.incrementAndGet();
                }
            }
        }

        /** Carry this second's close state forward as the next second's open (continuity). */
        private void endSecond() {
            for (int j = 0; j < owned.length; j++) {
                walkMid[j] = cMid[j];
                walkSp[j] = cSp[j];
            }
            if (kind == Kind.CORE_PRICE) {
                for (int j = 0; j < owned.length; j++) {
                    walkInd1[j] = cInd1[j];
                    walkInd2[j] = cInd2[j];
                }
            }
        }

        /**
         * Generate an order and execute it against the reconstructed book (walk levels),
         * emitting one trade per fill at the real level price. Returns false if the
         * global row cap was hit mid-order.
         */
        private boolean emitOrder(Sender sender, String table, long base, FxPair p, double bid, double ask) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            boolean buy = rnd.nextBoolean();
            boolean passive = rnd.nextDouble() < 0.4;
            double remaining = FxUniverse.tradeSizeLognormal();
            double limit = passive
                    ? (buy ? ask + rnd.nextInt(0, 3) * p.pip : bid - rnd.nextInt(0, 3) * p.pip)
                    : (buy ? ask + rnd.nextInt(3, 11) * p.pip : bid - rnd.nextInt(3, 11) * p.pip);
            String ecn = FxUniverse.ECNS[rnd.nextInt(FxUniverse.ECNS.length)];
            long orderLo = rnd.nextLong();
            long orderHi = rnd.nextLong();

            int fill = 0;
            for (int lvl = 0; lvl < cfg.maxLevels && remaining > 0; lvl++) {
                double price = buy
                        ? FxUniverse.quantizeToPip(ask + lvl * p.pip, p.pip, p.precision)
                        : FxUniverse.quantizeToPip(bid - lvl * p.pip, p.pip, p.precision);
                if (buy ? price > limit : price < limit) {
                    break;
                }
                if (capReached()) {
                    return false;
                }
                double lvlVol = FxUniverse.volumeForLevel(lvl, ladder);
                double qty = Math.min(remaining, lvlVol);
                remaining -= qty;
                String counterparty = leiPool[rnd.nextInt(leiPool.length)];
                sender.table(table)
                        .symbol("symbol", p.symbol)
                        .symbol("ecn", ecn)
                        .symbol("side", buy ? "buy" : "sell")
                        .symbol("counterparty", counterparty)
                        .boolColumn("passive", passive)
                        .doubleColumn("price", price)
                        .doubleColumn("quantity", qty)
                        .uuidColumn("trade_id", rnd.nextLong(), rnd.nextLong())
                        .uuidColumn("order_id", orderLo, orderHi)
                        .at(base + fill, ChronoUnit.NANOS);
                tradesRows.incrementAndGet();
                fill++;
            }
            return true;
        }

        /** Emit one order-book snapshot: bids/asks as DOUBLE[][] of shape [2][levels]. */
        private void emitSnapshot(Sender sender, String table, long ts, FxPair p, double bid, double ask) {
            int levels = ThreadLocalRandom.current().nextInt(cfg.minLevels, cfg.maxLevels + 1);
            DoubleArray bids = bidArrays.computeIfAbsent(levels, L -> new DoubleArray(2, L));
            DoubleArray asks = askArrays.computeIfAbsent(levels, L -> new DoubleArray(2, L));
            bids.clear();
            asks.clear();
            // row 0 = prices
            for (int i = 0; i < levels; i++) {
                bids.append(FxUniverse.quantizeToPip(bid - i * p.pip, p.pip, p.precision));
            }
            for (int i = 0; i < levels; i++) {
                asks.append(FxUniverse.quantizeToPip(ask + i * p.pip, p.pip, p.precision));
            }
            // row 1 = volumes
            for (int i = 0; i < levels; i++) {
                bids.append((double) FxUniverse.volumeForLevel(i, ladder));
            }
            for (int i = 0; i < levels; i++) {
                asks.append((double) FxUniverse.volumeForLevel(i, ladder));
            }
            sender.table(table)
                    .symbol("symbol", p.symbol)
                    .doubleArray("bids", bids)
                    .doubleArray("asks", asks)
                    .doubleColumn("best_bid", bid)
                    .doubleColumn("best_ask", ask)
                    .at(ts, ChronoUnit.NANOS);
        }

        /**
         * Emit one top-of-book core_price row: best bid/ask (from the same walk the
         * other tables use, so prices stay consistent) plus level-0 volumes, a random
         * ecn/reason, and the interpolated indicators. Mirrors the Python {@code emit_core}.
         */
        private void emitCore(Sender sender, String table, long ts, FxPair p,
                              double bid, double ask, double ind1, double ind2) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            String ecn = FxUniverse.ECNS[rnd.nextInt(FxUniverse.ECNS.length)];
            String reason = FxUniverse.CORE_REASONS[rnd.nextInt(FxUniverse.CORE_REASONS.length)];
            sender.table(table)
                    .symbol("symbol", p.symbol)
                    .symbol("ecn", ecn)
                    .symbol("reason", reason)
                    .doubleColumn("bid_price", bid)
                    .longColumn("bid_volume", FxUniverse.volumeForLevel(0, ladder))
                    .doubleColumn("ask_price", ask)
                    .longColumn("ask_volume", FxUniverse.volumeForLevel(0, ladder))
                    .doubleColumn("indicator1", Math.round(ind1 * 1000.0) / 1000.0)
                    .doubleColumn("indicator2", Math.round(ind2 * 1000.0) / 1000.0)
                    .at(ts, ChronoUnit.NANOS);
        }

        private int pickOwned() {
            int r = ThreadLocalRandom.current().nextInt(totalW);
            for (int j = 0; j < cumW.length; j++) {
                if (r < cumW[j]) {
                    return j;
                }
            }
            return cumW.length - 1;
        }

        private void waitWhilePaused() throws InterruptedException {
            AtomicBoolean flag = pausedFor(kind);
            // Only spins here while this pool is paused, so a fine 200ms poll is cheap
            // and lets the worker resume promptly once its table has caught up.
            while (flag.get() && running.get()) {
                Thread.sleep(200);
            }
        }
    }

    // ---------------------------------------------------------------- reporting / cap

    private Thread startThroughputSampler(List<Long> perSecond) {
        Thread t = new Thread(() -> {
            long lastT = 0, lastMd = 0, lastCp = 0, sec = 0;
            try {
                while (running.get()) {
                    Thread.sleep(1000);
                    long nowT = tradesRows.get();
                    long nowMd = mdRows.get();
                    long nowCp = coreRows.get();
                    long dT = nowT - lastT;
                    long dMd = nowMd - lastMd;
                    long dCp = nowCp - lastCp;
                    lastT = nowT;
                    lastMd = nowMd;
                    lastCp = nowCp;
                    sec++;
                    perSecond.add(dT + dMd + dCp);
                    System.out.printf("[rate] t=%ds  trades %,d/s  md %,d/s  core %,d/s  (total %,d/s)%n",
                            sec, dT, dMd, dCp, dT + dMd + dCp);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "qwp-throughput-sampler");
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Continuous-mode heartbeat: every ~10s print, per enabled table, the average
     * rows/sec over the interval and the accumulated row count. Daemon thread that
     * only reads the row counters, so it never touches the ingestion hot path.
     */
    private Thread startHeartbeat() {
        Thread t = new Thread(() -> {
            long lastMs = System.currentTimeMillis();
            long lastT = 0, lastMd = 0, lastCp = 0, elapsed = 0;
            try {
                while (running.get()) {
                    long target = 10_000, slept = 0;
                    while (slept < target && running.get()) {
                        long chunk = Math.min(500, target - slept);
                        Thread.sleep(chunk);
                        slept += chunk;
                    }
                    if (!running.get()) {
                        break;
                    }
                    long now = System.currentTimeMillis();
                    double dt = (now - lastMs) / 1000.0;
                    if (dt <= 0) {
                        dt = 1;
                    }
                    long nowT = tradesRows.get(), nowMd = mdRows.get(), nowCp = coreRows.get();
                    elapsed += Math.round(dt);
                    StringBuilder sb = new StringBuilder();
                    sb.append("[hb] t=").append(elapsed).append("s ");
                    if (cfg.tradesProcesses > 0) {
                        sb.append(String.format("  trades %,d/s (%,d)", Math.round((nowT - lastT) / dt), nowT));
                    }
                    if (cfg.marketDataProcesses > 0) {
                        sb.append(String.format("  md %,d/s (%,d)", Math.round((nowMd - lastMd) / dt), nowMd));
                    }
                    if (cfg.coreProcesses > 0) {
                        sb.append(String.format("  core %,d/s (%,d)", Math.round((nowCp - lastCp) / dt), nowCp));
                    }
                    long totDelta = (nowT - lastT) + (nowMd - lastMd) + (nowCp - lastCp);
                    long totRows = nowT + nowMd + nowCp;
                    sb.append(String.format("  total %,d/s (%,d)", Math.round(totDelta / dt), totRows));
                    System.out.println(sb);
                    lastMs = now;
                    lastT = nowT;
                    lastMd = nowMd;
                    lastCp = nowCp;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "qwp-heartbeat");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private Thread startDeadline(int seconds) {
        Thread t = new Thread(() -> {
            try {
                for (int s = 0; s < seconds && running.get(); s++) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (running.compareAndSet(true, false)) {
                System.out.printf("[run] reached --run_secs %d, stopping.%n", seconds);
            }
        }, "qwp-deadline");
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** One per-pool summary line: rows + rate over the pool's own active span. */
    private void printPoolDone(String name, long rows, long finishMs, long startMs) {
        if (rows == 0) {
            return;
        }
        double activeSecs = finishMs > startMs ? (finishMs - startMs) / 1000.0 : 0.0;
        double rate = activeSecs > 0 ? rows / activeSecs : 0.0;
        System.out.printf("  %-12s %,15d rows  (%,.0f/s over %.1fs)%n", name, rows, rate, activeSecs);
    }

    private void printThroughputSummary(List<Long> perSecond) {
        if (perSecond.isEmpty()) {
            return;
        }
        long[] sorted = new long[perSecond.size()];
        long sum = 0;
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = perSecond.get(i);
            sum += sorted[i];
        }
        Arrays.sort(sorted);
        System.out.printf("[throughput] %d per-second samples: min=%,d  median=%,d  avg=%,.0f  max=%,d  rows/sec%n",
                sorted.length, sorted[0], sorted[sorted.length / 2],
                (double) sum / sorted.length, sorted[sorted.length - 1]);
    }

    /**
     * The row-count `--total_market_data_events` caps: the **market_data** table
     * when that pool is enabled (it dominates and grows fastest, so it defines the
     * hard stop), otherwise the trades table. When this counter hits the cap the
     * whole run stops (both pools).
     */
    private AtomicLong capCounter() {
        return cfg.marketDataProcesses > 0 ? mdRows : tradesRows;
    }

    private boolean capReached() {
        return cfg.totalTrades > 0 && capCounter().get() >= cfg.totalTrades;
    }

    private boolean pastEnd(long secondStartNs, Long endNs) {
        return endNs != null && secondStartNs >= endNs;
    }

    // ---------------------------------------------------------------- transport / DDL

    private Sender buildSender(Kind kind, int workerId) {
        Sender.LineSenderBuilder b = Sender.builder(Sender.Transport.WEBSOCKET);
        for (String host : cfg.hosts) {
            b.address(host);
        }
        if (cfg.tls) {
            b.enableTls();
            if (cfg.tlsInsecure) {
                b.advancedTls().disableCertificateValidation();
            }
        }
        if (cfg.token != null) {
            b.httpToken(cfg.token);
        } else if (cfg.user != null) {
            b.httpUsernamePassword(cfg.user, cfg.password);
        }
        String sfPath = cfg.sfDir + "/" + tag(kind) + workerId;
        try {
            Files.createDirectories(Paths.get(sfPath));
        } catch (Exception e) {
            System.err.printf("[%s%d] WARN: could not pre-create sf dir %s: %s%n",
                    tag(kind), workerId, sfPath, e.getMessage());
        }
        final AtomicLong lastConnLogMs = new AtomicLong(0);
        final String who = tag(kind) + workerId;
        b.storeAndForwardDir(sfPath)
                .senderId(cfg.senderId + "-" + who)
                .transactional(true)
                .reconnectMaxDurationMillis(300_000)
                .reconnectInitialBackoffMillis(100)
                .reconnectMaxBackoffMillis(5_000)
                .autoFlushBytes(cfg.autoFlushBytes)
                .autoFlushRows(1_000_000)
                .autoFlushIntervalMillis(1_000)
                .errorHandler(error -> System.err.printf("[%s %s] BATCH ERROR: category=%s table=%s msg=%s%n",
                        who, LocalDateTime.now().format(FMT), error.getCategory(),
                        error.getTableName(), error.getServerMessage()))
                .connectionListener(event -> {
                    long now = System.currentTimeMillis();
                    long prev = lastConnLogMs.get();
                    if (now - prev >= 1000 && lastConnLogMs.compareAndSet(prev, now)) {
                        System.out.printf("[%s %s] CONNECTION: %s host=%s:%d%n",
                                who, LocalDateTime.now().format(FMT), event.getKind(),
                                event.getHost(), event.getPort());
                    }
                });
        return b.build();
    }

    private String retentionClause(String ossTtl) {
        if (!cfg.shortTtl) {
            return "";
        }
        return cfg.enterprise ? " STORAGE POLICY(" + ENTERPRISE_POLICY + ")" : " TTL " + ossTtl;
    }

    /**
     * Retention for materialized views. QuestDB does not yet accept STORAGE POLICY on
     * matviews (even on enterprise), so this always falls back to TTL when --short_ttl
     * is set. Mirrors the Python {@code mv_retention_clause}; once enterprise matviews
     * support storage policies, point this at {@link #retentionClause(String)}.
     */
    private String mvRetentionClause(String ossTtl) {
        if (!cfg.shortTtl) {
            return "";
        }
        return " TTL " + ossTtl;
    }

    private void createTables() {
        if (cfg.tradesProcesses > 0) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + cfg.tradesTable() + " ("
                    + "timestamp TIMESTAMP_NS PARQUET(delta_binary_packed, zstd(4)), "
                    + "symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter), "
                    + "ecn SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter), "
                    + "trade_id UUID PARQUET(default, zstd(4)), "
                    + "side SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter), "
                    + "passive BOOLEAN PARQUET(default, zstd(4)), "
                    + "price DOUBLE PARQUET(default, zstd(4)), "
                    + "quantity DOUBLE PARQUET(default, zstd(4)), "
                    + "counterparty SYMBOL PARQUET(rle_dictionary, zstd(4)), "
                    + "order_id UUID PARQUET(default, zstd(4))"
                    + ") timestamp(timestamp) PARTITION BY HOUR" + retentionClause("1 MONTH")
                    + " DEDUP UPSERT KEYS(timestamp, trade_id)";
            execDdl(cfg.tradesTable(), ddl);
        }
        if (cfg.marketDataProcesses > 0) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + cfg.marketDataTable() + " ("
                    + "timestamp TIMESTAMP PARQUET(delta_binary_packed, zstd(4)), "
                    + "symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter), "
                    + "bids DOUBLE[][] PARQUET(default, zstd(4)), "
                    + "asks DOUBLE[][] PARQUET(default, zstd(4)), "
                    + "best_bid DOUBLE PARQUET(default, zstd(4)), "
                    + "best_ask DOUBLE PARQUET(default, zstd(4))"
                    + ") timestamp(timestamp) PARTITION BY HOUR" + retentionClause("3 DAYS");
            execDdl(cfg.marketDataTable(), ddl);
        }
        if (cfg.coreProcesses > 0) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + cfg.corePriceTable() + " ("
                    + "timestamp TIMESTAMP PARQUET(delta_binary_packed, zstd(4)), "
                    + "symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter), "
                    + "ecn SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter), "
                    + "bid_price DOUBLE PARQUET(default, zstd(4)), "
                    + "bid_volume LONG PARQUET(default, zstd(4)), "
                    + "ask_price DOUBLE PARQUET(default, zstd(4)), "
                    + "ask_volume LONG PARQUET(default, zstd(4)), "
                    + "reason SYMBOL PARQUET(rle_dictionary, zstd(4)), "
                    + "indicator1 DOUBLE PARQUET(default, zstd(4)), "
                    + "indicator2 DOUBLE PARQUET(default, zstd(4))"
                    + ") timestamp(timestamp) PARTITION BY HOUR" + retentionClause("3 DAYS");
            execDdl(cfg.corePriceTable(), ddl);
        }
    }

    /**
     * Optional materialized views over market_data (Python parity, suffix-aware):
     * a 1m OHLC (REFRESH IMMEDIATE), a 15m OHLC cascading off the 1m view
     * (REFRESH EVERY 5m), and an hourly BBO (REFRESH EVERY 10m). Retention is TTL via
     * {@link #mvRetentionClause(String)}. The views are owned by the connecting user
     * (no OWNED BY clause), so creation never needs admin/superuser privileges.
     */
    private void createViews() {
        if (!cfg.createViews) {
            return;
        }
        if (cfg.marketDataProcesses <= 0) {
            System.out.println("[INFO] --create_views set but the market_data pool is off; skipping views "
                    + "(they read from " + cfg.marketDataTable() + ").");
            return;
        }
        String md = cfg.marketDataTable();
        String md1m = "qwp_market_data_ohlc_1m" + cfg.suffix;
        String md15m = "qwp_market_data_ohlc_15m" + cfg.suffix;
        String bbo1h = "qwp_bbo_1h" + cfg.suffix;
        String ttl = mvRetentionClause("3 DAYS");

        execDdl(md1m, "CREATE MATERIALIZED VIEW IF NOT EXISTS '" + md1m + "' WITH BASE '" + md + "' REFRESH IMMEDIATE AS ("
                + "SELECT timestamp, symbol, "
                + "first(best_bid) AS open, max(best_bid) AS high, min(best_bid) AS low, last(best_bid) AS close, "
                + "SUM(bids[2][1]) AS total_volume "
                + "FROM " + md + " SAMPLE BY 1m"
                + ") PARTITION BY HOUR" + ttl);

        execDdl(md15m, "CREATE MATERIALIZED VIEW IF NOT EXISTS '" + md15m + "' WITH BASE '" + md1m + "' REFRESH EVERY 5m AS ("
                + "SELECT timestamp, symbol, "
                + "first(open) AS open, max(high) AS high, min(low) AS low, last(close) AS close, "
                + "SUM(total_volume) AS total_volume "
                + "FROM " + md1m + " SAMPLE BY 15m"
                + ") PARTITION BY HOUR" + ttl);

        execDdl(bbo1h, "CREATE MATERIALIZED VIEW IF NOT EXISTS '" + bbo1h + "' REFRESH EVERY 10m AS ("
                + "SELECT timestamp, symbol, max(best_bid) AS bid, min(best_ask) AS ask "
                + "FROM " + md + " SAMPLE BY 1h"
                + ") PARTITION BY DAY" + ttl);
    }

    private void execDdl(String table, String ddl) {
        System.out.println("[INFO] ensuring table " + table + " exists ...");
        try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
            client.connect();
            client.execute(ddl, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                }

                @Override
                public void onEnd(long totalRows) {
                }

                @Override
                public void onError(byte status, String message) {
                    System.err.println("[DDL " + table + "] ERROR: " + message);
                }

                @Override
                public void onExecDone(short opType, long rowsAffected) {
                    System.out.println("[DDL] " + table + " ready.");
                }
            });
        }
    }

    // ---------------------------------------------------------------- WAL backpressure

    private AtomicBoolean pausedFor(Kind kind) {
        switch (kind) {
            case TRADES: return pausedTrades;
            case MARKET_DATA: return pausedMd;
            default: return pausedCore;
        }
    }

    private AtomicLong finishMsFor(Kind kind) {
        switch (kind) {
            case TRADES: return tradesFinishMs;
            case MARKET_DATA: return mdFinishMs;
            default: return coreFinishMs;
        }
    }

    private Thread startWalMonitor() {
        final List<Kind> kinds = new ArrayList<>();
        final List<String> tables = new ArrayList<>();
        final List<Integer> thresholds = new ArrayList<>();
        if (cfg.tradesProcesses > 0) {
            kinds.add(Kind.TRADES);
            tables.add(cfg.tradesTable());
            thresholds.add((cfg.tradesProcesses > 2 ? 3 : 5) * cfg.tradesProcesses);
        }
        if (cfg.marketDataProcesses > 0) {
            kinds.add(Kind.MARKET_DATA);
            tables.add(cfg.marketDataTable());
            thresholds.add((cfg.marketDataProcesses > 2 ? 3 : 5) * cfg.marketDataProcesses);
        }
        if (cfg.coreProcesses > 0) {
            kinds.add(Kind.CORE_PRICE);
            tables.add(cfg.corePriceTable());
            thresholds.add((cfg.coreProcesses > 2 ? 3 : 5) * cfg.coreProcesses);
        }
        Thread t = new Thread(() -> {
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
                client.connect();
                while (running.get()) {
                    boolean anyPaused = false;
                    for (int i = 0; i < tables.size(); i++) {
                        long[] lag = queryWalLag(client, tables.get(i));
                        if (lag == null) {
                            continue;
                        }
                        long diff = lag[0] - lag[1]; // sequencerTxn - writerTxn
                        AtomicBoolean flag = pausedFor(kinds.get(i));
                        // Hysteresis: pause when lag climbs above the high-water mark,
                        // resume once it drains back to half of it (not all the way to 0).
                        // Riding the apply ceiling in a tight sawtooth keeps the pool
                        // flowing instead of slamming to 0/s for the full drain.
                        int highWater = thresholds.get(i);
                        long lowWater = highWater / 2;
                        if (diff > highWater) {
                            if (flag.compareAndSet(false, true)) {
                                System.out.printf("[wal] %s lag %d > %d, pausing %s pool%n",
                                        tables.get(i), diff, highWater, tag(kinds.get(i)));
                            }
                        } else if (diff <= lowWater) {
                            if (flag.compareAndSet(true, false)) {
                                System.out.printf("[wal] %s drained to %d (<= %d), resuming %s pool%n",
                                        tables.get(i), diff, lowWater, tag(kinds.get(i)));
                            }
                        }
                        if (flag.get()) {
                            anyPaused = true;
                        }
                    }
                    // Poll fast (250ms) only while draining a pause; otherwise idle at 5s.
                    long sleepMs = anyPaused ? 250 : 5000;
                    long remaining = sleepMs;
                    while (remaining > 0 && running.get()) {
                        long chunk = Math.min(remaining, 500);
                        Thread.sleep(chunk);
                        remaining -= chunk;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("[wal] monitor stopped: " + e.getMessage());
            }
        }, "qwp-wal-monitor");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private long[] queryWalLag(QwpQueryClient client, String table) {
        final long[] out = {Long.MIN_VALUE, Long.MIN_VALUE};
        try {
            client.execute("SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = '" + table + "'",
                    new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batch.forEachRow(row -> {
                                out[0] = row.getLongValue(0);
                                out[1] = row.getLongValue(1);
                            });
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                        }
                    });
        } catch (Exception e) {
            return null;
        }
        return out[0] == Long.MIN_VALUE ? null : out;
    }

    private Thread startYahooRefresher(long wallStartMs) {
        final long marginSec = 5;
        Thread t = new Thread(() -> {
            YahooFinance yf = new YahooFinance();
            try {
                while (running.get()) {
                    for (int s = 0; s < cfg.yahooRefreshSecs && running.get(); s++) {
                        Thread.sleep(1000);
                    }
                    if (!running.get()) {
                        break;
                    }
                    // Stamp new brackets a few data-seconds ahead (real-time data clock
                    // tracks wall clock), so every worker adopts them at the same second
                    // -> the clamp stays consistent across both pools.
                    long effSec = (System.currentTimeMillis() - wallStartMs) / 1000 + marginSec;
                    yf.refreshBrackets(pairs, 1.0, effSec);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "qwp-yahoo-refresher");
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ---------------------------------------------------------------- start / helpers

    /**
     * Resolve the data-clock start, enforcing the same overlap safety as the Python
     * generator. Faster-than-life: advance the start past the latest existing row,
     * exit if the whole window is already present, and abort if {@code --end_ts} is at
     * or before the latest row. Real-time: wait (sleep) until wall-clock passes the
     * latest row, then start at "now". May {@code System.exit} with a clear message.
     */
    private long resolveStartNanos(Long endNs) {
        boolean ftl = "faster-than-life".equals(cfg.mode);
        Long latest = latestAcrossTables();

        if (ftl) {
            long startNs = cfg.startTs != null ? isoToNanos(cfg.startTs)
                    : (latest != null ? latest + 1_000L : nowNs());
            if (latest != null) {
                long next = latest + 1_000L;
                if (next > startNs) {
                    System.out.printf("[INFO] Advancing start from %s to %s to avoid overlap with existing data.%n",
                            nanosToIso(startNs), nanosToIso(next));
                    startNs = next;
                }
            }
            if (endNs != null && latest != null && endNs <= latest) {
                System.err.printf("[ERROR] --end_ts (%s) is at or before the most recent data in the tables (%s); "
                                + "the whole window is behind existing data and would ingest out-of-order. Aborting.%n",
                        cfg.endTs, nanosToIso(latest));
                System.exit(1);
            }
            if (endNs != null && startNs >= endNs) {
                if (latest != null) {
                    System.out.println("[INFO] All data in the requested range is already present. Exiting.");
                } else {
                    System.out.printf("[INFO] No work to do: start (%s) >= --end_ts (%s). Exiting.%n",
                            nanosToIso(startNs), cfg.endTs);
                }
                System.exit(0);
            }
            return startNs;
        }

        // Real-time: wait until wall-clock is past the latest row so we never overlap.
        if (latest != null) {
            long next = latest + 1_000L;
            long now = nowNs();
            if (next > now) {
                double waitSecs = (next - now) / 1e9;
                System.out.printf("[INFO] Last row in DB is %s. Waiting %.1fs to avoid overlap...%n",
                        nanosToIso(latest), waitSecs);
                try {
                    Thread.sleep((long) Math.ceil(waitSecs * 1000.0));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        long startNs = nowNs();
        if (endNs != null && startNs >= endNs) {
            System.out.printf("[INFO] No work to do: now (%s) >= --end_ts (%s). Exiting.%n",
                    nanosToIso(startNs), cfg.endTs);
            System.exit(0);
        }
        return startNs;
    }

    /** Latest timestamp (nanos) across every enabled table, or null if all empty. */
    private Long latestAcrossTables() {
        Long latest = null;
        if (cfg.tradesProcesses > 0) {
            latest = maxOf(latest, readMaxTimestampNanos(cfg.tradesTable(), false));
        }
        if (cfg.marketDataProcesses > 0) {
            latest = maxOf(latest, readMaxTimestampNanos(cfg.marketDataTable(), true));
        }
        if (cfg.coreProcesses > 0) {
            latest = maxOf(latest, readMaxTimestampNanos(cfg.corePriceTable(), true));
        }
        return latest;
    }

    private static long nowNs() {
        return Instant.now().toEpochMilli() * 1_000_000L;
    }

    private static String nanosToIso(long ns) {
        return Instant.ofEpochSecond(ns / NANOS_PER_SEC, ns % NANOS_PER_SEC).toString();
    }

    private static Long maxOf(Long a, Long b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return Math.max(a, b);
    }

    /** max(timestamp) from a table as nanos, or null. {@code micros} converts a TIMESTAMP column. */
    private Long readMaxTimestampNanos(String table, boolean micros) {
        final AtomicLong out = new AtomicLong(Long.MIN_VALUE);
        try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
            client.connect();
            client.execute("SELECT max(timestamp) FROM " + table,
                    new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batch.forEachRow(row -> out.set(row.getLongValue(0)));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                        }
                    });
        } catch (Exception e) {
            return null;
        }
        long v = out.get();
        if (v == Long.MIN_VALUE || v <= 0) {
            return null;
        }
        long ns = micros ? v * 1000L : v;
        long nowNs = Instant.now().toEpochMilli() * 1_000_000L;
        long year = 365L * 24 * 3600 * NANOS_PER_SEC;
        if (Math.abs(ns - nowNs) > 2 * year) {
            return null;
        }
        return ns;
    }

    /** Incremental: seed initial mids from the last stored trade prices. */
    private void seedInitialMidFromDb() {
        System.out.println("[INFO] incremental: seeding mids from last stored prices in " + cfg.tradesTable());
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < pairs.size(); i++) {
            idx.put(pairs.get(i).symbol, i);
        }
        try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
            client.connect();
            client.execute("SELECT symbol, price FROM " + cfg.tradesTable()
                            + " LATEST ON timestamp PARTITION BY symbol",
                    new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batch.forEachRow(row -> {
                                Integer i = idx.get(String.valueOf(row.getSymbol(0)));
                                if (i == null) {
                                    return;
                                }
                                double px = row.getDoubleValue(1);
                                if (px > 0) {
                                    FxPair p = pairs.get(i);
                                    initialMid[i] = FxUniverse.quantizeToPip(px, p.pip, p.precision);
                                    p.resetBracket(px * 0.99, px * 1.01);
                                }
                            });
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            System.err.println("[incremental] query error: " + message);
                        }
                    });
        } catch (Exception e) {
            System.err.println("[incremental] could not read prior state (" + e.getMessage() + ").");
        }
    }

    private static long alignToSecond(long ns) {
        return (ns / NANOS_PER_SEC) * NANOS_PER_SEC;
    }

    private static long isoToNanos(String iso) {
        OffsetDateTime odt;
        try {
            odt = OffsetDateTime.parse(iso);
        } catch (Exception e) {
            odt = LocalDateTime.parse(iso).atOffset(ZoneOffset.UTC);
        }
        Instant in = odt.toInstant();
        return in.getEpochSecond() * NANOS_PER_SEC + in.getNano();
    }
}
