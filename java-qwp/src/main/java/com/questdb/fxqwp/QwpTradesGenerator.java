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
    private static final String ENTERPRISE_POLICY =
            "TO parquet 1 hour, DROP NATIVE 2 days, DROP LOCAL 3 months";

    private enum Kind { TRADES, MARKET_DATA }

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
    private final AtomicLong tradesRows = new AtomicLong(0);
    private final AtomicLong mdRows = new AtomicLong(0);

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

        // 2) Schema for each enabled table.
        createTables();

        // 3) Start point (data clock).
        long startNs = resolveStartNanos();
        Long endNs = cfg.endTs == null ? null : isoToNanos(cfg.endTs);
        if (endNs != null && endNs <= startNs) {
            System.err.printf("[ERROR] --end_ts (%s) is not after the start point. Aborting.%n", cfg.endTs);
            return;
        }

        // 4) Build the two pools and run them.
        boolean realtime = "real-time".equals(cfg.mode);
        long wallStartMs = System.currentTimeMillis();
        long t0 = System.nanoTime();

        List<Long> perSecond = new ArrayList<>();
        Thread sampler = cfg.runSecs > 0 ? startThroughputSampler(perSecond) : null;
        Thread deadline = cfg.runSecs > 0 ? startDeadline(cfg.runSecs) : null;
        Thread walMonitor = startWalMonitor();
        Thread yahooRefresher = (realtime && !cfg.noYahoo && !cfg.incremental)
                ? startYahooRefresher(wallStartMs) : null;

        List<Thread> workers = new ArrayList<>();
        workers.addAll(spawnPool(Kind.TRADES, cfg.tradesProcesses, cfg.tradesMinPerSec,
                cfg.tradesMaxPerSec, startNs, endNs, wallStartMs));
        workers.addAll(spawnPool(Kind.MARKET_DATA, cfg.marketDataProcesses, cfg.marketDataMinEps,
                cfg.marketDataMaxEps, startNs, endNs, wallStartMs));
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
        if (walMonitor != null) {
            walMonitor.join(2000);
        }

        double secs = (System.nanoTime() - t0) / 1e9;
        printThroughputSummary(perSecond);
        System.out.printf("[DONE] in %.2fs: trades=%,d (%,.0f/s), market_data=%,d (%,.0f/s)%n",
                secs, tradesRows.get(), secs > 0 ? tradesRows.get() / secs : 0.0,
                mdRows.get(), secs > 0 ? mdRows.get() / secs : 0.0);
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
        return kind == Kind.TRADES ? "t" : "md";
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
        // market_data: reusable DoubleArrays keyed by level count
        private final Map<Integer, DoubleArray> bidArrays = new HashMap<>();
        private final Map<Integer, DoubleArray> askArrays = new HashMap<>();
        private boolean stop = false;

        Worker(Kind kind, int id, List<Integer> ownedList, int poolMin, int poolMax,
               long startNs, Long endNs, long wallStartMs) {
            this.kind = kind;
            this.id = id;
            this.startNs = startNs;
            this.endNs = endNs;
            this.wallStartMs = wallStartMs;
            this.owned = ownedList.stream().mapToInt(Integer::intValue).toArray();
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
            for (int j = 0; j < owned.length; j++) {
                int g = owned[j];
                walkRng[j] = new SplittableRandom(WALK_SEED_BASE + GOLDEN * g);
                walkMid[j] = initialMid[g];
                walkSp[j] = 4;
            }
        }

        @Override
        public void run() {
            String table = kind == Kind.TRADES ? cfg.tradesTable() : cfg.marketDataTable();
            boolean realtime = "real-time".equals(cfg.mode);
            try (Sender sender = buildSender(kind, id)) {
                long base = alignToSecond(startNs);
                long secondStartNs = base;
                long secIdx = 0;
                long lastCommitMs = System.currentTimeMillis();
                while (running.get() && !stop && !capReached() && !pastEnd(secondStartNs, endNs)) {
                    waitWhilePaused();
                    if (!running.get()) {
                        break;
                    }
                    // Data-second index, identical across both pools for the same
                    // secondStartNs -> deterministic bracket lookup -> consistent walk.
                    long k = (secondStartNs - base) / NANOS_PER_SEC;
                    emitSecond(sender, table, secondStartNs, realtime ? 7.0 : 5.0, k);
                    secondStartNs += NANOS_PER_SEC;

                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastCommitMs >= cfg.commitIntervalMs) {
                        sender.flush();
                        lastCommitMs = nowMs;
                    }
                    if (realtime) {
                        secIdx++;
                        long sleepMs = (wallStartMs + secIdx * 1000L) - System.currentTimeMillis();
                        if (sleepMs > 0) {
                            Thread.sleep(sleepMs);
                        }
                    }
                }
                sender.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.printf("[%s worker %d] FATAL: %s%n", tag(kind), id, e.getMessage());
            } finally {
                bidArrays.values().forEach(DoubleArray::close);
                askArrays.values().forEach(DoubleArray::close);
            }
        }

        private void emitSecond(Sender sender, String table, long secondStartNs, double driftPips, long k) {
            // Advance the deterministic walk once per owned symbol this second (keeps the
            // two pools in lockstep regardless of which events land on which symbol).
            // Bracket is looked up at second k, so both pools clamp identically.
            for (int j = 0; j < owned.length; j++) {
                FxPair p = pairs.get(owned[j]);
                oMid[j] = walkMid[j];
                oSp[j] = walkSp[j];
                double lo = p.lowAt(k);
                double hi = p.highAt(k);
                cMid[j] = FxUniverse.evolveMid(oMid[j], p, driftPips, lo, hi, walkRng[j]);
                cSp[j] = FxUniverse.evolveSpreadPips(oSp[j], walkRng[j]);
            }

            int nEvents = ThreadLocalRandom.current().nextInt(wMin, wMax + 1);
            long[] offsets = new long[nEvents];
            for (int e = 0; e < nEvents; e++) {
                offsets[e] = ThreadLocalRandom.current().nextLong(NANOS_PER_SEC);
            }
            Arrays.sort(offsets);

            for (long offset : offsets) {
                long ts = secondStartNs + offset;
                if (endNs != null && ts >= endNs) {
                    break;
                }
                int j = pickOwned();
                FxPair p = pairs.get(owned[j]);
                double f = (double) offset / NANOS_PER_SEC;
                double mid = FxUniverse.quantizeToPip(oMid[j] + (cMid[j] - oMid[j]) * f, p.pip, p.precision);
                int sp = Math.max(1, Math.min(8, (int) Math.round(oSp[j] + (cSp[j] - oSp[j]) * f)));
                double bid = FxUniverse.quantizeToPip(mid - sp * p.pip / 2.0, p.pip, p.precision);
                double ask = FxUniverse.quantizeToPip(mid + sp * p.pip / 2.0, p.pip, p.precision);

                if (kind == Kind.TRADES) {
                    if (!emitOrder(sender, table, ts, p, bid, ask)) {
                        stop = true;
                        break;
                    }
                } else {
                    if (capReached()) {
                        stop = true;
                        break;
                    }
                    emitSnapshot(sender, table, ts, p, bid, ask);
                    mdRows.incrementAndGet();
                }
            }

            for (int j = 0; j < owned.length; j++) {
                walkMid[j] = cMid[j];
                walkSp[j] = cSp[j];
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
            long lastT = 0, lastMd = 0, sec = 0;
            try {
                while (running.get()) {
                    Thread.sleep(1000);
                    long nowT = tradesRows.get();
                    long nowMd = mdRows.get();
                    long dT = nowT - lastT;
                    long dMd = nowMd - lastMd;
                    lastT = nowT;
                    lastMd = nowMd;
                    sec++;
                    perSecond.add(dT + dMd);
                    System.out.printf("[rate] t=%ds  trades %,d/s  md %,d/s  (total %,d/s)%n",
                            sec, dT, dMd, dT + dMd);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "qwp-throughput-sampler");
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
        return kind == Kind.TRADES ? pausedTrades : pausedMd;
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

    private long resolveStartNanos() {
        if ("faster-than-life".equals(cfg.mode) && cfg.startTs != null) {
            return isoToNanos(cfg.startTs);
        }
        long nowNs = Instant.now().toEpochMilli() * 1_000_000L;
        Long latest = null;
        if (cfg.tradesProcesses > 0) {
            latest = maxOf(latest, readMaxTimestampNanos(cfg.tradesTable(), false));
        }
        if (cfg.marketDataProcesses > 0) {
            latest = maxOf(latest, readMaxTimestampNanos(cfg.marketDataTable(), true));
        }
        if (latest == null) {
            return nowNs;
        }
        long after = latest + 1_000L;
        return "faster-than-life".equals(cfg.mode) ? after : Math.max(nowNs, after);
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
