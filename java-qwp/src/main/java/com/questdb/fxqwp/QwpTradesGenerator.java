package com.questdb.fxqwp;

import io.questdb.client.Sender;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Synthetic FX trade generator that ingests into QuestDB over QWP (WebSocket),
 * with HA failover across a fleet of hosts that share one credential set and one
 * TLS setting.
 *
 * <p>This is the simplified, trades-only sibling of the Python FX data generator.
 * It produces rows for a single {@code qwp_trades} table whose schema is identical
 * to the Python {@code fx_trades} table. There are no order books, no
 * {@code core_price}, and no materialized views.
 *
 * <p>Price realism is preserved the same way the Python generator does it: each
 * pair's mid follows a controlled pip random walk inside a Yahoo-anchored bracket,
 * and trades within a second are interpolated between the open and close states so
 * {@code close(t) == open(t+1)} holds across seconds.
 *
 * <p><b>Parallelism.</b> {@code --processes N} runs N worker threads (Java has no
 * GIL, so threads, not OS processes). Symbols are split across workers by a
 * popularity "snake draft" from both ends of the liquidity ranking: pick k goes to
 * worker {@code k % N} and takes one symbol from the most-popular end and one from
 * the least-popular end, so each worker gets a balanced mix. A worker owns its
 * symbols end-to-end (own state, own {@link Sender}), so per-symbol price
 * continuity is preserved and no two workers ever touch the same symbol. The global
 * {@code --total_market_data_events} cap is shared via an atomic counter. A
 * Python-style WAL monitor pauses all workers when the table's sequencer gets ahead
 * of the writer, and resumes when it has caught up.
 */
public final class QwpTradesGenerator {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final long NANOS_PER_SEC = 1_000_000_000L;
    private static final String ENTERPRISE_POLICY =
            "TO parquet 1 hour, DROP NATIVE 2 days, DROP LOCAL 3 months";

    private final Cli cfg;
    private final List<FxPair> pairs = FxUniverse.defaultPairs();
    private final String[] leiPool;

    // Shared per-pair price state, indexed in lockstep with `pairs`. Each worker
    // only reads/writes the indices it owns, so the disjoint partition makes this
    // safe without locking. The Yahoo refresher only touches FxPair.low/high (volatile).
    private final double[] mids;
    private final int[] spreadPips;
    private final int totalAllWeight; // sum of (11 - rank) across all pairs

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicLong emitted = new AtomicLong(0);

    private QwpTradesGenerator(Cli cfg) {
        this.cfg = cfg;
        this.leiPool = FxUniverse.generateLeiPool(cfg.leiPoolSize);
        int n = pairs.size();
        this.mids = new double[n];
        this.spreadPips = new int[n];
        int acc = 0;
        for (int i = 0; i < n; i++) {
            FxPair p = pairs.get(i);
            this.mids[i] = FxUniverse.quantizeToPip(p.midOfBracket(), p.pip, p.precision);
            this.spreadPips[i] = 4; // start at 4 pips, like load_initial_state_from_brackets
            acc += (11 - p.rank);
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
            // Print a readable failure instead of letting the exception propagate as
            // an opaque "exception occurred while executing the Java class" from the
            // Maven exec plugin. Common causes: host unreachable, TLS/auth, or a
            // client/server QWP protocol-version mismatch.
            System.err.printf("[FATAL] %s: %s%n", e.getClass().getSimpleName(), e.getMessage());
            for (Throwable c = e.getCause(); c != null && c != c.getCause(); c = c.getCause()) {
                System.err.printf("[FATAL]   caused by %s: %s%n", c.getClass().getSimpleName(), c.getMessage());
            }
            System.exit(1);
        }
    }

    private void run() throws Exception {
        System.out.println("=== qwp-fx-trades generator ===");
        System.out.printf("mode=%s  hosts=%s  tls=%s  table=%s  processes=%d%n",
                cfg.mode, cfg.hosts, cfg.tls, cfg.tableName(), cfg.processes);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        // 1) Reference data. Incremental continues from the last stored prices (and
        // skips Yahoo, like the Python --incremental path); otherwise we anchor
        // brackets on live Yahoo mids (unless --no_yahoo) and start at the bracket mid.
        if (cfg.incremental) {
            seedStateFromDb();
        } else {
            if (!cfg.noYahoo) {
                new YahooFinance().refreshBrackets(pairs, 1.0);
            }
            for (int i = 0; i < pairs.size(); i++) {
                FxPair p = pairs.get(i);
                mids[i] = FxUniverse.quantizeToPip(p.midOfBracket(), p.pip, p.precision);
            }
        }

        // 2) Schema (DDL over QWP query client, failover-aware).
        createTable();

        // 3) Timestamp safety: continue strictly after the last row already stored.
        long startNs = resolveStartNanos();
        Long endNs = cfg.endTs == null ? null : isoToNanos(cfg.endTs);
        if (endNs != null && endNs <= startNs) {
            System.err.printf("[ERROR] --end_ts (%s) is not after the start point (%s). Aborting.%n",
                    cfg.endTs, Instant.ofEpochSecond(0, startNs));
            return;
        }

        // 4) Partition symbols across workers, then run the worker fleet.
        List<List<Integer>> assignment = snakeDraft(cfg.processes);
        logAssignment(assignment);

        Thread walMonitor = startWalMonitor();
        boolean realtime = "real-time".equals(cfg.mode);
        Thread yahooRefresher = (realtime && !cfg.noYahoo && !cfg.incremental)
                ? startYahooRefresher() : null;

        long wallStartMs = System.currentTimeMillis();
        long t0 = System.nanoTime();

        // Per-second throughput sampling is only enabled with --run_secs (a bounded
        // throughput test); otherwise we just report total elapsed time at the end.
        // Both the sampler and the wall-clock cap hang off --run_secs.
        List<Long> perSecond = new ArrayList<>();
        Thread sampler = cfg.runSecs > 0 ? startThroughputSampler(perSecond) : null;
        Thread deadline = cfg.runSecs > 0 ? startDeadline(cfg.runSecs) : null;

        List<Thread> workers = new ArrayList<>();
        for (int w = 0; w < cfg.processes; w++) {
            Thread th = new Thread(new Worker(w, assignment.get(w), startNs, endNs, wallStartMs),
                    "qwp-worker-" + w);
            workers.add(th);
            th.start();
        }
        for (Thread th : workers) {
            th.join();
        }

        // Stop the background helpers and wrap up.
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
        long total = emitted.get();
        printThroughputSummary(perSecond);
        System.out.printf("[DONE] emitted %d trades in %.2fs = %,.0f trades/sec (%d workers).%n",
                total, secs, secs > 0 ? total / secs : 0.0, cfg.processes);
    }

    // ---------------------------------------------------------------- worker fleet

    /**
     * Split pair indices across {@code p} workers via a popularity snake draft from
     * both ends. Pick k (worker {@code k % p}) takes the next most-popular and the
     * next least-popular symbol, balancing trade-weight load across workers.
     */
    private List<List<Integer>> snakeDraft(int p) {
        int n = pairs.size();
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        // Most popular first: lower rank first, ties keep original order.
        Arrays.sort(order, Comparator.comparingInt((Integer i) -> pairs.get(i).rank)
                .thenComparingInt(i -> i));

        List<List<Integer>> res = new ArrayList<>();
        for (int w = 0; w < p; w++) {
            res.add(new ArrayList<>());
        }
        int top = 0, bottom = n - 1, k = 0;
        while (top <= bottom) {
            List<Integer> bucket = res.get(k % p);
            bucket.add(order[top++]);          // most-popular end
            if (top <= bottom) {
                bucket.add(order[bottom--]);   // least-popular end
            }
            k++;
        }
        return res;
    }

    private void logAssignment(List<List<Integer>> assignment) {
        if (cfg.processes == 1) {
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
            System.out.printf("[INFO] worker %d owns %d symbols: %s%n",
                    w, assignment.get(w).size(), sb);
        }
    }

    /** One ingest worker thread: owns a disjoint symbol set and its own QWP sender. */
    private final class Worker implements Runnable {
        private final int id;
        private final int[] owned;       // global pair indices this worker owns
        private final int[] cumW;        // cumulative weights over owned
        private final int totalW;
        private final int wMin;          // per-second trade floor for this worker
        private final int wMax;          // per-second trade ceiling for this worker
        private final long startNs;
        private final Long endNs;
        private final long wallStartMs;
        // reusable per-second buffers (indexed by position within `owned`)
        private final double[] oMid;
        private final double[] cMid;
        private final int[] oSp;
        private final int[] cSp;
        private boolean stop = false;

        Worker(int id, List<Integer> ownedList, long startNs, Long endNs, long wallStartMs) {
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
            // This worker's slice of the global per-second throughput, by weight share.
            double share = totalAllWeight > 0 ? (double) totalW / totalAllWeight : 1.0;
            this.wMin = Math.max(1, (int) Math.round(cfg.tradesMinPerSec * share));
            this.wMax = Math.max(wMin, (int) Math.round(cfg.tradesMaxPerSec * share));
            this.oMid = new double[owned.length];
            this.cMid = new double[owned.length];
            this.oSp = new int[owned.length];
            this.cSp = new int[owned.length];
        }

        @Override
        public void run() {
            try (Sender sender = buildSender(id)) {
                boolean realtime = "real-time".equals(cfg.mode);
                long secondStartNs = alignToSecond(startNs);
                long secIdx = 0;
                long lastCommitMs = System.currentTimeMillis();
                while (running.get() && !stop && !budgetReached() && !pastEnd(secondStartNs, endNs)) {
                    waitWhilePaused();
                    if (!running.get()) {
                        break;
                    }
                    int n = ThreadLocalRandom.current().nextInt(wMin, wMax + 1);
                    emitSecond(sender, secondStartNs, n, realtime ? 7.0 : 5.0);
                    secondStartNs += NANOS_PER_SEC;

                    // Commit (transactional flush) on the configured cadence. Deferred
                    // frames have been streaming via auto-flush; this is the commit point.
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
                sender.flush(); // final commit of any deferred rows
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.printf("[worker %d] FATAL: %s%n", id, e.getMessage());
            }
        }

        private void emitSecond(Sender sender, long secondStartNs, int nTrades, double driftPips) {
            for (int j = 0; j < owned.length; j++) {
                int idx = owned[j];
                FxPair p = pairs.get(idx);
                oMid[j] = mids[idx];
                oSp[j] = spreadPips[idx];
                cMid[j] = FxUniverse.evolveMid(oMid[j], p, driftPips);
                cSp[j] = FxUniverse.evolveSpreadPips(oSp[j]);
            }

            long[] offsets = new long[nTrades];
            for (int k = 0; k < nTrades; k++) {
                offsets[k] = ThreadLocalRandom.current().nextLong(NANOS_PER_SEC);
            }
            Arrays.sort(offsets);

            for (long offset : offsets) {
                long ts = secondStartNs + offset;
                if (endNs != null && ts >= endNs) {
                    break;
                }
                if (!claimSlot()) {
                    stop = true;
                    break;
                }
                int j = pickOwned();
                int idx = owned[j];
                FxPair p = pairs.get(idx);
                double f = (double) offset / NANOS_PER_SEC;
                double mid = FxUniverse.quantizeToPip(oMid[j] + (cMid[j] - oMid[j]) * f, p.pip, p.precision);
                int sp = (int) Math.round(oSp[j] + (cSp[j] - oSp[j]) * f);
                sp = Math.max(1, Math.min(8, sp));
                double bid = FxUniverse.quantizeToPip(mid - sp * p.pip / 2.0, p.pip, p.precision);
                double ask = FxUniverse.quantizeToPip(mid + sp * p.pip / 2.0, p.pip, p.precision);
                emitTrade(sender, ts, p, bid, ask);
            }

            // Carry close state forward as the next second's open state (per owned symbol).
            for (int j = 0; j < owned.length; j++) {
                mids[owned[j]] = cMid[j];
                spreadPips[owned[j]] = cSp[j];
            }
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
            while (paused.get() && running.get()) {
                Thread.sleep(5000);
            }
        }
    }

    /**
     * Per-second throughput sampler: every wall-clock second, print rows/sec and
     * record it. Gives a per-second view of ingest rate over the run; the final
     * summary reports min/median/avg/max. (The first and last samples can be partial
     * — startup ramp and the final fractional second.)
     */
    private Thread startThroughputSampler(List<Long> perSecond) {
        Thread t = new Thread(() -> {
            long last = 0;
            long sec = 0;
            try {
                while (running.get()) {
                    Thread.sleep(1000);
                    long now = emitted.get();
                    long delta = now - last;
                    last = now;
                    sec++;
                    perSecond.add(delta);
                    System.out.printf("[rate] t=%ds  %,d rows/sec  (cum %,d)%n", sec, delta, now);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "qwp-throughput-sampler");
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** Stops the run after a fixed wall-clock duration (for bounded throughput tests). */
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
     * Claim one slot against the global total. Returns false once the cap is hit so
     * the calling worker stops. The decrement keeps the final emitted count exact
     * even with multiple workers racing.
     */
    private boolean claimSlot() {
        if (cfg.totalTrades <= 0) {
            emitted.incrementAndGet();
            return true;
        }
        long s = emitted.getAndIncrement();
        if (s >= cfg.totalTrades) {
            emitted.decrementAndGet();
            return false;
        }
        return true;
    }

    private boolean budgetReached() {
        return cfg.totalTrades > 0 && emitted.get() >= cfg.totalTrades;
    }

    private boolean pastEnd(long secondStartNs, Long endNs) {
        return endNs != null && secondStartNs >= endNs;
    }

    private void emitTrade(Sender sender, long ts, FxPair p, double bid, double ask) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        boolean buy = rnd.nextBoolean();
        boolean passive = rnd.nextDouble() < 0.4;
        double quantity = FxUniverse.tradeSizeLognormal();

        // Crossing price relative to top of book: passive prints close to touch,
        // aggressive walks a few pips through it. (No depth ladder in this model.)
        int slipPips = passive ? rnd.nextInt(0, 3) : rnd.nextInt(0, 7);
        double price = buy ? ask + slipPips * p.pip : bid - slipPips * p.pip;
        price = FxUniverse.quantizeToPip(price, p.pip, p.precision);

        String ecn = FxUniverse.ECNS[rnd.nextInt(FxUniverse.ECNS.length)];
        String counterparty = leiPool[rnd.nextInt(leiPool.length)];

        // 128-bit ids straight from ThreadLocalRandom (lo, hi). These only need to be
        // unique for the synthetic data and the DEDUP key, not cryptographically
        // unpredictable, so we skip UUID.randomUUID()'s shared/contended SecureRandom.
        sender.table(cfg.tableName())
                .symbol("symbol", p.symbol)
                .symbol("ecn", ecn)
                .symbol("side", buy ? "buy" : "sell")
                .symbol("counterparty", counterparty)
                .boolColumn("passive", passive)
                .doubleColumn("price", price)
                .doubleColumn("quantity", quantity)
                .uuidColumn("trade_id", rnd.nextLong(), rnd.nextLong())
                .uuidColumn("order_id", rnd.nextLong(), rnd.nextLong())
                .at(ts, ChronoUnit.NANOS);
    }

    // ---------------------------------------------------------------- transport / DDL

    private Sender buildSender(int workerId) {
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
        // Each worker needs its own store-and-forward dir and sender id. The sender
        // does not mkdir -p its parent, so we create the full path up front.
        String sfPath = cfg.sfDir + "/w" + workerId;
        try {
            Files.createDirectories(Paths.get(sfPath));
        } catch (Exception e) {
            System.err.printf("[w%d] WARN: could not pre-create sf dir %s: %s%n",
                    workerId, sfPath, e.getMessage());
        }

        // Transactional mode is the only mode: auto-flush sends deferred frames
        // (FLAG_DEFER_COMMIT) that the server appends but does not commit; the
        // server commits a single WAL transaction only on an explicit flush(). The
        // worker calls flush() on a fixed cadence (--commit_interval_ms), so commit
        // size is decoupled from frame size: many byte-bounded frames commit as one
        // transaction. That keeps the sequencer/writer txn gap tiny.
        //
        // Auto-flush is bounded by BYTES, not rows, so every WebSocket frame stays
        // safely under the QWP server's frame cap (~1MB) regardless of how wide the
        // rows are. This removes the "1009 Message Too Big" failure class at the
        // source. The high row cap is a non-binding safety net; bytes trip first.
        final AtomicLong lastConnLogMs = new AtomicLong(0);
        b.storeAndForwardDir(sfPath)
                .senderId(cfg.senderId + "-" + workerId)
                .transactional(true)
                .reconnectMaxDurationMillis(300_000)
                .reconnectInitialBackoffMillis(100)
                .reconnectMaxBackoffMillis(5_000)
                .autoFlushBytes(cfg.autoFlushBytes)
                .autoFlushRows(1_000_000)
                .autoFlushIntervalMillis(1_000)
                .errorHandler(error -> System.err.printf("[w%d %s] BATCH ERROR: category=%s table=%s msg=%s%n",
                        workerId, LocalDateTime.now().format(FMT), error.getCategory(),
                        error.getTableName(), error.getServerMessage()))
                .connectionListener(event -> {
                    // Throttle connection logging to at most once/second per worker so a
                    // transient reconnect blip cannot flood the output.
                    long now = System.currentTimeMillis();
                    long prev = lastConnLogMs.get();
                    if (now - prev >= 1000 && lastConnLogMs.compareAndSet(prev, now)) {
                        System.out.printf("[w%d %s] CONNECTION: %s host=%s:%d%n",
                                workerId, LocalDateTime.now().format(FMT), event.getKind(),
                                event.getHost(), event.getPort());
                    }
                });
        return b.build();
    }

    private void createTable() {
        String retention = "";
        if (cfg.shortTtl) {
            retention = cfg.enterprise
                    ? " STORAGE POLICY(" + ENTERPRISE_POLICY + ")"
                    : " TTL 1 MONTH";
        }
        String ddl = "CREATE TABLE IF NOT EXISTS " + cfg.tableName() + " ("
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
                + ") timestamp(timestamp) PARTITION BY HOUR" + retention
                + " DEDUP UPSERT KEYS(timestamp, trade_id)";

        System.out.println("[INFO] ensuring table " + cfg.tableName() + " exists ...");
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
                    System.err.println("[DDL] ERROR: " + message);
                }

                @Override
                public void onExecDone(short opType, long rowsAffected) {
                    System.out.println("[DDL] table ready.");
                }
            });
        }
    }

    // ---------------------------------------------------------------- WAL backpressure

    /**
     * WAL backpressure: poll {@code wal_tables()} every 5s; if the sequencer gets
     * more than the threshold transactions ahead of the writer, pause all workers,
     * and resume only once the writer has fully caught up. Threshold is
     * {@code 3 x processes} for more than 2 workers and {@code 5 x processes} at or
     * below 2 — small values that stay sane because transactional commits keep the
     * transaction count low (only a few large txns in flight).
     */
    private Thread startWalMonitor() {
        final int threshold = (cfg.processes > 2 ? 3 : 5) * cfg.processes;
        Thread t = new Thread(() -> {
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
                client.connect();
                while (running.get()) {
                    long[] lag = queryWalLag(client);
                    if (lag != null) {
                        long diff = lag[0] - lag[1]; // sequencerTxn - writerTxn
                        if (diff > threshold && paused.compareAndSet(false, true)) {
                            System.out.printf("[wal] lag %d > %d, pausing ingestion%n", diff, threshold);
                        } else if (lag[0] == lag[1] && paused.compareAndSet(true, false)) {
                            System.out.println("[wal] caught up, resuming ingestion");
                        }
                    }
                    for (int s = 0; s < 5 && running.get(); s++) {
                        Thread.sleep(1000);
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

    private long[] queryWalLag(QwpQueryClient client) {
        final long[] out = {Long.MIN_VALUE, Long.MIN_VALUE};
        try {
            client.execute("SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = '"
                            + cfg.tableName() + "'",
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

    private Thread startYahooRefresher() {
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
                    yf.refreshBrackets(pairs, 1.0);
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
     * Decide where to start the data clock. We continue strictly after the latest
     * row already in the table to keep the series continuous and avoid clobbering
     * existing data; an explicit --start_ts (faster-than-life) wins when provided.
     */
    private long resolveStartNanos() {
        if ("faster-than-life".equals(cfg.mode) && cfg.startTs != null) {
            return isoToNanos(cfg.startTs);
        }
        long nowNs = Instant.now().toEpochMilli() * 1_000_000L;
        Long latest = readMaxTimestampNanos();
        if (latest == null) {
            return nowNs;
        }
        long after = latest + 1_000L; // 1 microsecond after the last row
        return "faster-than-life".equals(cfg.mode) ? after : Math.max(nowNs, after);
    }

    /**
     * Read {@code max(timestamp)} (nanos) from the table, or null if empty/unavailable.
     * Defensive against unit surprises: a value implausibly far from "now" is ignored.
     */
    private Long readMaxTimestampNanos() {
        final AtomicLong out = new AtomicLong(Long.MIN_VALUE);
        try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
            client.connect();
            client.execute("SELECT max(timestamp) FROM " + cfg.tableName(),
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
                            // empty/missing -> leave sentinel; we treat as "no data".
                        }
                    });
        } catch (Exception e) {
            return null;
        }
        long v = out.get();
        if (v == Long.MIN_VALUE || v <= 0) {
            return null;
        }
        long nowNs = Instant.now().toEpochMilli() * 1_000_000L;
        long year = 365L * 24 * 3600 * NANOS_PER_SEC;
        if (Math.abs(v - nowNs) > 2 * year) {
            System.out.printf("[WARN] stored max(timestamp)=%d looks off vs now; ignoring for start.%n", v);
            return null;
        }
        return v;
    }

    /**
     * Incremental start: seed each pair's mid from the last stored trade price
     * ({@code LATEST ON timestamp PARTITION BY symbol}) and recentre its bracket
     * around that price, so the series continues smoothly from existing data
     * without consulting Yahoo. Pairs with no stored data keep their template mid.
     */
    private void seedStateFromDb() {
        System.out.println("[INFO] incremental: seeding state from last stored prices in " + cfg.tableName());
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < pairs.size(); i++) {
            idx.put(pairs.get(i).symbol, i);
        }
        final int[] seeded = {0};
        try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg.queryClientConfig())) {
            client.connect();
            client.execute("SELECT symbol, price FROM " + cfg.tableName()
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
                                    mids[i] = FxUniverse.quantizeToPip(px, p.pip, p.precision);
                                    p.low = px * 0.99;
                                    p.high = px * 1.01;
                                    seeded[0]++;
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
            System.err.println("[incremental] could not read prior state (" + e.getMessage()
                    + "); starting from template brackets.");
        }
        System.out.printf("[INFO] incremental: seeded %d/%d pairs from stored data.%n",
                seeded[0], pairs.size());
    }

    private static long alignToSecond(long ns) {
        return (ns / NANOS_PER_SEC) * NANOS_PER_SEC;
    }

    private static long isoToNanos(String iso) {
        OffsetDateTime odt;
        try {
            odt = OffsetDateTime.parse(iso);
        } catch (Exception e) {
            // Accept a trailing-Z-less local form too, assume UTC.
            odt = LocalDateTime.parse(iso).atOffset(ZoneOffset.UTC);
        }
        Instant in = odt.toInstant();
        return in.getEpochSecond() * NANOS_PER_SEC + in.getNano();
    }
}
