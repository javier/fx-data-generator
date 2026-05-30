package com.questdb.fxqwp;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-worker synthetic FX trade generator that ingests into QuestDB over QWP
 * (WebSocket), with HA failover across a fleet of hosts that share one credential
 * set and one TLS setting.
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
 */
public final class QwpTradesGenerator {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final long NANOS_PER_SEC = 1_000_000_000L;
    private static final String ENTERPRISE_POLICY =
            "TO parquet 1 hour, DROP NATIVE 2 days, DROP LOCAL 3 months";

    private final Cli cfg;
    private final List<FxPair> pairs = FxUniverse.defaultPairs();
    private final String[] leiPool;

    // Per-pair price state, indexed in lockstep with `pairs`.
    private final double[] mids;
    private final int[] spreadPips;
    // Cumulative liquidity weights for symbol selection (weight = 11 - rank).
    private final int[] cumWeights;
    private final int totalWeight;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private long emitted = 0;

    private QwpTradesGenerator(Cli cfg) {
        this.cfg = cfg;
        this.leiPool = FxUniverse.generateLeiPool(cfg.leiPoolSize);
        int n = pairs.size();
        this.mids = new double[n];
        this.spreadPips = new int[n];
        this.cumWeights = new int[n];
        int acc = 0;
        for (int i = 0; i < n; i++) {
            FxPair p = pairs.get(i);
            this.mids[i] = FxUniverse.quantizeToPip(p.midOfBracket(), p.pip, p.precision);
            this.spreadPips[i] = 4; // start at 4 pips, like load_initial_state_from_brackets
            acc += (11 - p.rank);
            this.cumWeights[i] = acc;
        }
        this.totalWeight = acc;
    }

    public static void main(String[] args) throws Exception {
        Cli cfg;
        try {
            cfg = Cli.parse(args);
        } catch (IllegalArgumentException e) {
            System.exit(2);
            return;
        }
        new QwpTradesGenerator(cfg).run();
    }

    private void run() throws Exception {
        System.out.println("=== qwp-fx-trades generator ===");
        System.out.printf("mode=%s  hosts=%s  tls=%s  table=%s%n",
                cfg.mode, cfg.hosts, cfg.tls, cfg.tableName());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        // 1) Reference data. Incremental continues from the last stored prices
        // (and skips Yahoo, like the Python --incremental path); otherwise we anchor
        // brackets on live Yahoo mids (unless --no-yahoo) and start at the bracket mid.
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
            System.err.printf("[ERROR] --end-ts (%s) is not after the start point (%s). Aborting.%n",
                    cfg.endTs, Instant.ofEpochSecond(0, startNs));
            return;
        }

        // 4) Build the HA sender and ingest.
        // Timing spans the ingest loop + final flush only (the flush blocks on the
        // server ack), so the reported rate is end-to-end client->server throughput,
        // excluding JVM startup, Yahoo, and DDL.
        long t0 = System.nanoTime();
        try (Sender sender = buildSender()) {
            if ("real-time".equals(cfg.mode)) {
                runRealTime(sender, startNs, endNs);
            } else {
                runFasterThanLife(sender, startNs, endNs);
            }
            sender.flush();
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("[DONE] emitted %d trades in %.2fs = %,.0f trades/sec.%n",
                emitted, secs, secs > 0 ? emitted / secs : 0.0);
    }

    // ---------------------------------------------------------------- ingest loops

    private void runRealTime(Sender sender, long startNs, Long endNs) throws InterruptedException {
        System.out.println("[INFO] real-time mode: wall-clock aligned, Ctrl+C to stop.");
        long lastYahoo = System.currentTimeMillis();
        long wallStart = System.currentTimeMillis();
        long secIdx = 0;

        // Align the data clock to the start point, then advance one second per wall second.
        long secondStartNs = alignToSecond(startNs);

        while (running.get() && !budgetReached() && !pastEnd(secondStartNs, endNs)) {
            int nTrades = ThreadLocalRandom.current().nextInt(cfg.tradesMinPerSec, cfg.tradesMaxPerSec + 1);
            emitSecond(sender, secondStartNs, nTrades, endNs, 7.0);
            sender.flush();

            // Periodic Yahoo refresh without blocking the data clock for long.
            if (!cfg.noYahoo && (System.currentTimeMillis() - lastYahoo) >= cfg.yahooRefreshSecs * 1000L) {
                new YahooFinance().refreshBrackets(pairs, 1.0);
                lastYahoo = System.currentTimeMillis();
            }

            secIdx++;
            secondStartNs += NANOS_PER_SEC;

            // Sleep until the next wall-clock second boundary.
            long nextTickMs = wallStart + secIdx * 1000L;
            long sleepMs = nextTickMs - System.currentTimeMillis();
            if (sleepMs > 0) {
                Thread.sleep(sleepMs);
            }
        }
    }

    private void runFasterThanLife(Sender sender, long startNs, Long endNs) {
        System.out.println("[INFO] faster-than-life mode: no wall-clock wait, bounded by --total-trades/--end-ts.");
        long secondStartNs = alignToSecond(startNs);
        long lastReport = 0;

        while (running.get() && !budgetReached() && !pastEnd(secondStartNs, endNs)) {
            int nTrades = ThreadLocalRandom.current().nextInt(cfg.tradesMinPerSec, cfg.tradesMaxPerSec + 1);
            emitSecond(sender, secondStartNs, nTrades, endNs, 5.0);
            secondStartNs += NANOS_PER_SEC;

            if (emitted - lastReport >= 100_000) {
                System.out.printf("[ft-life] %d trades emitted (data clock %s)%n",
                        emitted, Instant.ofEpochSecond(0, secondStartNs));
                lastReport = emitted;
            }
        }
    }

    /**
     * Generate and emit one second's worth of trades. The mid is interpolated
     * between the open state (start of second) and the evolved close state, so the
     * series stays continuous across the second boundary.
     */
    private void emitSecond(Sender sender, long secondStartNs, int nTrades, Long endNs, double driftPips) {
        int n = pairs.size();
        double[] openMid = Arrays.copyOf(mids, n);
        int[] openSp = Arrays.copyOf(spreadPips, n);
        double[] closeMid = new double[n];
        int[] closeSp = new int[n];
        for (int i = 0; i < n; i++) {
            closeMid[i] = FxUniverse.evolveMid(openMid[i], pairs.get(i), driftPips);
            closeSp[i] = FxUniverse.evolveSpreadPips(openSp[i]);
        }

        // Trade offsets within the second, sorted so timestamps ascend.
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
            if (budgetReached()) {
                break;
            }
            int idx = pickPairIndex();
            FxPair p = pairs.get(idx);
            double f = (double) offset / NANOS_PER_SEC;

            double mid = FxUniverse.quantizeToPip(openMid[idx] + (closeMid[idx] - openMid[idx]) * f,
                    p.pip, p.precision);
            int sp = (int) Math.round(openSp[idx] + (closeSp[idx] - openSp[idx]) * f);
            sp = Math.max(1, Math.min(8, sp));
            double bid = FxUniverse.quantizeToPip(mid - sp * p.pip / 2.0, p.pip, p.precision);
            double ask = FxUniverse.quantizeToPip(mid + sp * p.pip / 2.0, p.pip, p.precision);

            emitTrade(sender, ts, p, bid, ask);
            emitted++;
        }

        // Carry the close state forward as the next second's open state.
        System.arraycopy(closeMid, 0, mids, 0, n);
        System.arraycopy(closeSp, 0, spreadPips, 0, n);
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

    private int pickPairIndex() {
        int r = ThreadLocalRandom.current().nextInt(totalWeight);
        for (int i = 0; i < cumWeights.length; i++) {
            if (r < cumWeights[i]) {
                return i;
            }
        }
        return cumWeights.length - 1;
    }

    private boolean budgetReached() {
        return cfg.totalTrades > 0 && emitted >= cfg.totalTrades;
    }

    private boolean pastEnd(long secondStartNs, Long endNs) {
        return endNs != null && secondStartNs >= endNs;
    }

    // ---------------------------------------------------------------- transport / DDL

    private Sender buildSender() {
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
        b.storeAndForwardDir(cfg.sfDir)
                .senderId(cfg.senderId)
                .reconnectMaxDurationMillis(300_000)
                .reconnectInitialBackoffMillis(100)
                .reconnectMaxBackoffMillis(5_000)
                .autoFlushRows(10_000)
                .autoFlushIntervalMillis(1_000)
                .errorHandler(error -> System.err.printf("[%s] BATCH ERROR: category=%s table=%s msg=%s%n",
                        LocalDateTime.now().format(FMT), error.getCategory(),
                        error.getTableName(), error.getServerMessage()))
                .connectionListener(event -> System.out.printf("[%s] CONNECTION: %s host=%s:%d%n",
                        LocalDateTime.now().format(FMT), event.getKind(), event.getHost(), event.getPort()));
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

    /**
     * Decide where to start the data clock. We continue strictly after the latest
     * row already in the table to keep the series continuous and avoid clobbering
     * existing data; an explicit --start-ts (faster-than-life) wins when provided.
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
