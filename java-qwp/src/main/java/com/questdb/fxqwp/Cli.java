package com.questdb.fxqwp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Command-line configuration, with option parity to the Python
 * {@code fx_data_generator.py}.
 *
 * <p>Argument names accept both Python underscore form ({@code --total_market_data_events},
 * {@code --start_ts}) and kebab form ({@code --total-trades}, {@code --start-ts}) — the
 * leading {@code --} is stripped and {@code -} normalised to {@code _} before matching.
 *
 * <p>Some Python options describe machinery this trades-only QWP generator does not
 * have (order books, the {@code market_data}/{@code core_price} tables, materialized
 * views, the PG/TCP transports, multi-process precompute). Those are still accepted
 * for command-line parity but have no effect; each one prints a one-line note so it
 * is never silently misleading.
 *
 * <p>Hosts, credentials and the TLS flag are shared across the whole HA fleet: one
 * credential set, one transport scheme (ws or wss) for every host.
 */
public final class Cli {

    // --- connection / HA -----------------------------------------------------
    public List<String> hosts = new ArrayList<>(List.of("127.0.0.1:9000"));
    public boolean tls = false;
    public boolean tlsInsecure = false;
    public String token = null;
    public String user = null;
    public String password = null;
    public String sfDir = "/tmp/qwp_trades_sf";
    public String senderId = "qwp-fx-trades";
    public int autoFlushBytes = 524288;   // QWP sender auto-flush size (bytes); 512 KiB, safely under the ~1MB WS frame cap

    // --- mode / volume / time ------------------------------------------------
    public String mode = null;                 // real-time | faster-than-life (required)
    public int tradesMinPerSec = 50;           // throughput floor (events/sec)
    public int tradesMaxPerSec = 200;          // throughput ceiling (events/sec)
    public long totalTrades = 1_000_000;       // max events; 0 = unlimited (real-time)
    public String startTs = null;              // ISO-8601 start (faster-than-life)
    public String endTs = null;                // ISO-8601 max timestamp (upper bound)
    public int tradesProcesses = 1;            // worker threads for qwp_trades (0 = off)
    public int marketDataProcesses = 0;        // worker threads for qwp_market_data (0 = off)
    public int runSecs = 0;                    // wall-clock run cap in seconds; 0 = no cap
    public int commitIntervalMs = 1000;        // transaction rate: commit (flush) cadence in ms

    // market_data volume (snapshots/sec across its whole pool) and order-book depth
    public int marketDataMinEps = 1200;
    public int marketDataMaxEps = 15000;
    public int minLevels = 40;
    public int maxLevels = 40;

    // --- reference data / schema --------------------------------------------
    public int yahooRefreshSecs = 300;
    public boolean noYahoo = false;
    public boolean incremental = false;        // seed state from last stored prices, skip Yahoo
    public boolean shortTtl = false;
    public boolean enterprise = false;
    public String suffix = "";                 // table-name suffix (parity with Python)
    public int leiPoolSize = 2000;

    public static Cli parse(String[] args) {
        Cli c = new Cli();
        List<String> notes = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String raw = args[i];
            String key = raw.startsWith("--") ? raw.substring(2).replace('-', '_')
                    : (raw.equals("-h") ? "help" : raw);
            switch (key) {
                // ---- connection / HA ----
                case "hosts":
                    c.hosts = normalizeHosts(req(args, ++i, raw));
                    break;
                case "host": // Python single-host form
                    c.hosts = normalizeHosts(req(args, ++i, raw));
                    break;
                case "tls":
                    c.tls = true;
                    break;
                case "tls_insecure":
                    c.tls = true;
                    c.tlsInsecure = true;
                    break;
                case "token":
                    if (c.token != null) {
                        fail("use either --token or --token_file, not both");
                    }
                    c.token = req(args, ++i, raw);
                    break;
                case "token_file":
                    if (c.token != null) {
                        fail("use either --token or --token_file, not both");
                    }
                    c.token = readTokenFile(req(args, ++i, raw));
                    break;
                case "user":
                    c.user = req(args, ++i, raw);
                    break;
                case "password":
                    c.password = req(args, ++i, raw);
                    break;
                case "sf_dir":
                    c.sfDir = req(args, ++i, raw);
                    break;
                case "sender_id":
                    c.senderId = req(args, ++i, raw);
                    break;
                case "auto_flush_bytes":
                case "autoflush_bytes":
                    c.autoFlushBytes = Integer.parseInt(req(args, ++i, raw));
                    break;

                // ---- mode / volume / time ----
                case "mode":
                    c.mode = req(args, ++i, raw);
                    break;
                case "orders_min_per_sec":
                case "trades_min_per_sec":
                    c.tradesMinPerSec = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "orders_max_per_sec":
                case "trades_max_per_sec":
                    c.tradesMaxPerSec = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "total_market_data_events":
                case "total_events":
                case "total_trades":
                    c.totalTrades = Long.parseLong(req(args, ++i, raw));
                    break;
                case "start_ts":
                    c.startTs = req(args, ++i, raw);
                    break;
                case "end_ts":
                    c.endTs = req(args, ++i, raw);
                    break;
                case "trades_processes":
                    c.tradesProcesses = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "market_data_processes":
                    c.marketDataProcesses = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "market_data_min_eps":
                    c.marketDataMinEps = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "market_data_max_eps":
                    c.marketDataMaxEps = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "min_levels":
                    c.minLevels = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "max_levels":
                    c.maxLevels = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "run_secs":
                case "runtime_secs":
                    c.runSecs = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "commit_interval_ms":
                    c.commitIntervalMs = Integer.parseInt(req(args, ++i, raw));
                    break;

                // ---- reference data / schema ----
                case "yahoo_refresh_secs":
                    c.yahooRefreshSecs = Integer.parseInt(req(args, ++i, raw));
                    break;
                case "no_yahoo":
                    c.noYahoo = true;
                    break;
                case "incremental":
                    i = boolFlag(args, i, v -> c.incremental = v);
                    break;
                case "short_ttl":
                    i = boolFlag(args, i, v -> c.shortTtl = v);
                    break;
                case "enterprise":
                    i = boolFlag(args, i, v -> c.enterprise = v);
                    break;
                case "suffix":
                    c.suffix = req(args, ++i, raw);
                    break;
                case "lei_pool_size":
                    c.leiPoolSize = Integer.parseInt(req(args, ++i, raw));
                    break;

                // ---- accepted but currently unused (kept for now) ----
                case "chunk_seconds":
                    notes.add(raw + " accepted but unused: state is streamed per-second, so there is no"
                            + " upfront precompute to chunk.");
                    i = skipValueIfPresent(args, i);
                    break;

                case "help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    fail("Unknown argument: " + raw);
                    break;
            }
        }
        for (String n : notes) {
            System.out.println("[note] " + n);
        }
        c.validate();
        return c;
    }

    private void validate() {
        if (!"real-time".equals(mode) && !"faster-than-life".equals(mode)) {
            fail("--mode must be 'real-time' or 'faster-than-life'");
        }
        if (hosts.isEmpty()) {
            fail("--hosts must list at least one host");
        }
        if (tradesProcesses > 0 && (tradesMinPerSec <= 0 || tradesMaxPerSec < tradesMinPerSec)) {
            fail("require 0 < --orders_min_per_sec <= --orders_max_per_sec");
        }
        if (token != null && (user != null || password != null)) {
            fail("use either --token OR --user/--password, not both");
        }
        if ((user == null) != (password == null)) {
            fail("--user and --password must be provided together");
        }
        if (leiPoolSize <= 0) {
            fail("--lei_pool_size must be > 0");
        }
        if (tradesProcesses < 0 || tradesProcesses > 30) {
            fail("--trades_processes must be between 0 and 30");
        }
        if (marketDataProcesses < 0 || marketDataProcesses > 30) {
            fail("--market_data_processes must be between 0 and 30");
        }
        if (tradesProcesses == 0 && marketDataProcesses == 0) {
            fail("enable at least one pool (--trades_processes and/or --market_data_processes > 0)");
        }
        if (marketDataProcesses > 0) {
            if (marketDataMinEps <= 0 || marketDataMaxEps < marketDataMinEps) {
                fail("require 0 < --market_data_min_eps <= --market_data_max_eps");
            }
            if (minLevels < 1 || maxLevels < minLevels) {
                fail("require 1 <= --min_levels <= --max_levels");
            }
        }
        if (autoFlushBytes < 1024) {
            fail("--auto_flush_bytes must be >= 1024");
        }
        if (runSecs < 0) {
            fail("--run_secs must be >= 0");
        }
        if (commitIntervalMs < 1) {
            fail("--commit_interval_ms must be >= 1");
        }
        if (autoFlushBytes > 900_000) {
            System.out.println("[note] --auto_flush_bytes " + autoFlushBytes
                    + " is near/above the QWP WebSocket frame cap (~1MB); large frames risk a 1009 rejection.");
        }
        if ("faster-than-life".equals(mode) && totalTrades <= 0 && endTs == null && runSecs <= 0) {
            fail("faster-than-life requires a bound: set --total_market_data_events > 0, --end_ts, or --run_secs");
        }
    }

    public String tradesTable() {
        return "qwp_trades" + suffix;
    }

    public String marketDataTable() {
        return "qwp_market_data" + suffix;
    }

    public String scheme() {
        return tls ? "wss" : "ws";
    }

    /** Config string for the {@link io.questdb.client.cutlass.qwp.client.QwpQueryClient} used for DDL/queries. */
    public String queryClientConfig() {
        StringBuilder sb = new StringBuilder();
        sb.append(scheme()).append("::addr=").append(String.join(",", hosts)).append(';');
        appendAuth(sb);
        if (tls && tlsInsecure) {
            sb.append("tls_verify=unsafe_off;");
        }
        if (hosts.size() > 1) {
            sb.append("failover=on;");
        }
        return sb.toString();
    }

    private void appendAuth(StringBuilder sb) {
        if (token != null) {
            sb.append("token=").append(token).append(';');
        } else if (user != null) {
            sb.append("username=").append(user).append(';');
            sb.append("password=").append(password).append(';');
        }
    }

    // ---- arg helpers --------------------------------------------------------

    /** Boolean flag that optionally consumes a following true/false value (Python style). */
    private static int boolFlag(String[] args, int i, java.util.function.Consumer<Boolean> set) {
        Boolean v = boolValue(args, i + 1);
        if (v != null) {
            set.accept(v);
            return i + 1;
        }
        set.accept(Boolean.TRUE);
        return i;
    }

    private static int skipValueIfPresent(String[] args, int i) {
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
            return i + 1;
        }
        return i;
    }

    private static Boolean boolValue(String[] args, int idx) {
        if (idx < args.length) {
            String v = args[idx].toLowerCase();
            if (v.equals("true")) {
                return Boolean.TRUE;
            }
            if (v.equals("false")) {
                return Boolean.FALSE;
            }
        }
        return null;
    }

    private static List<String> normalizeHosts(String csv) {
        List<String> out = new ArrayList<>();
        for (String h : csv.split(",")) {
            String t = h.trim();
            if (t.isEmpty()) {
                continue;
            }
            out.add(t.contains(":") ? t : t + ":9000");
        }
        return out;
    }

    private static String req(String[] args, int i, String flag) {
        if (i >= args.length) {
            fail("missing value for " + flag);
        }
        return args[i];
    }

    /** Read the QWP token from a file (trimmed), so it never appears on the command line. */
    private static String readTokenFile(String path) {
        try {
            String t = Files.readString(Paths.get(path)).trim();
            if (t.isEmpty()) {
                fail("--token_file " + path + " is empty");
            }
            return t;
        } catch (IOException e) {
            fail("could not read --token_file " + path + ": " + e.getMessage());
            return null; // unreachable: fail() throws
        }
    }

    private static void fail(String msg) {
        System.err.println("ERROR: " + msg + "\n");
        printUsage();
        throw new IllegalArgumentException(msg);
    }

    public static void printUsage() {
        System.out.println(String.join("\n", Arrays.asList(
                "qwp-fx-trades — synthetic FX trade generator over QuestDB QWP (WebSocket), HA-aware.",
                "Option names accept Python underscore form or kebab form (e.g. --start_ts or --start-ts).",
                "",
                "Required:",
                "  --mode <real-time|faster-than-life>",
                "",
                "Connection (shared across all hosts):",
                "  --hosts h1:9000,h2:9000,h3:9000   comma-separated HA fleet (default 127.0.0.1:9000)",
                "  --host <h:port>                   single host (parity alias for --hosts)",
                "  --tls                             use wss for every host",
                "  --tls_insecure                    use wss and disable certificate validation",
                "  --token <t>                       QWP/bearer token   (enterprise)",
                "  --token_file <path>               read the QWP token from a file (keeps it off the CLI)",
                "  --user <u> --password <p>         OR HTTP basic auth",
                "  --sf_dir <dir>                    store-and-forward dir (default /tmp/qwp_trades_sf)",
                "  --sender_id <id>                  store-and-forward sender id (default qwp-fx-trades)",
                "  --auto_flush_bytes <n>            QWP sender auto-flush size in bytes (default 524288 = 512 KiB)",
                "",
                "Pools (one thread set per table; symbols snake-drafted across each pool):",
                "  --trades_processes <n>            worker threads for qwp_trades, 0-30 (default 1; 0 = off)",
                "  --market_data_processes <n>       worker threads for qwp_market_data, 0-30 (default 0 = off)",
                "",
                "Volume / time (each *_per_sec / *_eps is the table-wide total across its pool):",
                "  --orders_min_per_sec <n>          qwp_trades orders/sec total (default 50); each order -> 1+ fills",
                "  --orders_max_per_sec <n>          qwp_trades orders/sec total (default 200)",
                "  --market_data_min_eps <n>         qwp_market_data snapshots/sec total (default 1200)",
                "  --market_data_max_eps <n>         qwp_market_data snapshots/sec total (default 15000)",
                "  --min_levels <n> --max_levels <n> order-book depth per snapshot (default 40/40)",
                "  --total_market_data_events <n>    max total rows across tables; 0 = unlimited (default 1000000)",
                "  --start_ts <iso>                  faster-than-life start (default: after last row / now)",
                "  --end_ts <iso>                    max timestamp / upper bound",
                "  --run_secs <n>                    stop after n wall-clock seconds (0 = no cap; for throughput tests)",
                "  --commit_interval_ms <n>          transaction rate: commit cadence in ms (default 1000)",
                "",
                "Reference data / schema:",
                "  --yahoo_refresh_secs <n>          real-time Yahoo refresh interval (default 300)",
                "  --no_yahoo                        skip Yahoo, use template brackets (offline)",
                "  --incremental [true|false]        seed prices from last stored row, skip Yahoo",
                "  --short_ttl [true|false]          attach retention to the table",
                "  --enterprise [true|false]         with --short_ttl, use STORAGE POLICY instead of TTL",
                "  --suffix <s>                      append suffix to the table name (-> qwp_trades<s>)",
                "  --lei_pool_size <n>               distinct counterparties (default 2000)",
                "  --chunk_seconds <n>               accepted but unused (state is streamed per-second)")));
    }
}
