package com.questdb.fxqwp;

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

    // --- mode / volume / time ------------------------------------------------
    public String mode = null;                 // real-time | faster-than-life (required)
    public int tradesMinPerSec = 50;           // throughput floor (events/sec)
    public int tradesMaxPerSec = 200;          // throughput ceiling (events/sec)
    public long totalTrades = 1_000_000;       // max events; 0 = unlimited (real-time)
    public String startTs = null;              // ISO-8601 start (faster-than-life)
    public String endTs = null;                // ISO-8601 max timestamp (upper bound)
    public int processes = 1;                  // single-worker generator (see note)

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
                    c.token = req(args, ++i, raw);
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
                case "processes": {
                    c.processes = Integer.parseInt(req(args, ++i, raw));
                    if (c.processes != 1) {
                        notes.add("--processes " + c.processes
                                + " ignored: qwp-fx-trades is single-worker (one QWP sender).");
                        c.processes = 1;
                    }
                    break;
                }

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
        if (tradesMinPerSec <= 0 || tradesMaxPerSec < tradesMinPerSec) {
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
        if ("faster-than-life".equals(mode) && totalTrades <= 0 && endTs == null) {
            fail("faster-than-life requires a bound: set --total_market_data_events > 0 or --end_ts");
        }
    }

    public String tableName() {
        return suffix.isEmpty() ? "qwp_trades" : "qwp_trades" + suffix;
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
                "  --user <u> --password <p>         OR HTTP basic auth",
                "  --sf_dir <dir>                    store-and-forward dir (default /tmp/qwp_trades_sf)",
                "  --sender_id <id>                  store-and-forward sender id (default qwp-fx-trades)",
                "",
                "Volume / time:",
                "  --orders_min_per_sec <n>          throughput floor, events/sec (default 50)",
                "  --orders_max_per_sec <n>          throughput ceiling, events/sec (default 200)",
                "  --total_market_data_events <n>    max events; 0 = unlimited (default 1000000)",
                "  --start_ts <iso>                  faster-than-life start (default: after last row / now)",
                "  --end_ts <iso>                    max timestamp / upper bound",
                "  --processes <n>                   single-worker only (n>1 noted and ignored)",
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
