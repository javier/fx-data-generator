package com.questdb.fxqwp;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal Yahoo Finance reference-price fetcher.
 *
 * <p>Hits the public chart endpoint and reads {@code regularMarketPrice} from the
 * JSON. We extract a single number with a regex rather than pulling in a JSON
 * dependency: the endpoint shape is stable and any parse failure simply falls back
 * to the pair's template bracket, which is exactly the resilience the Python
 * generator has ({@code fetch_fx_pairs_from_yahoo} catches everything and keeps the
 * template low/high). Java uses its own truststore, so there is no macOS certifi
 * problem to work around here.
 */
public final class YahooFinance {

    private static final Pattern PRICE =
            Pattern.compile("\"regularMarketPrice\"\\s*:\\s*([0-9]+(?:\\.[0-9]+)?)");

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /**
     * Recentre every pair's [low, high] bracket on its live mid price.
     *
     * @param pairs       pairs to refresh in place
     * @param bracketPct  half-width of the bracket as a percentage (1.0 == +/-1%)
     */
    /**
     * Refresh each pair's [low, high] bracket from its live mid.
     *
     * @param effSec the data-second at which the new brackets take effect. Pass
     *               {@link Long#MIN_VALUE} for the startup fetch (resets the timeline
     *               to a single initial bracket); pass a future data-second for a
     *               periodic real-time refresh (appends, so both pools adopt it at
     *               the same second). A failed fetch keeps the current bracket.
     */
    public void refreshBrackets(List<FxPair> pairs, double bracketPct, long effSec) {
        double pct = bracketPct / 100.0;
        System.out.println("[INFO] Refreshing reference data from Yahoo Finance.");
        for (FxPair pair : pairs) {
            Double mid = fetchMid(pair.yahooTicker());
            if (mid != null && mid > 0 && !mid.isNaN()) {
                double low = mid * (1 - pct);
                double high = mid * (1 + pct);
                if (effSec == Long.MIN_VALUE) {
                    pair.resetBracket(low, high);
                } else {
                    pair.appendBracket(effSec, low, high);
                }
            } else {
                System.out.printf("[YF] %s: keeping current bracket (fetch failed)%n", pair.symbol);
            }
        }
    }

    private Double fetchMid(String ticker) {
        String url = "https://query1.finance.yahoo.com/v8/finance/chart/"
                + ticker + "?range=1d&interval=1m";
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(8))
                    // Yahoo rejects requests without a browser-like UA.
                    .header("User-Agent", "Mozilla/5.0 (qwp-fx-trades)")
                    .GET()
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                return null;
            }
            Matcher m = PRICE.matcher(resp.body());
            if (m.find()) {
                return Double.parseDouble(m.group(1));
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
