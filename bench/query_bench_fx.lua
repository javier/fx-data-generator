-- FX OHLC 1m Query Benchmark for wrk
--
-- Each request queries the market_data_ohlc_1m materialized view for a single
-- randomly chosen FX pair (out of 30) over a random time window between 1 and
-- 20 minutes within the recent past. Every request picks a different symbol and
-- a different time range, simulating concurrent dashboard or API consumers each
-- looking at different instruments and timeframes.
--
-- Usage:
--   TOKEN="your_token" wrk -t4 -c10 -d30s -s query_bench_fx.lua https://172.31.42.41:9000
--
-- Or with hostname:
--   TOKEN="your_token" wrk -t4 -c10 -d30s -s query_bench_fx.lua https://enterprise-primary:9000
--
-- Environment variables:
--   TOKEN     - Bearer token for authentication (optional for local)
--   MAX_RANGE - Max window in minutes (default: 20)

-- All 30 symbols from the materialized view
local symbols = {
  "USDCAD", "CADJPY", "USDSGD", "GBPAUD", "USDNOK",
  "EURAUD", "GBPJPY", "AUDCAD", "NZDJPY", "USDJPY",
  "USDSEK", "USDHKD", "EURNZD", "EURCHF", "AUDNZD",
  "GBPCHF", "NZDUSD", "EURUSD", "USDMXN", "EURCAD",
  "USDCHF", "NZDCAD", "AUDUSD", "USDTRY", "AUDJPY",
  "EURGBP", "EURJPY", "GBPNZD", "USDZAR", "GBPUSD",
}

local bearer = os.getenv("TOKEN")
local max_range = tonumber(os.getenv("MAX_RANGE")) or 20

-- URL encode helper
local function urlencode(str)
  str = str:gsub("\n", "")
  return str:gsub("([^%w%-_%.~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end)
end

-- Print config once per thread
if not _G.config_printed then
  print(string.format("[query_bench_fx] max_range=%dm, symbols=%d", max_range, #symbols))
  _G.config_printed = true
end

math.randomseed(os.time() + (tonumber(tostring({}):match("0x(%x+)")) or 0))

request = function()
  -- Pick a random symbol
  local sym = symbols[math.random(#symbols)]

  -- Generate a random time range within the last max_range minutes.
  -- start_off > end_off (both subtracted from $now), gap >= 1 minute.
  local end_off = math.random(0, max_range - 1)
  local start_off = math.random(end_off + 1, max_range)

  local range
  if end_off == 0 then
    range = string.format("'$now-%dm..$now'", start_off)
  else
    range = string.format("'$now-%dm..$now-%dm'", start_off, end_off)
  end

  local query = string.format(
    "SELECT * FROM market_data_ohlc_1m WHERE symbol = '%s' AND timestamp IN %s",
    sym, range
  )

  local path = "/exec?query=" .. urlencode(query)

  local headers = {}
  if bearer then
    headers["Authorization"] = "Bearer " .. bearer
  end

  return wrk.format("GET", path, headers)
end
