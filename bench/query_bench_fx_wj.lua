-- FX Trades WINDOW JOIN Benchmark for wrk
--
-- Each request runs a WINDOW JOIN between fx_trades and core_price for a single
-- randomly chosen FX pair (out of 30) over a random time window between 10 minutes
-- and 2 hours in the recent past. The query finds each trade's surrounding
-- market context - the minimum ask and maximum bid within 10 seconds of each
-- trade - simulating execution quality or slippage analysis workloads.
--
-- Usage:
--   TOKEN="your_token" wrk -t4 -c10 -d30s -s query_bench_fx_wj.lua https://172.31.42.41:9000
--
-- Or with hostname:
--   TOKEN="your_token" wrk -t4 -c10 -d30s -s query_bench_fx_wj.lua https://enterprise-primary:9000
--
-- Environment variables:
--   TOKEN     - Bearer token for authentication (optional for local)
--   MAX_RANGE - Max window in minutes (default: 120, i.e. 2 hours)
--   MIN_WIDTH - Min window width in minutes (default: 10)

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
local max_range = tonumber(os.getenv("MAX_RANGE")) or 120
local min_width = tonumber(os.getenv("MIN_WIDTH")) or 10

-- URL encode helper
local function urlencode(str)
  str = str:gsub("\n", "")
  return str:gsub("([^%w%-_%.~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end)
end

-- Print config once per thread
if not _G.config_printed then
  print(string.format("[query_bench_fx_wj] max_range=%dh, min_width=%dm, symbols=%d",
    max_range / 60, min_width, #symbols))
  _G.config_printed = true
end

math.randomseed(os.time() + (tonumber(tostring({}):match("0x(%x+)")) or 0))

request = function()
  -- Pick a random symbol
  local sym = symbols[math.random(#symbols)]

  -- Generate a random time range within the last max_range minutes.
  -- Window width is at least min_width minutes.
  local width = math.random(min_width, max_range)
  local end_off = math.random(0, max_range - width)
  local start_off = end_off + width

  local range
  if end_off == 0 then
    range = string.format("'$now-%dm..$now'", start_off)
  else
    range = string.format("'$now-%dm..$now-%dm'", start_off, end_off)
  end

  local query = string.format(
    "SELECT t.symbol, t.timestamp, t.side, t.price, " ..
    "min(p.ask_price) AS min_ask, max(p.bid_price) AS max_bid " ..
    "FROM fx_trades t " ..
    "WINDOW JOIN core_price p ON (symbol) " ..
    "RANGE BETWEEN 10 seconds PRECEDING AND 10 seconds FOLLOWING " ..
    "EXCLUDE PREVAILING " ..
    "WHERE t.symbol = '%s' AND t.timestamp IN %s",
    sym, range
  )

  local path = "/exec?query=" .. urlencode(query)

  local headers = {}
  if bearer then
    headers["Authorization"] = "Bearer " .. bearer
  end

  return wrk.format("GET", path, headers)
end
