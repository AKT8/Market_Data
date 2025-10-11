
import requests, pandas as pd, numpy as np, duckdb, sys, time, ta


SYMBOL_API  = "https://chukul.com/api/data/symbol/"
HIST_API    = "https://chukul.com/api/data/historydata/?symbol={}"
DUCK_FILE   = "data.duckdb"

log = lambda m: print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def symbols():
    log("Fetching symbols…")
    s = pd.DataFrame(requests.get(SYMBOL_API,timeout=30).json())
    return s.query("type=='stock' and sector_id not in [13,14,15]")["symbol"]

def history(sym):
    d = pd.DataFrame(requests.get(HIST_API.format(sym),timeout=30).json())
    if d.empty: return d
    d["Datetime"] = pd.to_datetime(d["date"])
    d.sort_values("Datetime", inplace=True)
    return d.rename(columns={"symbol":"Symbol","open":"Open","high":"High",
                             "low":"Low","close":"Close","ltp":"Ltp","volume":"Volume"})[
        ["Datetime","Symbol","Open","High","Low","Close","Ltp","Volume"]]

def returns(df):
    log("  returns…")
    df = df.copy().set_index("Datetime")
    df["1W rolling return"] = df["Close"].pct_change(5)
    df["1M rolling return"] = df["Close"].pct_change(21)
    df["1W static return"] = df.groupby(df.index.to_period("W-SUN"))["Close"].transform(lambda x: x / x.iloc[0] - 1)
    df["1M static return"] = df.groupby(df.index.to_period("M"))["Close"].transform(lambda x: x / x.iloc[0] - 1)

    return df.reset_index()

def indicators(df):
    log("  indicators…")
    c,h,l,o = df["Close"],df["High"],df["Low"],df["Open"]

    df["Rsi14"] = ta.momentum.RSIIndicator(c,14).rsi()
    df["Rsi14_sma14"] = df["Rsi14"].rolling(14).mean()
    df["Stoch14_3_3s"] = ta.momentum.StochasticOscillator(h,l,c,14,3).stoch_signal().rolling(3).mean()
    df["Stoch14_3_3k"] = ta.momentum.StochasticOscillator(h,l,c,14,3).stoch_signal()
    df["Stoch21_10_10s"] = ta.momentum.StochasticOscillator(h,l,c,21,10).stoch_signal().rolling(10).mean()
    df["Stoch21_10_10k"] = ta.momentum.StochasticOscillator(h,l,c,21,10).stoch_signal()
    bb = ta.volatility.BollingerBands(c,20,2)
    df["Bb20_2_upper"],df["Bb20_2_lower"] = bb.bollinger_hband(),bb.bollinger_lband()
    atr = ta.volatility.AverageTrueRange(h, l, c, window=10).average_true_range()
    ema = ta.trend.EMAIndicator(c,20).ema_indicator()
    mult = 1.5
    df["Kc20_1.5_upper"] = ema + mult * atr
    df["Kc20_1.5_lower"] = ema - mult * atr
    df["Ema20_close"] = ta.trend.EMAIndicator(c,20).ema_indicator()
    df["Ema20_high"] = ta.trend.EMAIndicator(h,20).ema_indicator()
    df["Ema20_low"] = ta.trend.EMAIndicator(l,20).ema_indicator()
    return df

def save(df, name):
    log(f"  saving {name}")
    con = duckdb.connect(DUCK_FILE)
    con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM df LIMIT 0")
    con.execute(f"INSERT INTO {name} SELECT * FROM df")
    con.close()

def main():
    for sym in symbols():
        log(f"Processing {sym}")
        try:
            d = history(sym)
            if d.empty: continue
            d = indicators(returns(d))
            save(d, sym)
            #print(f"\n=== {sym} ===")
            #print(tabulate(d.tail(21), headers='keys', tablefmt='grid', showindex=False))

        except Exception as e:
            print(f"Error {sym}: {e}", file=sys.stderr)

if __name__ == "__main__": main()