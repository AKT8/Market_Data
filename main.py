import requests, pandas as pd, numpy as np, duckdb, sys, time
import ta     # pip install ta requests pandas numpy duckdb

SYMBOL_API  = "https://chukul.com/api/data/symbol/"
HIST_API    = "https://chukul.com/api/data/historydata/?symbol={}"
DUCK_FILE   = "nepse_data.duckdb"

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

    # business-week static (Fri/Sat = weekend)
    biz = df.resample("B").last()   # business days (Mon–Fri)
    # ensure we drop Fri/Sat because Nepali market weekend
    biz = biz[biz.index.dayofweek < 5]  # keep Mon-Thu
    def static(days): 
        return (biz["Close"].iloc[-1] / biz["Close"].iloc[-days-1]) - 1 if len(biz)>days else np.nan
    df["1W static return"] = static(5)
    df["1M static return"] = static(21)
    return df.reset_index()

def indicators(df):
    log("  indicators…")
    c,h,l = df["Close"],df["High"],df["Low"]
    macd = ta.trend.MACD(c)
    df["Macd"] = macd.macd()
    df["Rsi14"] = ta.momentum.RSIIndicator(c,14).rsi()
    df["Rsi14_sma14"] = df["Rsi14"].rolling(14).mean()
    df["Stoch14_3_3"] = ta.momentum.StochasticOscillator(h,l,c,14,3).stoch()
    df["Stoch21_10_10"] = ta.momentum.StochasticOscillator(h,l,c,21,10).stoch()
    bb = ta.volatility.BollingerBands(c,20,2)
    df["Bb20_2_upper"],df["Bb20_2_lower"] = bb.bollinger_hband(),bb.bollinger_lband()
    kc = ta.volatility.KeltnerChannel(h,l,c,20,10)
    df["Kc20_1.5_upper"],df["Kc20_1.5_lower"] = kc.keltner_channel_hband(),kc.keltner_channel_lband()
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
        except Exception as e:
            print(f"Error {sym}: {e}", file=sys.stderr)

if __name__ == "__main__": main()