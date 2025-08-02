import os, pandas as pd

BASE="/content" if os.path.isdir("/content") else os.getcwd()

def resolve_paths(cfg):
    parquet=cfg["data"]["parquet_indicators"]
    csv15=cfg["data"]["csv_15"]
    out_dir=cfg["paths"]["output_dir"]; logs=cfg["paths"]["logs_dir"]
    os.makedirs(out_dir,exist_ok=True); os.makedirs(logs,exist_ok=True)
    if not os.path.exists(parquet): raise FileNotFoundError(f"Parquet yok: {parquet}")
    return {"parquet":parquet,"csv15":csv15,"out_dir":out_dir,"logs":logs}

def _to_ts(s): return pd.to_datetime(s).tz_localize(None)

def load_universe_dataframe(parquet_path,wanted_columns=None,date_start=None,date_end=None):
    # pyarrow available in Colab; if not, pandas will use fastparquet
    df = pd.read_parquet(parquet_path)
    # normalize columns to lower-case map for robust selection
    rename_map = {c: c.lower() for c in df.columns}
    df.rename(columns=rename_map, inplace=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        if date_start: df=df[df["date"]>=_to_ts(date_start)]
        if date_end:   df=df[df["date"]<=_to_ts(date_end)]
    # standardize symbol to str upper
    if "symbol" in df.columns:
        df["symbol"]=df["symbol"].astype(str).str.upper()
    # dynamic selection
    if wanted_columns:
        keep=[]
        cols_lower=set(df.columns)
        for w in wanted_columns:
            wl=w.lower()
            if wl in cols_lower:
                keep.append(wl); continue
            # try fuzzy variants for common indicators
            # macd_line/macd_signal
            if wl=="macd_line":
                for cand in ["macd_line","macd_12_26_9","macd","macdline"]:
                    if cand in cols_lower: keep.append(cand); break
            elif wl=="macd_signal":
                for cand in ["macds_12_26_9","macd_signal","macds","signal"]:
                    if cand in cols_lower: keep.append(cand); break
            elif wl=="bbm_20_2":
                for cand in ["bbm_20_2.0","bbm_20_2","bbm","bbm_20_2_0"]:
                    if cand in cols_lower: keep.append(cand); break
            elif wl=="bbu_20_2":
                for cand in ["bbu_20_2.0","bbu_20_2","bbu","bbu_20_2_0"]:
                    if cand in cols_lower: keep.append(cand); break
            else:
                # fuzzy contains for rsi/adx/stoch
                if "rsi" in wl:
                    cand=[c for c in df.columns if c.startswith("rsi")]
                    keep+=cand
                elif "adx" in wl:
                    cand=[c for c in df.columns if c.startswith("adx")]
                    keep+=cand
        keep=sorted(dict.fromkeys(keep))
        base=["symbol","date"]
        cols=[c for c in base+keep if c in df.columns]
        df=df[cols].copy()
    return df, {"rows":len(df),"cols":list(df.columns)}

