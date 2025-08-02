import os, json, argparse, pandas as pd
from multi_day_engine import BASE, resolve_paths, load_universe_dataframe

def parse_args():
    ap=argparse.ArgumentParser()
    ap.add_argument("--mode",required=True,choices=["single","range"])
    ap.add_argument("--date"); ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--config",default=os.path.join(BASE,"config.json"))
    return ap.parse_args()

def load_cfg(path): return json.load(open(path,"r",encoding="utf-8"))

def read_watchlist(csv15):
    if not os.path.exists(csv15): return None
    try:
        df=pd.read_csv(csv15)
        symcol="symbol" if "symbol" in df.columns else df.columns[0]
        syms=df[symcol].astype(str).str.strip().str.upper().unique().tolist()
        return syms
    except Exception as e:
        print("⚠️ 15.csv okunamadı:",e); return None

def _default_filters(df):
    mask=pd.Series(True, index=df.index)
    rules=[]
    # dynamic column presence
    if "rsi_14" in df.columns: 
        mask &= df["rsi_14"].between(45,70); rules.append("rsi_14 between 45-70")
    else:
        rsi_like=[c for c in df.columns if c.startswith("rsi")]
        if rsi_like: mask &= df[rsi_like[0]].between(45,70); rules.append(f"{rsi_like[0]} between 45-70")
    adx_col = "adx_14" if "adx_14" in df.columns else next((c for c in df.columns if c.startswith("adx")), None)
    if adx_col:
        mask &= df[adx_col] >= 20; rules.append(f"{adx_col} >= 20")
    macd_line = "macd_line" if "macd_line" in df.columns else next((c for c in df.columns if c.startswith("macd") and "signal" not in c), None)
    macd_sig  = "macd_signal" if "macd_signal" in df.columns else next((c for c in df.columns if "signal" in c and "macd" in c), None)
    if macd_line and macd_sig:
        mask &= df[macd_line] > df[macd_sig]; rules.append(f"{macd_line} > {macd_sig}")
    return mask, rules

def _filters_from_csv(df, path="/content/resources/filters.csv"):
    if not os.path.exists(path): return _default_filters(df)
    try:
        fdf=pd.read_csv(path)
        exprs=[str(x).strip() for x in fdf.get("expr",[]).dropna().tolist() if str(x).strip()]
    except Exception as e:
        print("⚠️ filters.csv okunamadı:",e); return _default_filters(df)
    mask=pd.Series(True,index=df.index); used=[]
    for e in exprs:
        try:
            mask &= df.eval(e)
            used.append(e)
        except Exception as ex:
            print("⚠️ ifade atlandı:", e, "->", ex)
    if not used: return _default_filters(df)
    return mask, used

def screen_df(df, csv15_path):
    wl = read_watchlist(csv15_path)
    if wl and "symbol" in df.columns:
        df=df[df["symbol"].isin(wl)].copy()
    if "date" in df.columns:
        df=df.sort_values(["symbol","date"]).drop_duplicates(["symbol","date"], keep="last")
    mask, used=_filters_from_csv(df)
    return df[mask].copy(), used

def write_excel(df, out_xlsx, used_filters):
    import pandas as pd
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as wr:
        # summary
        if "date" in df.columns and not df.empty:
            g=df.groupby(df["date"].dt.date)["symbol"].nunique().reset_index()
            g.columns=["date","unique_symbols"]
        else:
            g=pd.DataFrame([{"date":"NA","unique_symbols": int(df["symbol"].nunique() if "symbol" in df.columns else 0)}])
        g.to_excel(wr, index=False, sheet_name="summary")
        pd.DataFrame({"filters_used": used_filters}).to_excel(wr, index=False, sheet_name="filters")
        # per-day sheets
        if "date" in df.columns and not df.empty:
            for d, grp in df.groupby(df["date"].dt.date):
                grp.sort_values(["symbol","date"], inplace=True)
                grp.to_excel(wr, index=False, sheet_name=str(d))
        else:
            df.to_excel(wr, index=False, sheet_name="Sonuclar")

def run_single(date_str,cfg): return run_range(date_str,date_str,cfg)

def run_range(start_str,end_str,cfg):
    paths=resolve_paths(cfg)
    wanted=["symbol","date","close","volume","rsi_14","macd_line","macd_signal","adx_14",
            "stoch_k","stoch_d","bbm_20_2","bbu_20_2","ichimoku_conversionline","ichimoku_baseline"]
    df_all,_=load_universe_dataframe(paths["parquet"],wanted_columns=wanted,date_start=start_str,date_end=end_str)
    screened,used=screen_df(df_all, paths["csv15"])
    os.makedirs(paths["out_dir"],exist_ok=True)
    out_xlsx=os.path.join(paths["out_dir"], f"Tarama_{start_str}_{end_str}.xlsx")
    write_excel(screened,out_xlsx,used)
    print(f"✅ Tarama bitti. Excel: {out_xlsx}; Kural sayısı: {len(used)}; Satır: {len(screened)}")
    return 0

def main():
    a=parse_args(); cfg=load_cfg(a.config)
    if a.mode=="single":
        if not a.date: raise SystemExit("--date gereklidir (YYYY-MM-DD)")
        run_single(a.date,cfg)
    else:
        if not (a.start and a.end): raise SystemExit("--start ve --end gereklidir (YYYY-MM-DD)")
        run_range(a.start,a.end,cfg)

if __name__=="__main__":
    main()
