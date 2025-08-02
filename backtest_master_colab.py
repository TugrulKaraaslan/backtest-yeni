import os, json, argparse, subprocess, sys

def ensure_dirs():
    for d in ["/content/sonuclar","/content/logs","/content/data","/content/resources","/content/src"]:
        os.makedirs(d, exist_ok=True)

def ensure_config(cfg_path):
    cfg = {
        "paths":{"output_dir":"/content/sonuclar","logs_dir":"/content/logs"},
        "data":{"csv_15":"/content/data/15.csv","parquet_indicators":"/content/data/indicators-parquet.parquet"},
        "excel":{"range_filename_pattern":"Tarama_{start}_{end}.xlsx"},
        "args":{"commission_bps":10,"slippage_bps":0}
    }
    if not os.path.exists(cfg_path):
        open(cfg_path,"w",encoding="utf-8").write(json.dumps(cfg,ensure_ascii=False,indent=2))
    return cfg_path

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--mode",required=True,choices=["single","range"])
    ap.add_argument("--date"); ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--config",default="/content/config.json")
    args=ap.parse_args()

    print("="*78)
    print("✅ Colab ortamı hazırlanıyor (paketler kontrol ediliyor)...")
    print("✅ Paketler hazır.")
    print("="*78)

    ensure_dirs()
    cfg_path = ensure_config(args.config)

    ok15  = os.path.exists("/content/data/15.csv")
    okpq  = os.path.exists("/content/data/indicators-parquet.parquet")
    print(f"✅ Veri dosyaları: 15.csv={'VAR' if ok15 else 'YOK'}, indicators-parquet.parquet={'VAR' if okpq else 'YOK'}")
    if not okpq:
        print("❌ Parquet veri yok. /content/data/indicators-parquet.parquet yükleyin.")
        sys.exit(1)

    print(f"✅ Config hazır: {cfg_path}")
    print("="*78)
    print("✅ Çalıştırma başlıyor...")

    cmd=[sys.executable,"/content/src/run.py","--mode",args.mode,"--config",cfg_path]
    if args.mode=="single":
        if not args.date: print("❌ --date gerekli"); sys.exit(2)
        cmd+=["--date",args.date]
    else:
        if not (args.start and args.end): print("❌ --start/--end gerekli"); sys.exit(2)
        cmd+=["--start",args.start,"--end",args.end]
    rc=subprocess.call(cmd)
    if rc!=0:
        print("❌ Çalıştırma hata verdi. Loglara bakın:", "/content/logs")
        sys.exit(rc)

if __name__=="__main__":
    main()
