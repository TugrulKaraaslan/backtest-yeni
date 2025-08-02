1) !pip -q install pandas openpyxl pyarrow fastparquet
2) ZIP + indicators-parquet.parquet + 15.csv yükle
3) from zipfile import ZipFile
   ZipFile('/content/backtest_PARQUET_DYNAMIC_20250802_170723.zip').extractall('/content')
4) Çalıştır:
   !python /content/backtest_master_colab.py --mode range --start 2025-03-07 --end 2025-03-11
