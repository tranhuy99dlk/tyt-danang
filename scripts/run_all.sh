#!/usr/bin/env bash
# Chay toan bo pipeline sau khi da tai du lieu (01_download.sh). Dung khi co loi.
set -euo pipefail
cd "$(dirname "$0")"
PY="${PYTHON:-python3}"
# 08_forecast: du bao toi 2030 tu chuoi 2015-2024 (bat lai 17/11/2026). Buoc 11 (Spark) chay trong container.
for s in 02_boundaries 03_grid 04_stations 05_network 06_kpi 07_diagnose 08_forecast 09_greedy 13_giao_thong 14_ban_do 10_quality 12_export_csv 11a_landing; do
  echo "=== $s ==="
  "$PY" "$s.py"
done
echo "Xong. Ket qua: ../outputs/tables, ../outputs/figures; nhat ky: ../logs; danh muc file: ../outputs/run_manifest.csv"
