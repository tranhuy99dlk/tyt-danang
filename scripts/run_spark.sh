#!/usr/bin/env bash
# Chay buoc 11 (Spark) roi buoc 16 (so sanh dinh dang Parquet/CSV, doc vung curated cua buoc 11) trong MOT container
# Spark, che do local[*]; cac vung luu tru nam o data/spark/.
# Sau do chay buoc 15 (TYT va CSKCB) tren may, vi buoc 15 doc ket qua cua Spark
# (tram_vung_phuc_vu_spark.csv, kpi_theo_xa_spark.csv); cuoi cung buoc 17 chot so lieu.
# Khong can dung cum HDFS/YARN. Image mac dinh la image cong khai ma cum cua du an cung dung
# (jupyter/pyspark-notebook: Spark 3.5 + Python 3.11 + pandas + pyarrow). Doi bang bien SPARK_IMAGE.
# Tien do (cac dong INFO) hien ngay tren man hinh; canh bao cua Spark/Java duoc ghi vao logs/11_spark_stderr_*.log.
# Muon chay tren cum co HDFS: xem docs/HUONG_DAN_SU_DUNG.md muc 5.
set -uo pipefail
cd "$(dirname "$0")/.."
IMG="${SPARK_IMAGE:-jupyter/pyspark-notebook:latest}"
PY="${PYTHON:-.venv/bin/python}"
mkdir -p logs
ERR="logs/11_spark_stderr_$(date -u +%Y%m%dT%H%M%SZ).log"
echo "Chay Spark trong image $IMG (lan dau Docker phai tai image, co the mat nhieu phut)..."
if docker run --rm -v "$PWD":/work -w /work "$IMG" \
     bash -c "python scripts/11_spark_pipeline.py && python scripts/16_so_sanh_dinh_dang.py" 2>"$ERR"; then
  echo "Xong buoc 11 va 16. Ket qua: outputs/tables/*_spark.csv, doi_soat_spark.csv, so_sanh_parquet_csv.csv;"
  echo "ke hoach thuc thi: logs/plan_kpi_*.txt, logs/plan_kpi_sql_*.txt, logs/plan_pruning_*.txt; canh bao cua Spark: $ERR"
else
  rc=$?
  echo "LOI (ma thoat $rc). 30 dong cuoi cua $ERR:"
  tail -30 "$ERR"
  exit $rc
fi
echo "Chay buoc 15 (TYT va co so kham chua benh) bang $PY ..."
OUT15="$("$PY" -W ignore scripts/15_tuyen_tren.py 2>&1)"; rc15=$?
printf '%s\n' "$OUT15" | grep -E "INFO (Doi chieu|Nhom|Ung vien)|WARNING|LECH|Error" || true
if [ $rc15 = 0 ]; then
  echo "Xong buoc 15. Ket qua: outputs/tables/tyt_cskcb*.csv, ung_vien_gop_tram.csv, doi_soat_nq1251_*.csv"
else
  echo "LOI o buoc 15 (ma thoat $rc15):"; printf '%s\n' "$OUT15" | tail -15; exit $rc15
fi
echo "Chot so lieu (buoc 17): doi chieu SHA-256 voi run_manifest.csv ..."
if "$PY" -W ignore scripts/17_chot_so_lieu.py; then
  echo "Xong buoc 17. Ket qua: outputs/tables/so_lieu_chot.csv, kiem_tra_run_manifest.csv"
else
  rc17=$?
  echo "LOI o buoc 17 (ma thoat $rc17). Xem log moi nhat logs/17_chot_so_lieu_*.log."
  exit "$rc17"
fi
