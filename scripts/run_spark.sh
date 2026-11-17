#!/usr/bin/env bash
# Chay buoc 11 (Spark) trong MOT container Spark, che do local[*]; cac vung luu tru nam o data/spark/.
# Khong can dung cum HDFS/YARN. Image mac dinh la image cong khai ma cum cua du an cung dung
# (jupyter/pyspark-notebook: Spark 3.5 + Python 3.11 + pandas + pyarrow). Doi bang bien SPARK_IMAGE.
# Tien do (cac dong INFO) hien ngay tren man hinh; canh bao cua Spark/Java duoc ghi vao logs/11_spark_stderr_*.log.
# Muon chay tren cum co HDFS: xem docs/HUONG_DAN_SU_DUNG.md muc 5.
set -uo pipefail
cd "$(dirname "$0")/.."
IMG="${SPARK_IMAGE:-jupyter/pyspark-notebook:latest}"
mkdir -p logs
ERR="logs/11_spark_stderr_$(date -u +%Y%m%dT%H%M%SZ).log"
echo "Chay Spark trong image $IMG (lan dau Docker phai tai image, co the mat nhieu phut)..."
if docker run --rm -v "$PWD":/work -w /work "$IMG" python scripts/11_spark_pipeline.py 2>"$ERR"; then
  echo "Xong. Ket qua: outputs/tables/*_spark.csv, doi_soat_spark.csv; canh bao cua Spark: $ERR"
else
  rc=$?
  echo "LOI (ma thoat $rc). 30 dong cuoi cua $ERR:"
  tail -30 "$ERR"
  exit $rc
fi
