#!/usr/bin/env bash
# =============================================================================
# 11c - Chuan bi de buoc 11 chay executor tren YARN.
# Image NodeManager cua hoc phan (bde2020, Debian 9, Java 8) KHONG co Python, nen executor PySpark khong khoi dong duoc.
# Thay vi sua image, dong goi mot moi truong Python (cung phien ban voi driver trong container JupyterLab) bang
# conda-pack, dua len HDFS; Spark gui kem cho moi executor qua spark.archives (cach chinh thuc trong tai lieu Spark,
# muc "Python Package Management").
# Chay TREN MAY, sau khi cum da len:   bash scripts/11c_yarn_python_env.sh
# Lam lai tu dau (vd doi image JupyterLab):  REBUILD=1 bash scripts/11c_yarn_python_env.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
NB="${NB:-mis5007-spark-notebook}"; NN="${NN:-mis5007-namenode}"
H="${HDFS_DIR:-/mis5007}"
OUT_DIR="data/yarn"; ARC="pyspark_env.tar.gz"
mkdir -p "$OUT_DIR" logs
LOG="logs/11c_yarn_python_env_$(date -u +%Y%m%dT%H%M%SZ).log"
run(){ echo "\$ $*" | tee -a "$LOG"; "$@" 2>&1 | tee -a "$LOG"; }

PYV=$(docker exec "$NB" python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
echo "Python cua driver (container $NB): $PYV" | tee -a "$LOG"
if [ ! -s "$OUT_DIR/$ARC" ] || [ "${REBUILD:-0}" = "1" ]; then
  # moi truong cho executor: Python cung phien ban + numpy (scripts/common.py import numpy; UDF norm_name nam trong common)
  run docker exec "$NB" bash -lc "rm -rf /tmp/yarn_pyenv && mamba create -y -q -p /tmp/yarn_pyenv -c conda-forge python=$PYV numpy conda-pack"
  run docker exec "$NB" bash -lc "/tmp/yarn_pyenv/bin/conda-pack -p /tmp/yarn_pyenv -o /tmp/$ARC --force -q"
  run docker cp "$NB:/tmp/$ARC" "$OUT_DIR/$ARC"
  docker exec "$NB" rm -rf /tmp/yarn_pyenv "/tmp/$ARC"
else
  echo "Da co $OUT_DIR/$ARC - dung lai (REBUILD=1 de dong goi lai)" | tee -a "$LOG"
fi
ls -lh "$OUT_DIR/$ARC" | tee -a "$LOG"
run docker exec "$NN" hdfs dfs -mkdir -p "$H/apps"
run docker cp "$OUT_DIR/$ARC" "$NN:/tmp/$ARC"
run docker exec "$NN" hdfs dfs -put -f "/tmp/$ARC" "$H/apps/$ARC"
run docker exec "$NN" hdfs dfs -chmod 644 "$H/apps/$ARC"
docker exec "$NN" rm -f "/tmp/$ARC"
run docker exec "$NN" hdfs dfs -ls -h "$H/apps"
echo "Xong. Chay buoc 11 tren YARN: xem docs/HUONG_DAN_SU_DUNG.md muc 5 (SPARK_MASTER=yarn). Nhat ky: $LOG" | tee -a "$LOG"
