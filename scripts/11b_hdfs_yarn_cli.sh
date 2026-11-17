#!/usr/bin/env bash
# =============================================================================
# 11b - HDFS CLI + YARN CLI: dua CSV tho len vung landing cua HDFS, liet ke cac vung, ghi trang thai cum.
# Chay TREN MAY (khong phai trong container), sau khi cum cua du an da len:
#     docker compose -f docker/docker-compose.yml up -d
#     bash scripts/11b_hdfs_yarn_cli.sh truoc    # truoc khi chay Spark: tao vung, dua CSV tho len
#     bash scripts/11b_hdfs_yarn_cli.sh sau      # sau khi chay Spark: liet ke curated / quarantine / gold
# Moi lenh va ket qua duoc ghi vao logs/11b_hdfs_yarn_cli_*.log lam minh chung.
# =============================================================================
set -u
cd "$(dirname "$0")/.." || exit 1
MODE="${1:-truoc}"
H="${HDFS_DIR:-/mis5007}"
NN="${NN:-mis5007-namenode}"; RM="${RM:-mis5007-resourcemanager}"   # ten container (xem docker/docker-compose.yml)
LOG="logs/11b_hdfs_yarn_cli_${MODE}_$(date -u +%Y%m%dT%H%M%SZ).log"
run(){ echo "\$ $*" | tee -a "$LOG"; "$@" 2>&1 | tee -a "$LOG"; echo | tee -a "$LOG"; }

if [ "$MODE" = "truoc" ]; then
  [ -s data/landing/pop_100m_raw.csv ] || { echo "Thieu data/landing/*.csv - chay scripts/11a_landing.py truoc" | tee -a "$LOG"; exit 1; }
  run docker exec "$NN" hdfs dfsadmin -safemode wait
  run docker exec "$NN" hdfs dfs -mkdir -p "$H/landing" "$H/curated" "$H/quarantine" "$H/gold"
  run docker exec "$NN" rm -rf /tmp/landing
  run docker cp data/landing "$NN":/tmp/landing
  run docker exec "$NN" bash -c "hdfs dfs -put -f /tmp/landing/*.csv $H/landing/"
  run docker exec "$NN" hdfs dfs -ls -h "$H/landing"
  run docker exec "$NN" hdfs dfs -count -h "$H/landing"
else
  run docker exec "$NN" hdfs dfs -setrep -R 1 "$H"       # cum 1 DataNode: dua moi file ve he so nhan ban 1
  run docker exec "$NN" hdfs dfs -ls -h "$H" "$H/landing" "$H/curated" "$H/quarantine" "$H/gold"
  run docker exec "$NN" hdfs dfs -ls -h "$H/curated/fact_dan_so_o_nam" "$H/quarantine/dong_loi"
  run docker exec "$NN" hdfs dfs -du -h "$H"
fi
run docker exec "$NN" hdfs dfsadmin -report
run docker exec "$NN" hdfs fsck "$H"
run docker exec "$RM" yarn version
run docker exec "$RM" yarn node -list -all
run docker exec "$RM" yarn application -list -appStates ALL
run docker exec "$RM" yarn queue -status default
echo "Da ghi $LOG"
