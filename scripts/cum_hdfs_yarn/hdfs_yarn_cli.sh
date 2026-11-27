#!/usr/bin/env bash
# =============================================================================
# HDFS CLI + YARN CLI cho cụm Docker của dự án: đưa tầng Bronze lên HDFS, liệt kê các tầng, ghi trạng thái cụm.
# Chạy TRÊN MÁY (không phải trong container), sau khi cụm đã lên:
#     docker compose -f docker/docker-compose.yml up -d
#     bash scripts/cum_hdfs_yarn/hdfs_yarn_cli.sh truoc    # trước khi chạy Spark: tạo thư mục, đưa data/lake/bronze lên /mis5007/bronze
#     bash scripts/cum_hdfs_yarn/hdfs_yarn_cli.sh sau      # sau khi chạy Spark: liệt kê bronze / silver / gold, kiểm tra sức khoẻ HDFS
# Mọi lệnh và kết quả được ghi vào logs/hdfs_yarn_cli_*.log làm minh chứng.
# =============================================================================
set -u                                                  # dừng nếu dùng biến chưa khai báo
cd "$(dirname "$0")/../.." || exit 1                       # về gốc dự án (script nằm ở scripts/cum_hdfs_yarn/)
MODE="${1:-truoc}"                                      # chế độ: truoc (mặc định) hoặc sau
H="${HDFS_DIR:-/mis5007}"                               # thư mục gốc của dự án trên HDFS
NN="${NN:-mis5007-namenode}"; RM="${RM:-mis5007-resourcemanager}"   # tên container NameNode và ResourceManager
mkdir -p logs                                           # bảo đảm có thư mục log
LOG="logs/hdfs_yarn_cli_${MODE}_$(date -u +%Y%m%dT%H%M%SZ).log"     # file log của lần chạy
run(){ echo "\$ $*" | tee -a "$LOG"; "$@" 2>&1 | tee -a "$LOG"; echo | tee -a "$LOG"; }   # hàm: in lệnh, chạy, ghi kết quả

if [ "$MODE" = "truoc" ]; then                          # chế độ trước khi chạy Spark
  [ -d data/lake/bronze ] || { echo "Thieu data/lake/bronze - chay scripts/chay_toan_bo.sh truoc" | tee -a "$LOG"; exit 1; }   # chưa có Bronze thì dừng
  run docker exec "$NN" hdfs dfsadmin -safemode wait   # chờ HDFS thoát chế độ an toàn
  run docker exec "$NN" hdfs dfs -mkdir -p "$H/bronze" "$H/silver" "$H/gold"   # tạo 3 tầng
  run docker exec "$NN" rm -rf /tmp/bronze              # xoá bản sao tạm cũ trong container
  run docker cp data/lake/bronze "$NN":/tmp/bronze      # chép Bronze từ máy vào container NameNode
  run docker exec "$NN" bash -c "hdfs dfs -rm -r -f -skipTrash $H/bronze && hdfs dfs -put /tmp/bronze $H/bronze"   # đưa lên HDFS (thay bản cũ)
  run docker exec "$NN" hdfs dfs -ls -R -h "$H/bronze"  # liệt kê
  run docker exec "$NN" hdfs dfs -count -h "$H/bronze"  # đếm thư mục, file, dung lượng
else                                                    # chế độ sau khi chạy Spark
  run docker exec "$NN" hdfs dfs -setrep -R 1 "$H"      # cụm 1 DataNode: đưa mọi file về hệ số nhân bản 1
  run docker exec "$NN" hdfs dfs -ls -h "$H" "$H/silver" "$H/gold"   # liệt kê các tầng
  run docker exec "$NN" hdfs dfs -ls -h "$H/silver/fact_dan_so_o_nam" "$H/silver/quarantine_records" "$H/silver/excluded_records"   # phân vùng của vài bảng
  run docker exec "$NN" hdfs dfs -du -h "$H"            # dung lượng từng tầng
fi                                                      # hết nhánh
run docker exec "$NN" hdfs dfsadmin -report             # báo cáo cụm HDFS
run docker exec "$NN" hdfs fsck "$H"                    # kiểm tra sức khoẻ (Status: HEALTHY)
run docker exec "$RM" yarn version                      # phiên bản YARN
run docker exec "$RM" yarn node -list -all              # danh sách NodeManager
run docker exec "$RM" yarn application -list -appStates ALL   # các ứng dụng đã chạy trên YARN
run docker exec "$RM" yarn queue -status default        # hàng đợi mặc định
echo "Da ghi $LOG"                                      # thông báo
