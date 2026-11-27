#!/usr/bin/env bash
# =============================================================================
# Chuẩn bị để pipeline Spark chạy executor trên YARN (làm MỘT lần).
# Image NodeManager của học phần (bde2020, amd64, Java 8) KHÔNG có Python, nên executor PySpark không khởi động được.
# Cách làm: đóng gói một môi trường Python bằng conda-pack, đưa lên HDFS; Spark gửi kèm cho mọi executor qua
# spark.archives (tài liệu Spark, mục "Python Package Management").
#   - Gói phải cùng KIẾN TRÚC với NodeManager (amd64). Trên máy Apple Silicon, container JupyterLab là arm64 nên không
#     đóng gói trong đó được -> đóng gói trong một container miniforge chạy theo kiến trúc của NodeManager.
#   - Gói ghim ĐÚNG phiên bản Python, numpy, pandas, pyarrow, statsmodels của driver (container JupyterLab), vì UDF đổi
#     toạ độ và hàm dự báo applyInPandas chạy trên executor và trao đổi dữ liệu với driver.
# Chạy TRÊN MÁY, sau khi cụm đã lên:   bash scripts/cum_hdfs_yarn/yarn_python_env.sh      (REBUILD=1 để đóng gói lại)
# TRẠNG THÁI: đã chạy thử 26/11/2026 trên MacBook Apple Silicon – đóng gói khoảng 1 phút, gói 247 MB; pipeline chạy executor
#   trên YARN 2 lần, cả 2 ứng dụng FINISHED/SUCCEEDED, 47/47 đối soát ĐẠT (logs/02_08_spark_20261126T155537Z.log);
#   27/11/2026 chạy lại với pipeline 5 bước: 184 giây, 47/47 ĐẠT (logs/02_08_spark_20261126T165529Z.log).
# =============================================================================
set -euo pipefail                                       # dừng khi có lỗi
cd "$(dirname "$0")/../.."                                 # về gốc dự án (script nằm ở scripts/cum_hdfs_yarn/)
NB="${NB:-mis5007-spark-notebook}"; NN="${NN:-mis5007-namenode}"; NM="${NM:-mis5007-nodemanager}"   # tên container JupyterLab, NameNode, NodeManager
H="${HDFS_DIR:-/mis5007}"                               # thư mục dự án trên HDFS
ANH="${MINIFORGE_IMAGE:-condaforge/miniforge3:24.3.0-0}"   # image dùng để đóng gói (có mamba và conda-forge)
OUT_DIR="data/yarn"; ARC="pyspark_env.tar.gz"           # nơi lưu gói trên máy và tên gói
mkdir -p "$OUT_DIR" logs                                # tạo thư mục
LOG="logs/yarn_python_env_$(date -u +%Y%m%dT%H%M%SZ).log"   # file log
run(){ echo "\$ $*" | tee -a "$LOG"; "$@" 2>&1 | tee -a "$LOG"; }   # hàm: in lệnh, chạy, ghi log

KT=$(docker image inspect --format '{{.Architecture}}' "$(docker inspect --format '{{.Config.Image}}' "$NM")")   # kiến trúc image NodeManager (amd64)
PB=$(docker exec "$NB" python -c 'import sys,numpy,pandas,pyarrow,statsmodels as s; print(f"python={sys.version_info[0]}.{sys.version_info[1]} numpy={numpy.__version__} pandas={pandas.__version__} pyarrow={pyarrow.__version__} statsmodels={s.__version__}")')   # phiên bản thư viện của driver
echo "NodeManager: linux/$KT | thu vien cua driver: $PB" | tee -a "$LOG"   # in thông tin
if [ ! -s "$OUT_DIR/$ARC" ] || [ "${REBUILD:-0}" = "1" ]; then   # chưa có gói hoặc yêu cầu đóng gói lại
  run docker run --rm --platform "linux/$KT" -v "$PWD/$OUT_DIR:/out" "$ANH" bash -lc \
    "mamba create -y -q -p /tmp/env -c conda-forge $PB conda-pack && /tmp/env/bin/conda-pack -p /tmp/env -o /out/$ARC --force -q"   # tạo môi trường đúng kiến trúc rồi đóng gói ra data/yarn/
else                                                    # đã có gói
  echo "Da co $OUT_DIR/$ARC - dung lai (REBUILD=1 de dong goi lai)" | tee -a "$LOG"   # dùng lại
fi                                                      # hết nhánh
ls -lh "$OUT_DIR/$ARC" | tee -a "$LOG"                  # in dung lượng gói
run docker exec "$NN" hdfs dfs -mkdir -p "$H/apps"      # thư mục chứa gói trên HDFS
run docker cp "$OUT_DIR/$ARC" "$NN:/tmp/$ARC"           # chép gói vào NameNode
run docker exec "$NN" hdfs dfs -put -f "/tmp/$ARC" "$H/apps/$ARC"   # đưa lên HDFS
run docker exec "$NN" hdfs dfs -chmod 644 "$H/apps/$ARC"   # cho phép mọi người đọc
docker exec "$NN" rm -f "/tmp/$ARC"                     # dọn file tạm
run docker exec "$NN" hdfs dfs -ls -h "$H/apps"         # liệt kê
echo "Xong. Chay pipeline tren YARN: xem docs/HUONG_DAN_CHAY.md Phan B (SPARK_MASTER=yarn). Nhat ky: $LOG" | tee -a "$LOG"   # hướng dẫn
