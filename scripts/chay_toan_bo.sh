#!/usr/bin/env bash
# =============================================================================
# CHẠY TOÀN BỘ PIPELINE (sau khi đã tải dữ liệu bằng 1a_tai_du_lieu.sh):
#   1_thu_thap/1b  chuyển đổi đa nguồn vào Bronze  ->  1_thu_thap/1c  tra cứu không gian  ->  2_spark_5_buoc/chay_spark  Spark 5 bước  ->  3_bao_cao/bao_cao
# Chạy:  bash scripts/chay_toan_bo.sh      (trong VS Code: Cmd+Shift+B)
# Dừng ngay ở bước đầu tiên bị lỗi; mỗi bước ghi log riêng trong logs/.
# =============================================================================
set -euo pipefail                                       # dừng khi có lỗi, biến chưa khai báo, hoặc lỗi trong chuỗi lệnh "|"
cd "$(dirname "$0")"                                    # chuyển vào thư mục scripts/
ROOT="$(cd .. && pwd)"                                  # thư mục gốc dự án
PY="${PYTHON:-$ROOT/.venv/bin/python}"                  # trình thông dịch Python: biến PYTHON hoặc .venv của dự án
[ -x "$PY" ] || { echo "Khong thay $PY - tao .venv theo docs/HUONG_DAN_CHAY.md"; exit 1; }   # không có Python thì dừng
for buoc in 1_thu_thap/1b_chuyen_doi_bronze 1_thu_thap/1c_tra_cuu_khong_gian 2_spark_5_buoc/chay_spark 3_bao_cao/bao_cao; do   # 4 phần theo thứ tự bước
  echo "=== $buoc ==="                                  # in tên bước
  "$PY" "$buoc.py"                                      # chạy bước
done                                                    # hết vòng
echo "Xong. Ket qua: outputs/tables (CSV), outputs/figures (PNG), data/lake (bronze/silver/gold); doi soat: outputs/tables/7_kiem_chung/doi_soat_spark.csv"   # thông báo
