#!/usr/bin/env bash
# =============================================================================
# 0 – KIỂM TRA MÁY trước khi chạy dự án: công cụ, Python (.venv), Java + PySpark, Docker, file bắt buộc, ổ đĩa, cổng mạng.
# Chạy:  bash scripts/0_kiem_tra_may.sh     (không sửa gì trên máy; mã thoát 0 = đủ điều kiện)
# =============================================================================
cd "$(dirname "$0")/.." || exit 1                       # chuyển về thư mục gốc dự án (cha của scripts/)
bad=0                                                   # cờ: 1 = có mục bắt buộc còn thiếu
ok(){ echo "  [co]    $*"; }                            # hàm in một mục đạt
miss(){ echo "  [THIEU] $*"; bad=1; }                   # hàm in một mục thiếu và bật cờ
warn(){ echo "  [LUU Y] $*"; }                          # hàm in một lưu ý (không bắt buộc)

echo "1. Cong cu dong lenh"                             # tiêu đề nhóm 1
for c in bash curl shasum xargs; do                     # các lệnh mà script tải dữ liệu cần
  command -v "$c" >/dev/null 2>&1 && ok "$c" || miss "$c - can cho scripts/1_thu_thap/1a_tai_du_lieu.sh"   # có thì [co], không thì [THIEU]
done                                                    # hết vòng lặp

echo "2. Python va thu vien"                            # tiêu đề nhóm 2
if [ -x .venv/bin/python ]; then                        # đã có môi trường ảo .venv
  ok "moi truong ao .venv ($(.venv/bin/python --version 2>&1))"                                   # in phiên bản Python
  .venv/bin/python -c "import pandas, rasterio, shapely, pyproj, scipy, statsmodels, pyarrow, openpyxl" 2>/dev/null \
    && ok "thu vien xu ly du lieu (pandas, rasterio, shapely, scipy...)" \
    || miss "thu vien trong .venv - chay: uv pip install --python .venv/bin/python -r requirements.lock.txt"  # kiểm tra import được thư viện
  .venv/bin/python -c "import pyspark" 2>/dev/null \
    && ok "pyspark $(.venv/bin/python -c 'import pyspark; print(pyspark.__version__)')" \
    || miss "pyspark trong .venv - chay: uv pip install --python .venv/bin/python -r requirements.lock.txt"   # kiểm tra PySpark
elif command -v uv >/dev/null 2>&1; then                # chưa có .venv nhưng có uv
  miss "chua co .venv - tao bang: uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt"  # hướng dẫn tạo
else                                                    # không có cả uv
  miss "uv (hoac Python 3.11) - xem docs/HUONG_DAN_CHAY.md muc A1"                              # hướng dẫn cài
fi                                                      # hết nhánh

echo "3. Java (Spark can Java 17)"                      # tiêu đề nhóm 3
JH="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"         # JAVA_HOME nếu đã đặt, nếu không thì đường dẫn Homebrew mặc định
[ -x "$JH/bin/java" ] || JH="/usr/local/opt/openjdk@17"  # máy Intel: Homebrew cài ở /usr/local
if [ -x "$JH/bin/java" ]; then                          # tìm thấy Java
  ok "$("$JH/bin/java" -version 2>&1 | head -1)  ($JH)" # in phiên bản Java
else                                                    # không thấy
  miss "Java 17 - cai bang: brew install openjdk@17"    # hướng dẫn cài
fi                                                      # hết nhánh

echo "4. Docker (chi can khi trinh dien HDFS / YARN)"   # tiêu đề nhóm 4 – không bắt buộc cho pipeline chính
if command -v docker >/dev/null 2>&1; then              # có lệnh docker
  ok "$(docker --version)"                              # in phiên bản
  if docker info >/dev/null 2>&1; then                  # Docker Desktop đang chạy
    mem=$(docker info --format '{{.MemTotal}}' 2>/dev/null); gb=$(( ${mem:-0} / 1073741824 ))   # bộ nhớ cấp cho Docker (GB)
    [ "$gb" -ge 6 ] && ok "Docker dang chay, RAM ~${gb} GB" || warn "RAM cap cho Docker ~${gb} GB (< 6 GB)"  # đủ 6 GB không
  else                                                  # đã cài nhưng chưa mở
    warn "Docker chua chay - chi can khi dung cum HDFS/YARN (mo Docker Desktop)"                  # lưu ý
  fi                                                    # hết nhánh
else                                                    # chưa cài Docker
  warn "chua cai Docker - chi can khi dung cum HDFS/YARN"                                         # lưu ý
fi                                                      # hết nhánh

echo "5. File phai tu chep vao (khong tai duoc bang script)"   # tiêu đề nhóm 5
for f in data/raw/location-tyt.xlsx data/raw/danh-sach-cskcb.xlsx data/reference/danh_sach_56_tram.csv \
         data/reference/dan_so_chinh_thuc_nq1251.csv data/reference/nq1251_sap_xep.csv data/reference/tt30_2024_goi_dich_vu.csv; do  # các file nhóm tự chuẩn bị
  [ -s "$f" ] && ok "$f" || miss "$f"                   # có và không rỗng thì [co]
done                                                    # hết vòng lặp

echo "6. Dia trong"                                     # tiêu đề nhóm 6
free_gb=$(( $(df -k . | tail -1 | awk '{print $4}') / 1048576 ))   # dung lượng trống (GB) của ổ chứa dự án
[ "$free_gb" -ge 5 ] && ok "con ~${free_gb} GB" || warn "con ~${free_gb} GB (< 5 GB): du lieu ~1,3 GB, ho du lieu ~0,3 GB"   # đủ 5 GB không

echo "7. Cong mang cua cum Docker (9871, 9864, 8088, 8042, 8888, 4041)"   # tiêu đề nhóm 7
for p in 9871 9864 8088 8042 8888 4041; do              # các cổng cụm HDFS/YARN/JupyterLab dùng
  if (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null; then exec 3>&- 3<&-; warn "cong $p dang duoc dung (cum mis5007 dang chay hoac chuong trinh khac)"   # thử mở kết nối: được = cổng đang bận
  else ok "cong $p trong"; fi                           # cổng rảnh
done                                                    # hết vòng lặp
echo                                                    # dòng trống
[ $bad = 0 ] && echo "KET QUA: du dieu kien." || echo "KET QUA: con muc [THIEU] o tren - xu ly truoc khi chay."   # kết luận
exit $bad                                               # mã thoát: 0 = đạt, 1 = thiếu
