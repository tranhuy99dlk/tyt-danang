#!/usr/bin/env bash
# =============================================================================
# 1a – THU THẬP (giai đoạn 2 của vòng đời): TẢI DỮ LIỆU NGUỒN MỞ về data/raw/ (vùng nguồn, không sửa, không xoá)
#   S1 WorldPop Global2 R2025A (CC BY 4.0)            – dân số lưới 100 m, 1 km, nhóm tuổi
#   S2 OpenStreetMap qua Overpass API (ODbL 1.0)      – ranh giới hành chính, mạng đường
#   Bổ trợ: AWS Terrain Tiles (độ cao), GADM 4.1 (ranh giới, chỉ dùng học thuật)
# Chạy:  bash scripts/1_thu_thap/1a_tai_du_lieu.sh          (153 file + 2 truy vấn OSM, khoảng 1 GB)
# Chạy lại an toàn: file đã tải và đã ghi vào data/manifest_raw.csv sẽ được bỏ qua.
# Bước tiếp theo: python scripts/1_thu_thap/1b_chuyen_doi_bronze.py (nạp vào tầng Bronze).
# =============================================================================
set -u                                                  # dừng nếu dùng biến chưa khai báo
SELF="$0"                                               # đường dẫn của chính script (để gọi lại ở chế độ con)
# ---- chế độ con: tải MỘT file (được xargs gọi song song nhiều lần) ----------
if [ "${1:-}" = "__dl" ]; then                          # tham số đầu là "__dl" -> đang ở chế độ con
  url="$2"; dest="$3"; src="$4"; lic="$5"; ROOT="$6"; MAN="$7"; LOGF="$8"; UA="$9"   # nhận địa chỉ, nơi lưu, nguồn, giấy phép, gốc, sổ, log, tên trình duyệt
  ts(){ date -u +%Y-%m-%dT%H:%M:%SZ; }                  # hàm in giờ UTC
  rel="${dest#$ROOT/}"                                  # đường dẫn tương đối của file đích
  if [ -s "$dest" ] && grep -q ",$url," "$MAN" 2>/dev/null; then echo "[$(ts)] SKIP $rel" >> "$LOGF"; exit 0; fi   # đã tải và có trong sổ -> bỏ qua
  tmp="$dest.part"                                      # tải vào file tạm, xong mới đổi tên (tránh file dở dang)
  for i in 1 2 3 4; do                                  # thử tối đa 4 lần
    if curl -sS -L -A "$UA" --fail --retry 3 --connect-timeout 30 -o "$tmp" "$url" 2>>"$LOGF"; then mv "$tmp" "$dest"; break; fi   # tải được -> đổi tên, thoát vòng
    echo "[$(ts)] RETRY $i $url" >> "$LOGF"; sleep $((i*15))   # ghi log, chờ lâu dần rồi thử lại
  done                                                  # hết vòng thử
  if [ ! -s "$dest" ]; then echo "[$(ts)] FAIL $url" | tee -a "$LOGF"; exit 1; fi   # vẫn không có file -> báo FAIL
  b=$(wc -c < "$dest" | tr -d ' '); h=$(shasum -a 256 "$dest" | cut -d' ' -f1)      # dung lượng và mã SHA-256 của file
  echo "$rel,$url,$b,$h,$(ts),\"$src\",\"$lic\"" >> "$MAN"   # ghi một dòng vào sổ manifest_raw.csv
  echo "[$(ts)] OK $rel ($b bytes)" | tee -a "$LOGF"   # báo OK
  exit 0                                                # kết thúc chế độ con
fi                                                      # hết chế độ con

cd "$(dirname "$SELF")/../.." || exit 1                 # về thư mục gốc dự án (script nằm ở scripts/1_thu_thap/)
ROOT="$(pwd)"                                           # đường dẫn gốc
RAW="$ROOT/data/raw"; LOG="$ROOT/logs"                  # thư mục dữ liệu nguồn và log
mkdir -p "$RAW/worldpop/pop100m" "$RAW/worldpop/pop1km" "$RAW/worldpop/age1km" "$RAW/worldpop/doc" \
         "$RAW/osm/queries" "$RAW/terrain" "$RAW/gadm" "$LOG"   # tạo các thư mục cần thiết
LOGF="$LOG/1a_tai_du_lieu_$(date -u +%Y%m%dT%H%M%SZ).log"     # file log của lần chạy
MAN="$ROOT/data/manifest_raw.csv"                       # sổ ghi nguồn gốc mọi file tải về
UA="MIS5007-coursework-danang-health-access/1.0 (student project; curl)"   # tên trình duyệt chung (không chứa thông tin cá nhân)
WP="https://data.worldpop.org/GIS"                      # gốc địa chỉ WorldPop
LIC_WP="CC BY 4.0 (WorldPop, University of Southampton)"   # giấy phép WorldPop
LIC_OSM="ODbL 1.0 ((c) OpenStreetMap contributors)"     # giấy phép OSM
BB="15.88,107.78,16.24,108.36"                          # hộp bao Nam,Tây,Bắc,Đông (cùng hộp bao với common.BBOX)
PAR="${PAR:-4}"                                         # số luồng tải song song (mặc định 4)
[ -f "$MAN" ] || echo "file,url,bytes,sha256,downloaded_utc,source,license" > "$MAN"   # chưa có sổ thì tạo dòng tiêu đề
log(){ echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOGF"; }   # hàm ghi log ra màn hình và file
log "Bat dau tai. ROOT=$ROOT PAR=$PAR"                  # dòng log đầu

LIST="$LOG/.dl_list.tsv"; : > "$LIST"                   # danh sách file cần tải (làm rỗng trước)
add(){ printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$LIST"; }   # hàm thêm một dòng: địa chỉ, nơi lưu, nguồn, giấy phép

# ---- S1 WorldPop -------------------------------------------------------------
add "https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf" "$RAW/worldpop/doc/Global2_Release_Statement_R2025A_v1.pdf" "WorldPop R2025A doc" "$LIC_WP"   # tài liệu phát hành
add "https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/census/global2_census_data_sources_R2025A_v1.xlsx" "$RAW/worldpop/doc/global2_census_data_sources_R2025A_v1.xlsx" "WorldPop R2025A doc" "$LIC_WP"   # nguồn điều tra dân số
YRS="2015 2016 2017 2018 2019 2020 2021 2022 2023 2024" # 10 năm phân tích
for y in $YRS; do                                       # lưới 100 m từng năm
  add "$WP/Population/Global_2015_2030/R2025A/$y/VNM/v1/100m/constrained/vnm_pop_${y}_CN_100m_R2025A_v1.tif" "$RAW/worldpop/pop100m/vnm_pop_${y}_CN_100m_R2025A_v1.tif" "WorldPop Global2 R2025A pop 100m constrained" "$LIC_WP"   # thêm vào danh sách
done                                                    # hết vòng
for y in $YRS; do                                       # lưới 1 km + 7 nhóm tuổi từng năm
  add "$WP/Population/Global_2015_2030/R2025A/$y/VNM/v1/1km_ua/constrained/vnm_pop_${y}_CN_1km_R2025A_UA_v1.tif" "$RAW/worldpop/pop1km/vnm_pop_${y}_CN_1km_R2025A_UA_v1.tif" "WorldPop Global2 R2025A pop 1km UA constrained" "$LIC_WP"   # tổng dân 1 km
  for a in 60 65 70 75 80 85 90; do                     # 7 nhóm tuổi 60+
    add "$WP/AgeSex_structures/Global_2015_2030/R2025A/$y/VNM/v1/1km_ua/constrained/vnm_t_${a}_${y}_CN_1km_R2025A_UA_v1.tif" "$RAW/worldpop/age1km/vnm_t_${a}_${y}_CN_1km_R2025A_UA_v1.tif" "WorldPop Global2 R2025A agesex 1km UA constrained" "$LIC_WP"   # nhóm tuổi 1 km
  done                                                  # hết nhóm tuổi
done                                                    # hết năm
# Dự báo (Spark Bước 4.4): lưới 1 km các năm CHIẾU 2025–2030 của WorldPop – chỉ để đối chiếu và phân bố dân 2030 (A9), không vào KPI
for y in 2025 2026 2027 2028 2029 2030; do              # các năm chiếu
  add "$WP/Population/Global_2015_2030/R2025A/$y/VNM/v1/1km_ua/constrained/vnm_pop_${y}_CN_1km_R2025A_UA_v1.tif" "$RAW/worldpop/pop1km/vnm_pop_${y}_CN_1km_R2025A_UA_v1.tif" "WorldPop Global2 R2025A pop 1km UA constrained" "$LIC_WP"   # tổng dân 1 km
  for a in 60 65 70 75 80 85 90; do                     # 7 nhóm tuổi
    add "$WP/AgeSex_structures/Global_2015_2030/R2025A/$y/VNM/v1/1km_ua/constrained/vnm_t_${a}_${y}_CN_1km_R2025A_UA_v1.tif" "$RAW/worldpop/age1km/vnm_t_${a}_${y}_CN_1km_R2025A_UA_v1.tif" "WorldPop Global2 R2025A agesex 1km UA constrained" "$LIC_WP"   # nhóm tuổi 1 km
  done                                                  # hết nhóm tuổi
done                                                    # hết năm
# ---- Bổ trợ: độ cao AWS Terrain Tiles (mức phóng z11) ------------------------
for x in 1637 1638 1639 1640; do for y in 930 931 932; do   # 4 x 3 = 12 mảnh phủ Đà Nẵng
  add "https://s3.amazonaws.com/elevation-tiles-prod/geotiff/11/$x/$y.tif" "$RAW/terrain/z11_${x}_${y}.tif" "AWS Terrain Tiles (Mapzen) z11" "Mapzen Terrain Tiles - attribution required (SRTM/NASA public domain and others)"   # thêm
done; done                                              # hết hai vòng
# ---- Bổ trợ: GADM 4.1 cấp xã (ranh giới trước 2025) -------------------------
add "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_VNM_3.json.zip" "$RAW/gadm/gadm41_VNM_3.json.zip" "GADM 4.1 VNM level 3" "GADM licence: academic/non-commercial use, no redistribution"   # thêm

N=$(wc -l < "$LIST" | tr -d ' ')                        # số file cần tải
log "Tai $N file (song song $PAR luong)..."             # in
# Không dùng -I{}: xargs của macOS giới hạn chuỗi thay thế 255 byte. Mỗi dòng được xargs nối vào cuối làm tham số $6.
tr '\n' '\0' < "$LIST" | xargs -0 -n1 -P "$PAR" bash -c 'IFS=$'"'"'\t'"'"' read -r u d s l <<< "$6"; bash "$1" __dl "$u" "$d" "$s" "$l" "$2" "$3" "$4" "$5"' _ "$ROOT/scripts/1_thu_thap/1a_tai_du_lieu.sh" "$ROOT" "$MAN" "$LOGF" "$UA" 2>>"$LOGF"   # gọi chế độ con song song
rc=$?                                                   # mã thoát của xargs
[ $rc = 0 ] || log "FAIL xargs ma thoat=$rc (123 = co file FAIL o tren; ma khac = xargs khong chay duoc)"   # báo lỗi nếu có

# ---- S2 OpenStreetMap qua Overpass (nội dung truy vấn giữ nguyên từng byte như bản nhóm đã tải) ----
# (Hai truy vấn cơ sở y tế OSM của bản cũ đã bỏ: toạ độ trạm lấy từ file nhập tay, CSKCB từ danh sách của Sở Y tế.)
# truy vấn 1: ranh giới hành chính cấp quận (6) và xã (8) mốc 31/12/2024
cat > "$RAW/osm/queries/admin_2024-12-31.overpassql" <<Q  # ghi nội dung dưới đây (tới dòng Q) vào file truy vấn
[out:json][timeout:600][date:"2024-12-31T00:00:00Z"];
relation["boundary"="administrative"]["admin_level"~"^(6|8)\$"]($BB);
out geom;
Q
# truy vấn 2: mạng đường xe máy đi được
cat > "$RAW/osm/queries/roads_current.overpassql" <<Q  # ghi nội dung dưới đây (tới dòng Q) vào file truy vấn
[out:json][timeout:900];
way["highway"~"^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street|service|track|road|motorway_link|trunk_link|primary_link|secondary_link|tertiary_link)\$"]($BB);
out geom;
Q
EPS="https://overpass-api.de/api/interpreter https://overpass.kumi.systems/api/interpreter https://maps.mail.ru/osm/tools/overpass/api/interpreter"   # 3 máy chủ Overpass dự phòng
for q in admin_2024-12-31 roads_current; do           # từng truy vấn
  out="$RAW/osm/$q.json"; qf="$RAW/osm/queries/$q.overpassql"   # file kết quả và file truy vấn
  if [ -s "$out" ] && grep -q "^data/raw/osm/$q.json," "$MAN"; then log "SKIP $q"; continue; fi   # đã có (bản trong kho GitHub) -> bỏ qua
  ok=0                                                  # cờ thành công
  for ep in $EPS; do                                    # thử từng máy chủ
    for i in 1 2; do                                    # mỗi máy chủ thử 2 lần
      log "Overpass $q <- $ep (lan $i)"                 # in
      curl -sS -A "$UA" --fail --connect-timeout 30 --max-time 1200 -X POST --data-urlencode "data@$qf" -o "$out.part" "$ep" 2>>"$LOGF"   # gửi truy vấn
      if [ -s "$out.part" ] && head -c 200 "$out.part" | grep -q '"version"' && grep -q '"elements"' "$out.part"; then mv "$out.part" "$out"; ok=1; break; fi   # kết quả hợp lệ -> giữ
      log "  that bai, thu lai"; sleep 30               # thất bại -> chờ 30 giây
    done                                                # hết số lần thử
    [ $ok = 1 ] && break                                # thành công -> thôi thử máy chủ khác
  done                                                  # hết máy chủ
  if [ $ok = 1 ]; then                                  # thành công
    b=$(wc -c < "$out" | tr -d ' '); h=$(shasum -a 256 "$out" | cut -d' ' -f1)   # dung lượng, SHA-256
    echo "data/raw/osm/$q.json,$ep#query=data/raw/osm/queries/$q.overpassql,$b,$h,$(date -u +%Y-%m-%dT%H:%M:%SZ),\"OpenStreetMap via Overpass API\",\"$LIC_OSM\"" >> "$MAN"   # ghi sổ
    log "OK $q ($b bytes)"                              # in
  else log "FAIL $q"; fi                                # thất bại hẳn
done                                                    # hết truy vấn

log "Tong ket:"; NOK=$(grep -c ' OK ' "$LOGF"); NF=$(grep -c ' FAIL ' "$LOGF"); log "  OK=$NOK  FAIL=$NF  manifest=$MAN"   # đếm OK / FAIL
du -sh "$RAW" | tee -a "$LOGF"                          # dung lượng thư mục dữ liệu
log "Xong. Neu co dong FAIL, chay lai lenh nay (file da co se duoc bo qua). Buoc tiep: python scripts/1_thu_thap/1b_chuyen_doi_bronze.py"   # hướng dẫn bước tiếp
