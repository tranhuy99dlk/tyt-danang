#!/usr/bin/env bash
# =============================================================================
# 01_download.sh - Tai du lieu nguon mo cho de tai tram y te Da Nang cu (MIS5007)
#   S1 WorldPop Global2 R2025A (CC BY 4.0)
#   S2 OpenStreetMap qua Overpass API (ODbL 1.0, (c) OpenStreetMap contributors)
#   Bo tro: AWS Terrain Tiles (do cao), GADM 4.1 (ranh gioi doi chieu, chi dung hoc thuat)
# Chay:  bash 01_download.sh          (mac dinh: 105 file + 4 truy van OSM, khoang 0,9 GB)
#        FULL=1 bash 01_download.sh   (them 60+ 100 m nam 2024)
# Pham vi du an: 2015-2024 (10 moc nam). Luoi 100 m tai du ca 10 nam.
# Chay lai an toan: file da tai va da ghi manifest se duoc bo qua.
# =============================================================================
set -u
SELF="$0"
# ---- che do con: tai mot file (goi qua xargs de chay song song) -------------
if [ "${1:-}" = "__dl" ]; then
  url="$2"; dest="$3"; src="$4"; lic="$5"; ROOT="$6"; MAN="$7"; LOGF="$8"; UA="$9"
  ts(){ date -u +%Y-%m-%dT%H:%M:%SZ; }
  rel="${dest#$ROOT/}"
  if [ -s "$dest" ] && grep -q ",$url," "$MAN" 2>/dev/null; then echo "[$(ts)] SKIP $rel" >> "$LOGF"; exit 0; fi
  tmp="$dest.part"
  for i in 1 2 3 4; do
    if curl -sS -L -A "$UA" --fail --retry 3 --connect-timeout 30 -o "$tmp" "$url" 2>>"$LOGF"; then mv "$tmp" "$dest"; break; fi
    echo "[$(ts)] RETRY $i $url" >> "$LOGF"; sleep $((i*15))
  done
  if [ ! -s "$dest" ]; then echo "[$(ts)] FAIL $url" | tee -a "$LOGF"; exit 1; fi
  b=$(wc -c < "$dest" | tr -d ' '); h=$(shasum -a 256 "$dest" | cut -d' ' -f1)
  echo "$rel,$url,$b,$h,$(ts),\"$src\",\"$lic\"" >> "$MAN"
  echo "[$(ts)] OK $rel ($b bytes)" | tee -a "$LOGF"
  exit 0
fi

cd "$(dirname "$SELF")/.." || exit 1
ROOT="$(pwd)"
RAW="$ROOT/data/raw"; LOG="$ROOT/logs"
mkdir -p "$RAW/worldpop/pop100m" "$RAW/worldpop/pop1km" "$RAW/worldpop/age1km" "$RAW/worldpop/age100m" "$RAW/worldpop/doc" \
         "$RAW/osm/queries" "$RAW/terrain" "$RAW/gadm" "$LOG"
LOGF="$LOG/01_download_$(date -u +%Y%m%dT%H%M%SZ).log"
MAN="$ROOT/data/manifest_raw.csv"
UA="MIS5007-coursework-danang-health-access/1.0 (student project; curl)"
WP="https://data.worldpop.org/GIS"
LIC_WP="CC BY 4.0 (WorldPop, University of Southampton)"
LIC_OSM="ODbL 1.0 ((c) OpenStreetMap contributors)"
BB="15.88,107.78,16.24,108.36"   # S,W,N,E - hop bao Da Nang cu (dat lien), co vung dem
PAR="${PAR:-4}"
[ -f "$MAN" ] || echo "file,url,bytes,sha256,downloaded_utc,source,license" > "$MAN"
log(){ echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOGF"; }
log "Bat dau tai. ROOT=$ROOT FULL=${FULL:-0} PAR=$PAR"

LIST="$LOG/.dl_list.tsv"; : > "$LIST"
add(){ printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$LIST"; }

# ---- S1 WorldPop -------------------------------------------------------------
add "https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf" "$RAW/worldpop/doc/Global2_Release_Statement_R2025A_v1.pdf" "WorldPop R2025A doc" "$LIC_WP"
add "https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/census/global2_census_data_sources_R2025A_v1.xlsx" "$RAW/worldpop/doc/global2_census_data_sources_R2025A_v1.xlsx" "WorldPop R2025A doc" "$LIC_WP"
YRS="2015 2016 2017 2018 2019 2020 2021 2022 2023 2024"
for y in $YRS; do
  add "$WP/Population/Global_2015_2030/R2025A/$y/VNM/v1/100m/constrained/vnm_pop_${y}_CN_100m_R2025A_v1.tif" "$RAW/worldpop/pop100m/vnm_pop_${y}_CN_100m_R2025A_v1.tif" "WorldPop Global2 R2025A pop 100m constrained" "$LIC_WP"
done
for y in $YRS; do
  add "$WP/Population/Global_2015_2030/R2025A/$y/VNM/v1/1km_ua/constrained/vnm_pop_${y}_CN_1km_R2025A_UA_v1.tif" "$RAW/worldpop/pop1km/vnm_pop_${y}_CN_1km_R2025A_UA_v1.tif" "WorldPop Global2 R2025A pop 1km UA constrained" "$LIC_WP"
  for a in 60 65 70 75 80 85 90; do
    add "$WP/AgeSex_structures/Global_2015_2030/R2025A/$y/VNM/v1/1km_ua/constrained/vnm_t_${a}_${y}_CN_1km_R2025A_UA_v1.tif" "$RAW/worldpop/age1km/vnm_t_${a}_${y}_CN_1km_R2025A_UA_v1.tif" "WorldPop Global2 R2025A agesex 1km UA constrained" "$LIC_WP"
  done
done
if [ "${FULL:-0}" = "1" ]; then
  for a in 60 65 70 75 80 85 90; do
    add "$WP/AgeSex_structures/Global_2015_2030/R2025A/2024/VNM/v1/100m/constrained/vnm_t_${a}_2024_CN_100m_R2025A_v1.tif" "$RAW/worldpop/age100m/vnm_t_${a}_2024_CN_100m_R2025A_v1.tif" "WorldPop Global2 R2025A agesex 100m constrained" "$LIC_WP"
  done
fi
# ---- Bo tro: do cao AWS Terrain Tiles (z11) ---------------------------------
for x in 1637 1638 1639 1640; do for y in 930 931 932; do
  add "https://s3.amazonaws.com/elevation-tiles-prod/geotiff/11/$x/$y.tif" "$RAW/terrain/z11_${x}_${y}.tif" "AWS Terrain Tiles (Mapzen) z11" "Mapzen Terrain Tiles - attribution required (SRTM/NASA public domain and others)"
done; done
# ---- Bo tro: GADM 4.1 cap xa (ranh gioi truoc 2025, doi chieu) -------------
add "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_VNM_3.json.zip" "$RAW/gadm/gadm41_VNM_3.json.zip" "GADM 4.1 VNM level 3" "GADM licence: academic/non-commercial use, no redistribution"

N=$(wc -l < "$LIST" | tr -d ' ')
log "Tai $N file (song song $PAR luong)..."
# Khong dung -I{}: xargs cua macOS gioi han chuoi thay the 255 byte, dong trong $LIST dai hon -> xargs dung ngay.
# Moi dong duoc xargs noi vao cuoi lam tham so $6.
tr '\n' '\0' < "$LIST" | xargs -0 -n1 -P "$PAR" bash -c 'IFS=$'"'"'\t'"'"' read -r u d s l <<< "$6"; bash "$1" __dl "$u" "$d" "$s" "$l" "$2" "$3" "$4" "$5"' _ "$ROOT/scripts/01_download.sh" "$ROOT" "$MAN" "$LOGF" "$UA" 2>>"$LOGF"
rc=$?
[ $rc = 0 ] || log "FAIL xargs ma thoat=$rc (123 = co file FAIL o tren; ma khac = xargs khong chay duoc, xem dong loi ngay truoc)"

# ---- S2 OpenStreetMap qua Overpass ------------------------------------------
NAMERE='[Tt][Rr](a|A|ạ|Ạ)[Mm] [Yy] [Tt](e|E|ế|Ế)|^TYT'
cat > "$RAW/osm/queries/stations_2024-12-31.overpassql" <<Q
[out:json][timeout:300][date:"2024-12-31T00:00:00Z"];
( nwr["name"~"$NAMERE"]($BB); nwr["amenity"~"^(clinic|doctors|hospital)\$"]($BB); nwr["healthcare"]($BB); );
out center tags;
Q
cat > "$RAW/osm/queries/stations_current.overpassql" <<Q
[out:json][timeout:300];
( nwr["name"~"$NAMERE"]($BB); nwr["amenity"~"^(clinic|doctors|hospital)\$"]($BB); nwr["healthcare"]($BB); );
out center tags;
Q
cat > "$RAW/osm/queries/admin_2024-12-31.overpassql" <<Q
[out:json][timeout:600][date:"2024-12-31T00:00:00Z"];
relation["boundary"="administrative"]["admin_level"~"^(6|8)\$"]($BB);
out geom;
Q
cat > "$RAW/osm/queries/roads_current.overpassql" <<Q
[out:json][timeout:900];
way["highway"~"^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street|service|track|road|motorway_link|trunk_link|primary_link|secondary_link|tertiary_link)\$"]($BB);
out geom;
Q
EPS="https://overpass-api.de/api/interpreter https://overpass.kumi.systems/api/interpreter https://maps.mail.ru/osm/tools/overpass/api/interpreter"
for q in stations_2024-12-31 stations_current admin_2024-12-31 roads_current; do
  out="$RAW/osm/$q.json"; qf="$RAW/osm/queries/$q.overpassql"
  if [ -s "$out" ] && grep -q "^data/raw/osm/$q.json," "$MAN"; then log "SKIP $q"; continue; fi
  ok=0
  for ep in $EPS; do
    for i in 1 2; do
      log "Overpass $q <- $ep (lan $i)"
      curl -sS -A "$UA" --fail --connect-timeout 30 --max-time 1200 -X POST --data-urlencode "data@$qf" -o "$out.part" "$ep" 2>>"$LOGF"
      if [ -s "$out.part" ] && head -c 200 "$out.part" | grep -q '"version"' && grep -q '"elements"' "$out.part"; then mv "$out.part" "$out"; ok=1; break; fi
      log "  that bai, thu lai"; sleep 30
    done
    [ $ok = 1 ] && break
  done
  if [ $ok = 1 ]; then
    b=$(wc -c < "$out" | tr -d ' '); h=$(shasum -a 256 "$out" | cut -d' ' -f1)
    echo "data/raw/osm/$q.json,$ep#query=data/raw/osm/queries/$q.overpassql,$b,$h,$(date -u +%Y-%m-%dT%H:%M:%SZ),\"OpenStreetMap via Overpass API\",\"$LIC_OSM\"" >> "$MAN"
    log "OK $q ($b bytes)"
  else log "FAIL $q"; fi
done

log "Tong ket:"; NOK=$(grep -c ' OK ' "$LOGF"); NF=$(grep -c ' FAIL ' "$LOGF"); log "  OK=$NOK  FAIL=$NF  manifest=$MAN"
du -sh "$RAW" | tee -a "$LOGF"
log "Xong. Neu co dong FAIL, chay lai lenh nay (file da co se duoc bo qua)."
