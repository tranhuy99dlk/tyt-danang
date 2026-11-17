#!/usr/bin/env bash
# =============================================================================
# 00 - Kiem tra may truoc khi chay du an: cong cu, file phai tu chep, dia, RAM Docker, cong mang.
# Chay:  bash scripts/00_check.sh        (khong sua gi tren may; ma thoat 0 = du dieu kien)
# =============================================================================
cd "$(dirname "$0")/.." || exit 1
bad=0
ok(){ echo "  [co]    $*"; }
miss(){ echo "  [THIEU] $*"; bad=1; }
warn(){ echo "  [LUU Y] $*"; }

echo "1. Cong cu dong lenh"
for c in bash curl shasum xargs; do command -v "$c" >/dev/null 2>&1 && ok "$c" || miss "$c - can cho scripts/01_download.sh"; done
if [ -x .venv/bin/python ]; then ok "moi truong ao .venv ($(.venv/bin/python --version 2>&1))"
elif command -v uv >/dev/null 2>&1; then warn "chua co .venv - tao bang: uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt"
elif command -v python3.11 >/dev/null 2>&1 || command -v python3.12 >/dev/null 2>&1; then warn "chua co .venv - tao bang: python3.11 -m venv .venv && .venv/bin/pip install -r requirements.lock.txt"
else miss "Python 3.11/3.12 hoac uv - xem docs/HUONG_DAN_CAI_DAT.md muc 2"; fi

echo "2. Docker"
if command -v docker >/dev/null 2>&1; then
  ok "$(docker --version)"
  docker compose version >/dev/null 2>&1 && ok "$(docker compose version)" || miss "docker compose (Docker Desktop moi da kem san)"
  if docker info >/dev/null 2>&1; then
    mem=$(docker info --format '{{.MemTotal}}' 2>/dev/null); gb=$(( ${mem:-0} / 1073741824 ))
    [ "$gb" -ge 6 ] && ok "Docker dang chay, RAM cap cho Docker ~${gb} GB" || warn "RAM cap cho Docker ~${gb} GB (< 6 GB): tang trong Docker Desktop > Settings > Resources"
  else miss "Docker chua chay - mo Docker Desktop roi chay lai lenh nay"; fi
else miss "docker - cai Docker Desktop (https://www.docker.com/products/docker-desktop/)"; fi

echo "3. File phai tu chep vao (khong tai duoc bang script)"
for f in data/raw/location-tyt.xlsx data/reference/danh_sach_56_tram.csv data/reference/dan_so_chinh_thuc_nq1251.csv; do
  [ -s "$f" ] && ok "$f" || miss "$f"; done

echo "4. Dia trong"
free_kb=$(df -k . | tail -1 | awk '{print $4}'); free_gb=$(( free_kb / 1048576 ))
[ "$free_gb" -ge 20 ] && ok "con ~${free_gb} GB" || warn "con ~${free_gb} GB (< 20 GB): du lieu ~1,3 GB, image Docker ~10 GB"

echo "5. Cong mang cua cum (9871 NameNode, 9864 DataNode, 8088 YARN, 8042 NodeManager, 8888 JupyterLab, 4041 Spark UI)"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^mis5007-namenode$'; then ok "cum mis5007 dang chay (cac cong dang do cum nay dung)"
else
  for p in 9871 9864 8088 8042 8888 4041; do
    if (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null; then exec 3>&- 3<&-; warn "cong $p dang bi chuong trinh khac dung - doi cong trong docker/docker-compose.yml"; else ok "cong $p trong"; fi
  done
fi
echo
[ $bad = 0 ] && echo "KET QUA: du dieu kien. Buoc tiep: docs/HUONG_DAN_CAI_DAT.md muc 5." || echo "KET QUA: con muc [THIEU] o tren - xu ly truoc khi chay."
exit $bad
