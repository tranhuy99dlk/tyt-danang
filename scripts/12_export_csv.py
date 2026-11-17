# -*- coding: utf-8 -*-
"""12 - Xuat luoi dan cu (tu raster .tif WorldPop) va khoang cach tiep can ra CSV, kem bang giai thich.

Khong tinh so moi: chi doc data/interim/grid_access.parquet (san pham cua 03 + 05 + 06) va ghi lai
duoi dang CSV de doc bang Excel / nap vao cong cu khac. Giu nguyen ten cot goc.

Dau ra (outputs/tables/):
  dan_cu_luoi_100m.csv     moi dong = 1 o 100 m cua raster WorldPop co dan, nam trong 56 xa/phuong
  tiep_can_luoi_100m.csv   cung cac o do: khoang cach chim bay + duong di ngan nhat toi tram gan nhat
  giai_thich_csv_luoi.csv  y nghia tung cot, don vi, nguon, gia dinh lien quan
Chay sau 06_kpi.py:  cd scripts && python 12_export_csv.py
"""
import pandas as pd

from common import INTERIM, TAB, BASE_YEAR, YEARS, get_logger, save_csv

S = "12_export_csv"
log = get_logger(S)

PY = [f"pop_{y}" for y in YEARS]
B = BASE_YEAR
POP_COLS = ["cell_id", "lon", "lat", "ma_tram", "ten_day_du", "quan_huyen", "loai_dv", "nhom",
            *PY, "share60_src", f"share60_{B}", f"pop60_{B}", "elev_m", "slope_deg"]
ACC_COLS = ["cell_id", "ma_tram", f"pop_{B}", f"pop60_{B}", "dist_m", "tram_gan_nhat", "dist_m_chi_osm",
            "net_dist_m", "net_time_min", "net_station", "snap_cell_m", "detour"]
ROUND = {"lon": 6, "lat": 6, **{c: 6 for c in PY}, f"share60_{B}": 6,
         f"pop60_{B}": 6, "elev_m": 1, "slope_deg": 2, "dist_m": 1, "dist_m_chi_osm": 1,
         "net_dist_m": 1, "net_time_min": 2, "snap_cell_m": 1, "detour": 3}

WP = "WorldPop Global2 R2025A 100 m constrained (CC BY 4.0)"
OSM = "OpenStreetMap qua Overpass (ODbL 1.0)"
F1, F2 = "dan_cu_luoi_100m.csv", "tiep_can_luoi_100m.csv"
# (file, cot, kieu, don_vi, y_nghia, nguon, gia_dinh)
EXPLAIN = [
    (F1, "(moi dong)", "", "", "Mot o 100 m x 100 m cua raster WorldPop co dan > 0 va co tam o nam trong 1 trong 56 xa/phuong. "
     "O khong dan va o ngoai 56 xa da bi loai (xem logs/03_grid_*.log)", WP, "A2"),
    (F1, "cell_id", "chuoi", "", "Ma o = hang/cot cua o trong cua so raster cat theo hop bao Da Nang cu; khoa noi 2 file", WP, ""),
    (F1, "lon, lat", "so thuc", "do (WGS84)", "Toa do TAM o", WP, ""),
    (F1, "ma_tram", "chuoi", "", "Ma xa/phuong (= ma tram TYT01-TYT56) chua tam o", "Ranh gioi 56 xa (02_boundaries.py)", "A2, A12"),
    (F1, "ten_day_du, quan_huyen, loai_dv", "chuoi", "", "Ten xa/phuong, quan/huyen, loai don vi truoc 01/01/2025", "data/reference/danh_sach_56_tram.csv", ""),
    (F1, "nhom", "chuoi", "", "Phuong / Xa dong bang-ven do / Xa mien nui (phan loai theo dia hinh)", "06_kpi.py", "A7"),
    (F1, ", ".join(PY), "so thuc", "nguoi/o", "Gia tri pixel cua file vnm_pop_<nam>_CN_100m_R2025A_v1.tif = so nguoi UOC TINH trong o. "
     "La so thuc (mo hinh phan bo dan), khong phai dem dau nguoi; cong cac o cua mot xa ra dan so uoc tinh cua xa", WP, "A2"),
    (F1, "share60_src", "chuoi", "", "Nguon cua ty le 60+ (o 1 km chua o, hoac trung binh xa neu o 1 km = 0)", "WorldPop agesex 1 km", "A3"),
    (F1, f"share60_{B}", "so thuc", "0-1", "Ty le dan tu 60 tuoi tro len, lay tu o 1 km chua o", "WorldPop agesex 1 km (CC BY 4.0)", "A3"),
    (F1, f"pop60_{B}", "so thuc", "nguoi/o", f"Dan 60+ uoc tinh = pop_{B} x share60_{B}", "Tinh tu WorldPop", "A3"),
    (F1, "elev_m, slope_deg", "so thuc", "m, do", "Do cao va do doc tai tam o", "AWS Terrain Tiles (Mapzen) z11", ""),
    (F2, "(moi dong)", "", "", "Cung tap o voi dan_cu_luoi_100m.csv (noi bang cell_id). Khoang cach do tu tam o toi tram", "", ""),
    (F2, "ma_tram", "chuoi", "", "Xa/phuong chua o (khong phai tram gan nhat)", "", ""),
    (F2, f"pop_{B}, pop60_{B}", "so thuc", "nguoi/o", "Lap lai tu file dan cu de tinh ty le dan theo nguong khoang cach", WP, "A2, A3"),
    (F2, "dist_m", "so thuc", "m", "Khoang cach DUONG CHIM BAY tu tam o toi tram gan nhat trong 56 tram", "Tinh tu WorldPop + toa do tram", "A1, A5"),
    (F2, "tram_gan_nhat", "chuoi", "", "Ma tram gan nhat theo duong chim bay", "", "A5"),
    (F2, "dist_m_chi_osm", "so thuc", "m", "Nhu dist_m nhung chi dung cac tram co toa do thuc (nhap tay hoac OSM), bo tram uoc luong", "location-tyt.xlsx + " + OSM, "A5, A13"),
    (F2, "net_dist_m", "so thuc", "m", "Khoang cach DUONG DI NGAN NHAT theo mang duong OSM (Dijkstra) tu o toi tram gan nhat, "
     "gom ca doan tu tam o ra duong", OSM, "A6"),
    (F2, "net_time_min", "so thuc", "phut", "Thoi gian di xe may theo toc do gia dinh tung loai duong + di bo doan ra duong", OSM, "A6"),
    (F2, "net_station", "chuoi", "", "Ma tram gan nhat theo mang duong (co the khac tram_gan_nhat)", OSM, "A6"),
    (F2, "snap_cell_m", "so thuc", "m", "Khoang cach tu tam o toi nut duong gan nhat", OSM, "A6"),
    (F2, "detour", "so thuc", "lan", "He so vong = net_dist_m / max(dist_m, 50); > 1,5 nghia la duong di vong nhieu", "", "A6, A8"),
]


def main():
    g = pd.read_parquet(INTERIM / "grid_access.parquet")
    miss = [c for c in POP_COLS + ACC_COLS if c not in g.columns]
    assert not miss, f"grid_access.parquet thieu cot: {miss}"
    g = g.sort_values("cell_id").round(ROUND)
    log.info("Doc grid_access.parquet: %d o, tong dan %d = %.0f nguoi", len(g), BASE_YEAR, g[f"pop_{BASE_YEAR}"].sum())

    save_csv(g[POP_COLS], TAB / F1, S, note="luoi dan cu 100 m tu raster WorldPop (A2, A3)")
    save_csv(g[ACC_COLS], TAB / F2, S, note="khoang cach chim bay + mang duong moi o (A1, A5, A6)")
    ex = pd.DataFrame(EXPLAIN, columns=["tap_du_lieu", "ten_truong", "kieu", "don_vi", "y_nghia", "nguon", "gia_dinh"])
    save_csv(ex, TAB / "giai_thich_csv_luoi.csv", S, note="giai thich cot cua 2 file luoi CSV")
    log.info("Da ghi %s (%d dong), %s (%d dong), giai_thich_csv_luoi.csv (%d dong)", F1, len(g), F2, len(g), len(ex))


if __name__ == "__main__":
    main()
