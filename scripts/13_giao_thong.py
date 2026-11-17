# -*- coding: utf-8 -*-
"""13 - Minh chung giao thong: tung tram hien huu va tung diem de xuat nam canh duong nao, cap duong gi,
vung phuc vu cua tram di lai co thuan tien khong.

Khong tinh lai KPI: chi doc mang duong OSM (data/raw/osm/roads_current.json), do thi duong va ket qua
duong di ngan nhat cua 05_network.py, vung phuc vu cua 06_kpi.py, diem de xuat cua 09_greedy.py.
Dau ra (outputs/tables/):
  tram_giao_thong.csv         1 dong / tram: duong gan nhat (ten, cap, khoang cach), duong cap cao nhat trong 300 m,
                              va chi so di lai cua vung phuc vu theo mang duong
  diem_de_xuat_giao_thong.csv 1 dong / diem de xuat: nhu tren + duong di toi tram hien huu gan nhat
Cap duong theo the "highway" cua OSM; ten duong la the "name" (rong neu OSM chua gan ten).
Chay sau 09_greedy.py:  cd scripts && python 13_giao_thong.py
"""
import json

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree
from shapely.strtree import STRtree

from common import RAW, INTERIM, TAB, CRS_M, BASE_YEAR, MAIN_KM, get_logger, save_csv, wmedian

S = "13_giao_thong"
log = get_logger(S)
P = f"pop_{BASE_YEAR}"
R_NEAR = 300.0       # ban kinh tim duong cap cao nhat quanh diem (m)
# thu hang cap duong: so nho = cap cao. motorway (cao toc) xep rieng vi xe may khong duoc di.
RANK = {"trunk": 1, "primary": 2, "secondary": 3, "tertiary": 4, "unclassified": 5, "residential": 6,
        "living_street": 7, "road": 7, "service": 8, "track": 9, "motorway": 10}
VI = {"trunk": "quốc lộ/đường trục", "primary": "đường chính cấp 1", "secondary": "đường chính cấp 2",
      "tertiary": "đường liên khu vực", "unclassified": "đường nhỏ nối khu dân cư", "residential": "đường khu dân cư",
      "living_street": "đường nội bộ", "road": "đường chưa phân loại", "service": "đường nội bộ/lối vào",
      "track": "đường mòn/đường đất", "motorway": "cao tốc (xe máy không đi)"}


def load_roads():
    d = json.load(open(RAW / "osm" / "roads_current.json", encoding="utf-8"))
    tr = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True)
    lines, cls, names = [], [], []
    for w in d["elements"]:
        if w["type"] != "way" or len(w.get("geometry", [])) < 2:
            continue
        x, y = tr.transform([g["lon"] for g in w["geometry"]], [g["lat"] for g in w["geometry"]])
        lines.append(shapely.LineString(np.c_[x, y]))
        cls.append(w.get("tags", {}).get("highway", "road").replace("_link", ""))
        names.append(w.get("tags", {}).get("name", ""))
    return np.array(lines), np.array(cls), np.array(names), d.get("osm3s", {}).get("timestamp_osm_base")


def road_info(x, y, lines, cls, names, tree):
    """Duong gan nhat va duong cap cao nhat (khong tinh cao toc) trong R_NEAR quanh tung diem."""
    pts = shapely.points(x, y)
    (ip, il), dist = tree.query_nearest(pts, return_distance=True, all_matches=False)
    o = np.argsort(ip)
    il, dist = il[o], dist[o]
    out = pd.DataFrame({"duong_gan_nhat": names[il], "cap_duong_gan_nhat": cls[il], "kc_toi_duong_gan_nhat_m": dist.round(1)})
    jp, jl = tree.query(pts, predicate="dwithin", distance=R_NEAR)
    w = pd.DataFrame({"p": jp, "cap": cls[jl], "ten": names[jl], "kc": shapely.distance(pts[jp], lines[jl])})
    w["rk"] = w.cap.map(RANK).fillna(8)
    w = w[w.cap != "motorway"].sort_values(["p", "rk", "kc"]).drop_duplicates("p").set_index("p")
    out["cap_cao_nhat_trong_300m"] = w.cap.reindex(range(len(out))).values
    out["duong_cap_cao_nhat_trong_300m"] = w.ten.reindex(range(len(out))).values
    out["kc_toi_duong_cap_cao_nhat_m"] = w.kc.reindex(range(len(out))).round(1).values
    out["mo_ta_cap_duong_gan_nhat"] = out.cap_duong_gan_nhat.map(VI)
    return out


def main():
    lines, cls, names, moc = load_roads()
    tree = STRtree(lines)
    log.info("Mang duong OSM: %d doan (moc %s); %.1f%% doan co ten", len(lines), moc, 100 * (names != "").mean())
    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    g = pd.read_parquet(INTERIM / "grid_access.parquet")

    # ---- tram hien huu ----
    t = pd.concat([st[["ma_tram", "ten_tram", "quan_huyen", "loai_dv", "lat", "lon", "kc_tram_tam_dan_cu_m"]],
                   road_info(st.x_m.values, st.y_m.values, lines, cls, names, tree)], axis=1)
    rows = []
    for ma, d in g.groupby("net_station"):          # vung phuc vu THEO MANG DUONG (tram gan nhat theo duong di)
        far = d[d.dist_m > 500]
        rows.append(dict(ma_tram=ma, dan_phuc_vu_theo_duong=round(d[P].sum()),
                         kc_duong_di_trung_vi_m=round(wmedian(d.net_dist_m, d[P])),
                         thoi_gian_trung_vi_phut=round(wmedian(d.net_time_min, d[P]), 1),
                         ty_le_dan_trong_3km_duong_di_pct=round(100 * d.loc[d.net_dist_m <= MAIN_KM * 1000, P].sum() / d[P].sum(), 2),
                         he_so_vong_trung_vi=round(wmedian(far.detour, far[P]), 3) if len(far) else np.nan,
                         dan_cach_tram_tren_5km_duong_di=round(d.loc[d.net_dist_m > 5000, P].sum())))
    t = t.merge(pd.DataFrame(rows), on="ma_tram", how="left")
    save_csv(t, TAB / "tram_giao_thong.csv", S, note="duong canh tram (OSM) + chi so di lai vung phuc vu (A6)")
    log.info("Tram: cap duong gan nhat %s", t.cap_duong_gan_nhat.value_counts().to_dict())
    log.info("Tram: cap duong cao nhat trong 300 m %s", t.cap_cao_nhat_trong_300m.value_counts().to_dict())
    log.info("Tram: kc toi duong gan nhat trung vi %.0f m, max %.0f m; tram co he so vong > 1,5: %d",
             t.kc_toi_duong_gan_nhat_m.median(), t.kc_toi_duong_gan_nhat_m.max(), int((t.he_so_vong_trung_vi > 1.5).sum()))

    # ---- diem de xuat (09_greedy.py) ----
    sp = TAB / "greedy_sites.csv"
    if not sp.exists():
        log.warning("Khong co greedy_sites.csv -> bo qua diem de xuat")
        return
    s = pd.read_csv(sp, encoding="utf-8-sig")
    e = pd.concat([s[["thu_tu", "xa", "lat", "lon", "dan_moi_duoc_phu", "ty_le_3km_sau_pct", "kc_toi_tram_hien_huu_m", "tram_hien_huu_gan_nhat"]],
                   road_info(s.x_m.values, s.y_m.values, lines, cls, names, tree)], axis=1)
    z = np.load(INTERIM / "road_graph.npz")           # do thi cua 05: duong di tu diem de xuat toi tram hien huu gan nhat
    nk, ntree = len(z["x"]), cKDTree(np.c_[z["x"], z["y"]])
    ds, ns = ntree.query(st[["x_m", "y_m"]].values)
    n_st = len(st)
    G = coo_matrix((np.r_[z["l"], z["l"], ds + 1e-6, ds + 1e-6],
                    (np.r_[z["i"], z["j"], nk + np.arange(n_st), ns], np.r_[z["j"], z["i"], ns, nk + np.arange(n_st)])),
                   shape=(nk + n_st, nk + n_st)).tocsr()
    dist, _, src = dijkstra(G, directed=False, indices=nk + np.arange(n_st), min_only=True, return_predecessors=True)
    de, ne = ntree.query(s[["x_m", "y_m"]].values)
    e["duong_di_toi_tram_hien_huu_gan_nhat_m"] = (dist[ne] + de).round(0)
    e["tram_gan_nhat_theo_duong_di"] = st.ten_tram.values[(src[ne] - nk).clip(0, n_st - 1)]
    e["he_so_vong_toi_tram_hien_huu"] = (e.duong_di_toi_tram_hien_huu_gan_nhat_m / e.kc_toi_tram_hien_huu_m).round(2)
    save_csv(e, TAB / "diem_de_xuat_giao_thong.csv", S, note="duong canh diem de xuat (OSM) + duong di toi tram hien huu")
    log.info("Diem de xuat: %d diem; cap duong gan nhat %s", len(e), e.cap_duong_gan_nhat.value_counts().to_dict())


if __name__ == "__main__":
    main()
