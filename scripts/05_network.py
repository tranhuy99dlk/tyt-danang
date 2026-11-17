# -*- coding: utf-8 -*-
"""05 - Khoang cach/thoi gian theo mang duong OSM tu moi o luoi toi tram gan nhat.

Gia dinh A6: duong 2 chieu (xe may), toc do theo loai duong (common.SPEED_KMH),
doan tu tam o luoi/tram ra nut duong gan nhat tinh theo duong chim bay, di bo 5 km/h.
Dau ra: data/interim/grid_network.parquet, outputs/tables/qa_network.csv
"""
import json

import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree

from common import RAW, INTERIM, TAB, CRS_M, BASE_YEAR, SPEED_KMH, WALK_KMH, get_logger, save_csv, register

S = "05_network"
log = get_logger(S)


def main():
    p = RAW / "osm" / "roads_current.json"
    if not p.exists():
        log.warning("Khong co du lieu duong -> bo qua buoc mang duong")
        return
    d = json.load(open(p, encoding="utf-8"))
    ways = [e for e in d["elements"] if e["type"] == "way" and "geometry" in e]
    log.info("So doan duong OSM: %d (moc %s)", len(ways), d.get("osm3s", {}).get("timestamp_osm_base"))
    tr = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True)
    node_id, xs, ys = {}, [], []
    ei, ej, el, et = [], [], [], []
    by_class = {}
    for w in ways:
        hw = w.get("tags", {}).get("highway", "road")
        v = SPEED_KMH.get(hw.replace("_link", ""), 20) / 3.6  # m/s
        geom = w["geometry"]
        ids = w.get("nodes") or [f"{round(g['lon'], 7)},{round(g['lat'], 7)}" for g in geom]
        lon = np.array([g["lon"] for g in geom])
        lat = np.array([g["lat"] for g in geom])
        x, y = tr.transform(lon, lat)
        idx = []
        for k, (nid, xx, yy) in enumerate(zip(ids, x, y)):
            if nid not in node_id:
                node_id[nid] = len(xs)
                xs.append(xx)
                ys.append(yy)
            idx.append(node_id[nid])
        seg = np.hypot(np.diff(x), np.diff(y))
        ei += idx[:-1]
        ej += idx[1:]
        el += list(seg)
        et += list(seg / v / 60.0)  # phut
        by_class[hw] = by_class.get(hw, 0) + seg.sum() / 1000
    n = len(xs)
    xs, ys = np.array(xs), np.array(ys)
    ei, ej, el, et = map(np.array, (ei, ej, el, et))
    log.info("Do thi: %d nut, %d canh, tong %.0f km", n, len(ei), el.sum() / 1000)
    A = coo_matrix((np.ones(len(ei)), (ei, ej)), shape=(n, n)).tocsr()
    ncomp, lab = connected_components(A, directed=False)
    big = np.bincount(lab).argmax()
    keep = lab == big
    log.info("Thanh phan lien thong lon nhat: %d/%d nut (%.1f%%)", keep.sum(), n, 100 * keep.mean())
    remap = -np.ones(n, int)
    remap[keep] = np.arange(keep.sum())
    m = keep[ei] & keep[ej]
    ei2, ej2, el2, et2 = remap[ei[m]], remap[ej[m]], el[m], et[m]
    # bo canh trung (2 way chung doan) va canh do dai 0, tranh viec csr cong don trong so
    e = pd.DataFrame({"a": np.minimum(ei2, ej2), "b": np.maximum(ei2, ej2), "l": el2, "t": et2})
    e = e[e.a != e.b].groupby(["a", "b"], as_index=False).min()
    ei2, ej2 = e.a.values, e.b.values
    el2, et2 = e.l.values + 1e-6, e.t.values + 1e-9
    xk, yk = xs[keep], ys[keep]
    nk = len(xk)
    tree = cKDTree(np.c_[xk, yk])
    np.savez_compressed(INTERIM / "road_graph.npz", x=xk, y=yk, i=ei2, j=ej2, l=el2, t=et2)
    register(INTERIM / "road_graph.npz", S, rows=len(ei2), note="do thi duong (thanh phan lien thong lon nhat)")

    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    grid = pd.read_parquet(INTERIM / "grid_100m.parquet")
    ds, ns = tree.query(st[["x_m", "y_m"]].values)
    dc, nc = tree.query(grid[["x_m", "y_m"]].values)
    log.info("Tram cach duong gan nhat: trung vi %.0f m, max %.0f m", np.median(ds), ds.max())
    log.info("O luoi cach duong gan nhat: trung vi %.0f m, p95 %.0f m, max %.0f m", np.median(dc), np.percentile(dc, 95), dc.max())

    res = {}
    for wname, wvec, off_s in (("dist", el2, ds), ("time", et2, ds / (WALK_KMH / 3.6) / 60)):
        S_ = len(st)
        rows = np.r_[ei2, ej2, nk + np.arange(S_), ns]
        cols = np.r_[ej2, ei2, ns, nk + np.arange(S_)]
        vals = np.r_[wvec, wvec, off_s + 1e-6, off_s + 1e-6]
        G = coo_matrix((vals, (rows, cols)), shape=(nk + S_, nk + S_)).tocsr()
        dist, _, src = dijkstra(G, directed=False, indices=nk + np.arange(S_), min_only=True, return_predecessors=True)
        res[wname] = (dist[:nk], src[:nk] - nk)
    off_c_d = dc
    off_c_t = dc / (WALK_KMH / 3.6) / 60
    out = pd.DataFrame({"cell_id": grid.cell_id,
                        "net_dist_m": res["dist"][0][nc] + off_c_d,
                        "net_time_min": res["time"][0][nc] + off_c_t,
                        "net_station_idx": res["dist"][1][nc],
                        "snap_cell_m": dc})
    out["net_station"] = st.ma_tram.values[out.net_station_idx.clip(0, len(st) - 1)]
    out = out.drop(columns="net_station_idx")
    log.info("Khoang cach mang duong: trung vi o %.0f m; o khong toi duoc (inf): %d", np.median(out.net_dist_m[np.isfinite(out.net_dist_m)]), (~np.isfinite(out.net_dist_m)).sum())
    op = INTERIM / "grid_network.parquet"
    out.to_parquet(op, index=False)
    register(op, S, rows=len(out))
    qa = [dict(kiem_tra="So doan duong OSM", gia_tri=len(ways)),
          dict(kiem_tra="So nut / canh do thi", gia_tri=f"{n} / {len(ei)}"),
          dict(kiem_tra="Tong chieu dai duong (km)", gia_tri=round(el.sum() / 1000, 1)),
          dict(kiem_tra="Ty le nut thuoc thanh phan lien thong lon nhat (%)", gia_tri=round(100 * keep.mean(), 2)),
          dict(kiem_tra="Tram cach nut duong gan nhat > 200 m", gia_tri=int((ds > 200).sum())),
          dict(kiem_tra="O luoi cach duong > 500 m (so o)", gia_tri=int((dc > 500).sum())),
          dict(kiem_tra=f"O luoi cach duong > 500 m (dan {BASE_YEAR})", gia_tri=round(float(grid.loc[dc > 500, f"pop_{BASE_YEAR}"].sum())))]
    for k, v in sorted(by_class.items(), key=lambda kv: -kv[1]):
        qa.append(dict(kiem_tra=f"Chieu dai duong loai {k} (km)", gia_tri=round(v, 1)))
    save_csv(pd.DataFrame(qa), TAB / "qa_network.csv", S)


if __name__ == "__main__":
    main()
