# -*- coding: utf-8 -*-
"""02 - Ranh gioi 56 xa/phuong Da Nang cu (moc 31/12/2024, truoc NQ 1251).

Nguon chinh: OSM (Overpass, truy van theo ngay 2024-12-31) - ODbL.
Doi chieu/du phong: GADM 4.1 cap 3 (chi dung hoc thuat).
Dau ra: data/interim/communes_56.geojson, outputs/tables/qa_boundaries.csv
"""
import json
import zipfile

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely import make_valid
from shapely.geometry import LineString, Polygon, MultiPolygon, mapping, shape
from shapely.ops import linemerge, polygonize, unary_union, transform

from common import (RAW, REF, INTERIM, TAB, DISTRICTS, CRS_M, norm_name, get_logger,
                    save_csv, register)

S = "02_boundaries"
log = get_logger(S)
to_m = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True).transform


def key(s):
    return norm_name(s).replace(" ", "")


def rel_to_geom(rel):
    outers, inners = [], []
    for m in rel.get("members", []):
        if m.get("type") != "way" or "geometry" not in m:
            continue
        coords = [(p["lon"], p["lat"]) for p in m["geometry"] if p]
        if len(coords) < 2:
            continue
        (inners if m.get("role") == "inner" else outers).append(LineString(coords))
    if not outers:
        return None
    polys = list(polygonize(linemerge(outers)))
    if not polys:
        return None
    g = unary_union(polys)
    if inners:
        holes = list(polygonize(linemerge(inners)))
        if holes:
            g = g.difference(unary_union(holes))
    return make_valid(g)


def load_osm():
    p = RAW / "osm" / "admin_2024-12-31.json"
    if not p.exists():
        log.warning("Khong co %s", p)
        return None, None
    data = json.load(open(p, encoding="utf-8"))
    rels = [e for e in data["elements"] if e["type"] == "relation"]
    log.info("OSM admin: %d relation (timestamp %s)", len(rels), data.get("osm3s", {}).get("timestamp_osm_base"))
    rows = []
    for r in rels:
        t = r.get("tags", {})
        g = rel_to_geom(r)
        if g is None or g.is_empty:
            continue
        rows.append(dict(osm_id=r["id"], admin_level=t.get("admin_level"), name=t.get("name", ""), geom=g))
    df = pd.DataFrame(rows)
    dist = df[df.admin_level == "6"].copy()
    dist["k"] = dist["name"].map(key)
    wanted = {key(d): d for d in DISTRICTS}
    dist = dist[dist.k.isin(wanted)]
    log.info("OSM quan/huyen khop: %d/7 (%s)", len(dist), ", ".join(dist.name))
    comm = df[df.admin_level == "8"].copy()
    # gan quan/huyen theo diem dai dien
    def which_dist(g):
        pt = g.representative_point()
        for _, d in dist.iterrows():
            if d.geom.contains(pt):
                return wanted[d.k]
        return None
    comm["quan_huyen"] = comm.geom.map(which_dist)
    comm = comm[comm.quan_huyen.notna()].copy()
    return comm, dist


def load_gadm():
    p = RAW / "gadm" / "gadm41_VNM_3.json.zip"
    if not p.exists():
        log.warning("Khong co %s", p)
        return None
    with zipfile.ZipFile(p) as z:
        name = [n for n in z.namelist() if n.endswith(".json")][0]
        data = json.load(z.open(name))
    rows = []
    for f in data["features"]:
        pr = f["properties"]
        if key(pr.get("NAME_1", "")) != key("Đà Nẵng"):
            continue
        rows.append(dict(gid=pr.get("GID_3"), name=pr.get("NAME_3"), district=pr.get("NAME_2"),
                         type=pr.get("TYPE_3"), geom=make_valid(shape(f["geometry"]))))
    df = pd.DataFrame(rows)
    log.info("GADM Da Nang cap 3: %d don vi", len(df))
    return df


def match(ref, cand, cand_name_col, cand_dist_col, dist_key_fn):
    """Khop theo (ten xa da chuan hoa, quan/huyen)."""
    out = {}
    for _, r in ref.iterrows():
        k = key(r.ten_xa_phuong)
        dk = key(r.quan_huyen.replace("Quận ", "").replace("Huyện ", ""))
        c = cand[(cand[cand_name_col].map(key) == k)]
        if cand_dist_col and len(c) > 1:
            c = c[c[cand_dist_col].map(dist_key_fn) == dk]
        if len(c) >= 1:
            out[r.ma_tram] = c.iloc[0]
    return out


def main():
    ref = pd.read_csv(REF / "danh_sach_56_tram.csv", encoding="utf-8-sig")
    osm, _ = load_osm()
    gadm = load_gadm()
    m_osm = match(ref, osm, "name", "quan_huyen",
                  lambda s: key(str(s).replace("Quận ", "").replace("Huyện ", ""))) if osm is not None else {}
    m_gadm = match(ref, gadm, "name", "district", key) if gadm is not None else {}
    log.info("Khop 56 xa: OSM=%d, GADM=%d", len(m_osm), len(m_gadm))

    primary = "OSM" if len(m_osm) >= len(m_gadm) else "GADM"
    feats, qa = [], []
    for _, r in ref.iterrows():
        g_o = m_osm.get(r.ma_tram)
        g_g = m_gadm.get(r.ma_tram)
        go = g_o.geom if g_o is not None else None
        gg = g_g.geom if g_g is not None else None
        if primary == "OSM":
            g, src = (go, "OSM 2024-12-31") if go is not None else (gg, "GADM 4.1 (du phong)")
        else:
            g, src = (gg, "GADM 4.1") if gg is not None else (go, "OSM 2024-12-31 (du phong)")
        iou = np.nan
        if go is not None and gg is not None:
            a, b = transform(to_m, go), transform(to_m, gg)
            iou = a.intersection(b).area / a.union(b).area
        area = transform(to_m, g).area / 1e6 if g is not None else np.nan
        qa.append(dict(ma_tram=r.ma_tram, ten_xa_phuong=r.ten_day_du, quan_huyen=r.quan_huyen,
                       co_osm=go is not None, co_gadm=gg is not None, nguon_dung=src if g is not None else "THIEU",
                       dien_tich_km2=round(area, 3) if g is not None else np.nan,
                       iou_osm_gadm=round(iou, 3) if iou == iou else np.nan,
                       osm_id=(int(g_o.osm_id) if g_o is not None else None),
                       gadm_id=(g_g.gid if g_g is not None else None)))
        if g is not None:
            feats.append({"type": "Feature", "geometry": mapping(g),
                          "properties": dict(ma_tram=r.ma_tram, ten_xa_phuong=r.ten_xa_phuong,
                                             ten_day_du=r.ten_day_du, loai_dv=r.loai_dv,
                                             quan_huyen=r.quan_huyen, nguon_ranh_gioi=src,
                                             dien_tich_km2=round(area, 3))})
    qa = pd.DataFrame(qa)
    # chong lan / khe ho
    geoms_m = [transform(to_m, shape(f["geometry"])) for f in feats]
    sum_area = sum(g.area for g in geoms_m) / 1e6
    union_area = unary_union(geoms_m).area / 1e6 if geoms_m else 0
    log.info("Tong dien tich 56 xa: %.2f km2; dien tich hop: %.2f km2; chong lan: %.3f km2",
             sum_area, union_area, sum_area - union_area)
    log.info("Thieu ranh gioi: %d xa: %s", (qa.nguon_dung == "THIEU").sum(),
             ", ".join(qa[qa.nguon_dung == "THIEU"].ten_xa_phuong))
    if qa.iou_osm_gadm.notna().any():
        log.info("IoU OSM-GADM: trung vi %.3f, min %.3f", qa.iou_osm_gadm.median(), qa.iou_osm_gadm.min())
    out = INTERIM / "communes_56.geojson"
    json.dump({"type": "FeatureCollection", "features": feats,
               "meta": {"primary_source": primary, "sum_area_km2": sum_area, "union_area_km2": union_area}},
              open(out, "w", encoding="utf-8"), ensure_ascii=False)
    register(out, S, rows=len(feats), note=f"nguon chinh {primary}")
    save_csv(qa, TAB / "qa_boundaries.csv", S)


if __name__ == "__main__":
    main()
