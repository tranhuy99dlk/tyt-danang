# -*- coding: utf-8 -*-
"""04 - Toa do 56 tram y te: toa do nguoi dung nhap tay, doi chieu voi OSM.

Uu tien: toa do NHAP TAY trong data/raw/location-tyt.xlsx (neu hop le, gia dinh A13)
         > OSM moc 31/12/2024 nam trong ranh gioi xa > OSM hien tai trong ranh gioi
         > OSM ngoai ranh gioi (danh dau nghi sai toa do) > khong tim thay.
Toa do nhap tay bi LOAI khi nam ngoai hop bao Da Nang cu (nghi go nham) -> ghi "CAN XEM" trong
qa_stations.csv va quay ve OSM / uoc luong; khong tu sua so cua nguoi dung.
Tram khong tim thay toa do: dung tam dan cu (trong so dan 2025) cua xa lam VI TRI UOC LUONG
(gia dinh A5, danh dau ro, co phan tich do nhay bo cac tram uoc luong).
Dau ra: data/interim/stations_56.csv, outputs/tables/qa_stations.csv,
        outputs/tables/osm_health_facilities.csv
"""
import json
import re

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point, shape
from shapely.ops import transform as shp_transform

from common import (RAW, REF, INTERIM, TAB, CRS_M, BASE_YEAR, BBOX, norm_name, strip_station_name,
                    get_logger, save_csv)

S = "04_stations"
log = get_logger(S)
to_m = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True)
RE_TYT = re.compile(r"tr[aạ]m\s+y\s+t[eế]|^tyt\b", re.I)
MANUAL = RAW / "location-tyt.xlsx"       # cot 1: quan/huyen, cot 2: ten xa/phuong, cot 3: "lat, lon"
SRC_MANUAL = "NHAP TAY (data/raw/location-tyt.xlsx)"
RE_LATLON = re.compile(r"^\s*(-?\d+(?:[.]\d+)?)\s*[,;]\s*(-?\d+(?:[.]\d+)?)\s*$")


def load_manual(ref):
    """Doc toa do nhap tay. Tra ve (dict ma_tram -> ban ghi, danh sach dong khong dung duoc)."""
    if not MANUAL.exists():
        log.info("Khong co %s -> chi dung OSM / uoc luong", MANUAL.name)
        return {}, []
    d = pd.read_excel(MANUAL, header=None, dtype=str).iloc[:, :3]
    keyset = dict(zip(ref.ten_xa_phuong.map(norm_name), ref.ma_tram))
    dist_of = dict(zip(ref.ma_tram, ref.quan_huyen.map(norm_name)))
    W, S_, E, N = BBOX
    ok, bad, dist = {}, [], None
    for i, (c0, c1, c2) in enumerate(d.itertuples(index=False), start=1):
        if isinstance(c0, str) and re.match(r"^\s*(quận|huyện)\s", c0, re.I):
            dist = c0.strip()
        if not isinstance(c1, str) or not isinstance(c2, str):
            continue
        m = RE_LATLON.match(c2)
        if not m:                      # dong tieu de hoac o toa do khong doc duoc
            if keyset.get(norm_name(c1)):
                bad.append(dict(dong_excel=i, ten=c1.strip(), ma_tram=keyset.get(norm_name(c1)), ly_do=f"khong doc duoc toa do '{c2}'"))
            continue
        lat, lon = float(m.group(1)), float(m.group(2))
        ma = keyset.get(norm_name(c1))
        rec = dict(dong_excel=i, ten=c1.strip(), ma_tram=ma, lat=lat, lon=lon)
        if ma is None:
            bad.append(dict(rec, ly_do="ten khong khop danh sach 56 tram"))
        elif ma in ok:
            bad.append(dict(rec, ly_do="trung ten voi dong truoc"))
        elif not (W <= lon <= E and S_ <= lat <= N):
            bad.append(dict(rec, ly_do=f"ngoai hop bao Da Nang cu (lat={lat:.5f}, lon={lon:.5f})"))
        else:
            if dist and norm_name(dist) != dist_of[ma]:
                log.warning("CAN XEM dong %d: %s nam duoi '%s' trong file nhap tay, danh sach goc ghi khac", i, c1.strip(), dist)
            ok[ma] = rec
    log.info("Toa do nhap tay (%s): %d dong hop le, %d dong bi loai", MANUAL.name, len(ok), len(bad))
    for b in bad:
        log.warning("CAN XEM toa do nhap tay bi loai - dong %s, %s: %s", b["dong_excel"], b["ten"], b["ly_do"])
    return ok, bad


def load(snapshot):
    p = RAW / "osm" / f"stations_{snapshot}.json"
    if not p.exists():
        log.warning("Khong co %s", p)
        return pd.DataFrame()
    d = json.load(open(p, encoding="utf-8"))
    rows = []
    for e in d["elements"]:
        t = e.get("tags", {})
        lat = e.get("lat", e.get("center", {}).get("lat"))
        lon = e.get("lon", e.get("center", {}).get("lon"))
        if lat is None:
            continue
        rows.append(dict(snapshot=snapshot, osm=f"{e['type']}/{e['id']}", name=t.get("name", ""),
                         amenity=t.get("amenity", ""), healthcare=t.get("healthcare", ""),
                         addr=" ".join(t.get(k, "") for k in ("addr:subdistrict", "addr:ward", "addr:district")).strip(),
                         lon=lon, lat=lat))
    df = pd.DataFrame(rows)
    log.info("OSM %s: %d co so y te/ten tram (moc du lieu %s)", snapshot, len(df), d.get("osm3s", {}).get("timestamp_osm_base"))
    return df


def main():
    ref = pd.read_csv(REF / "danh_sach_56_tram.csv", encoding="utf-8-sig")
    feats = json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]
    poly = {f["properties"]["ma_tram"]: shape(f["geometry"]) for f in feats}
    grid = pd.read_parquet(INTERIM / "grid_100m.parquet")

    osm = pd.concat([load("2024-12-31"), load("current")], ignore_index=True)
    save_csv(osm, TAB / "osm_health_facilities.csv", S, note="moi co so y te OSM trong hop bao (2 moc)")
    tyt = osm[osm.name.str.contains(RE_TYT, na=False)].copy()
    tyt["k"] = tyt.name.map(strip_station_name)
    log.info("Doi tuong co ten 'Tram y te': %d (2 moc)", len(tyt))

    ref["k"] = ref.ten_xa_phuong.map(norm_name)
    keyset = dict(zip(ref.k, ref.ma_tram))
    tyt["ma_tram"] = tyt.k.map(keyset)
    # thu ghep theo dia chi neu ten khong ra
    nm = tyt.ma_tram.isna()
    tyt.loc[nm, "ma_tram"] = tyt.loc[nm, "addr"].map(lambda a: keyset.get(norm_name(a.split(" ")[0])) if a else None)

    manual, manual_bad = load_manual(ref)
    bad_of = {b["ma_tram"]: b["ly_do"] for b in manual_bad if b.get("ma_tram")}

    out, qa = [], []
    for _, r in ref.iterrows():
        c = tyt[tyt.ma_tram == r.ma_tram].copy()
        pg = poly.get(r.ma_tram)
        mn = manual.get(r.ma_tram)
        if mn is not None:
            pm = to_m.transform
            mx, my = pm(mn["lon"], mn["lat"])
            inside = bool(pg.contains(Point(mn["lon"], mn["lat"]))) if pg is not None else None
            dout = 0.0 if (inside or pg is None) else Point(mx, my).distance(shp_transform(pm, pg))
            kc_osm = np.nan                       # doi chieu: ung vien OSM gan nhat cua cung tram
            if len(c):
                cx, cy = pm(c.lon.values, c.lat.values)
                kc_osm = float(np.hypot(cx - mx, cy - my).min())
            out.append(dict(ma_tram=r.ma_tram, ten_tram=r.ten_tram, quan_huyen=r.quan_huyen, loai_dv=r.loai_dv,
                            lon=mn["lon"], lat=mn["lat"], nguon_toa_do=SRC_MANUAL, osm_id="", ten_osm="",
                            trong_ranh_gioi_xa=inside if inside is not None else True,
                            kc_ngoai_ranh_gioi_m=round(float(dout), 1), uoc_luong=False))
            qa.append(dict(ma_tram=r.ma_tram, ten=r.ten_day_du, so_ung_vien_osm=len(c), ung_vien_cach_xa_300m=0,
                           trong_ranh_gioi=inside, nguon=SRC_MANUAL,
                           kc_nhap_tay_vs_osm_m=round(kc_osm, 0) if kc_osm == kc_osm else np.nan, ghi_chu_nhap_tay=""))
        elif len(c) and pg is not None:
            c["inside"] = [pg.contains(Point(x, y)) for x, y in zip(c.lon, c.lat)]
            pm = to_m.transform
            pg_m = shp_transform(pm, pg)
            c["dist_m"] = [0.0 if ins else Point(*pm(x, y)).distance(pg_m)
                           for x, y, ins in zip(c.lon, c.lat, c.inside)]
            c["rank"] = (~c.inside).astype(int) * 2 + (c.snapshot != "2024-12-31").astype(int)
            c = c.sort_values(["rank"])
            best = c.iloc[0]
            # trung lap: cac ung vien khac cach > 300 m so voi diem duoc chon
            bx, by = to_m.transform(best.lon, best.lat)
            cx, cy = to_m.transform(c.lon.values, c.lat.values)
            far = np.hypot(cx - bx, cy - by)
            n_dup_far = int((far > 300).sum())
            src = f"OSM {best.snapshot}"
            out.append(dict(ma_tram=r.ma_tram, ten_tram=r.ten_tram, quan_huyen=r.quan_huyen, loai_dv=r.loai_dv,
                            lon=best.lon, lat=best.lat, nguon_toa_do=src, osm_id=best.osm, ten_osm=best["name"],
                            trong_ranh_gioi_xa=bool(best.inside), kc_ngoai_ranh_gioi_m=round(float(best.dist_m), 1),
                            uoc_luong=False))
            qa.append(dict(ma_tram=r.ma_tram, ten=r.ten_day_du, so_ung_vien_osm=len(c), ung_vien_cach_xa_300m=n_dup_far,
                           trong_ranh_gioi=bool(best.inside), nguon=src, kc_nhap_tay_vs_osm_m=np.nan,
                           ghi_chu_nhap_tay=("CAN XEM: nhap tay bi loai - " + bad_of[r.ma_tram]) if r.ma_tram in bad_of else ""))
        else:
            gx = grid[grid.ma_tram == r.ma_tram]
            if len(gx) and gx[f"pop_{BASE_YEAR}"].sum() > 0:
                w = gx[f"pop_{BASE_YEAR}"]
                lon, lat = np.average(gx.lon, weights=w), np.average(gx.lat, weights=w)
            elif pg is not None:
                lon, lat = pg.representative_point().x, pg.representative_point().y
            else:
                lon = lat = np.nan
            out.append(dict(ma_tram=r.ma_tram, ten_tram=r.ten_tram, quan_huyen=r.quan_huyen, loai_dv=r.loai_dv,
                            lon=lon, lat=lat, nguon_toa_do="UOC LUONG: tam dan cu xa (A5)", osm_id="", ten_osm="",
                            trong_ranh_gioi_xa=True, kc_ngoai_ranh_gioi_m=0.0, uoc_luong=True))
            qa.append(dict(ma_tram=r.ma_tram, ten=r.ten_day_du, so_ung_vien_osm=0, ung_vien_cach_xa_300m=0,
                           trong_ranh_gioi=None, nguon="khong tim thay tren OSM", kc_nhap_tay_vs_osm_m=np.nan,
                           ghi_chu_nhap_tay=("CAN XEM: nhap tay bi loai - " + bad_of[r.ma_tram]) if r.ma_tram in bad_of else ""))
    st = pd.DataFrame(out)
    st["x_m"], st["y_m"] = to_m.transform(st.lon.values, st.lat.values)
    # khoang cach tram -> tam dan cu cua xa (chi bao "dat lech")
    cen = grid.groupby("ma_tram").apply(lambda d: pd.Series({
        "cx": np.average(d.x_m, weights=d[f"pop_{BASE_YEAR}"] + 1e-9),
        "cy": np.average(d.y_m, weights=d[f"pop_{BASE_YEAR}"] + 1e-9)}))
    st = st.join(cen, on="ma_tram")
    st["kc_tram_tam_dan_cu_m"] = np.hypot(st.x_m - st.cx, st.y_m - st.cy).round(0)
    st = st.drop(columns=["cx", "cy"])
    # trung toa do giua cac tram
    from scipy.spatial import cKDTree
    t = cKDTree(st[["x_m", "y_m"]].values)
    pairs = t.query_pairs(50)
    log.info("Nguon toa do: nhap tay %d, OSM %d, uoc luong (A5) %d; ngoai ranh gioi xa: %d; cap tram trung toa do (<50 m): %d",
             (st.nguon_toa_do == SRC_MANUAL).sum(), st.nguon_toa_do.str.startswith("OSM").sum(), st.uoc_luong.sum(),
             (~st.trong_ranh_gioi_xa).sum(), len(pairs))
    save_csv(st, INTERIM / "stations_56.csv", S)
    q = pd.DataFrame(qa)
    q["cap_trung_toa_do_50m"] = 0
    for i, j in pairs:
        q.loc[[i, j], "cap_trung_toa_do_50m"] = 1
    save_csv(q, TAB / "qa_stations.csv", S)
    # luon ghi (ke ca rong) de file khong giu noi dung cu cua lan chay truoc
    save_csv(pd.DataFrame(manual_bad, columns=["dong_excel", "ten", "ma_tram", "lat", "lon", "ly_do"]),
             TAB / "qa_stations_nhap_tay_bi_loai.csv", S, note="dong trong location-tyt.xlsx khong dung duoc (CAN XEM)")
    unmatched = tyt[tyt.ma_tram.isna()][["snapshot", "osm", "name", "lon", "lat"]]
    save_csv(unmatched, TAB / "osm_tyt_khong_ghep_duoc.csv", S, note="tram OSM khong ghep vao 56 xa (thuong o Quang Nam/Hue)")


if __name__ == "__main__":
    main()
