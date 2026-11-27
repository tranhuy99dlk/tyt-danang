# -*- coding: utf-8 -*-
"""1c – THU THẬP: BẢNG TRA CỨU KHÔNG GIAN (phần hình học Spark không làm được: điểm trong đa giác, độ cao, đồ thị đường).

Vì sao không làm trong Spark: Spark không có sẵn phép "điểm nằm trong đa giác", không đọc được ảnh độ cao
và không dựng được đồ thị đường từ OpenStreetMap. Script này làm ba việc đó MỘT LẦN, rồi nạp kết quả vào
Bronze như một nguồn dữ liệu riêng (source_system=tra_cuu_khong_gian). Spark vẫn kiểm tra các bảng này ở
Bước 2 · Kiểm tra & làm sạch như mọi dữ liệu thô khác.

  1. Ranh giới 56 xã/phường (mốc trước 01/01/2025): so khớp tên với GADM 4.1 và OSM, tự chọn nguồn khớp
     nhiều xã hơn (giả định A12) -> ranh_gioi_56_xa.geojson, outputs/tables/1_du_lieu_nguon/qa_boundaries.csv
  2. Mọi pixel 100 m trong hộp bao: toạ độ tâm, xã chứa tâm pixel (rỗng = ngoài 56 xã), ô 1 km chứa nó,
     độ cao, độ dốc, nút đường gần nhất -> o_luoi_100m.csv
  3. Đồ thị đường OSM (thành phần liên thông lớn nhất, giả định A6) -> duong_nut.csv, duong_canh.csv,
     outputs/tables/1_du_lieu_nguon/qa_network.csv

Chạy sau 1b_chuyen_doi_bronze.py:  python scripts/1_thu_thap/1c_tra_cuu_khong_gian.py
"""
import glob  # tìm file theo mẫu tên (các mảnh ảnh độ cao)
import json  # đọc/ghi JSON, GeoJSON
import shutil  # xoá thư mục khi nạp lại

import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu
import rasterio  # đọc raster GeoTIFF
import shapely  # thư viện hình học (điểm trong đa giác nhanh trên mảng)
from pyproj import Transformer  # đổi hệ toạ độ WGS84 -> UTM 49N
from rasterio.merge import merge  # ghép nhiều mảnh ảnh độ cao thành một
from rasterio.windows import from_bounds  # cắt cửa sổ raster theo hộp bao
from scipy.sparse import coo_matrix  # ma trận thưa để biểu diễn đồ thị
from scipy.sparse.csgraph import connected_components  # tìm thành phần liên thông của đồ thị
from scipy.spatial import cKDTree  # cây tìm láng giềng gần nhất
from shapely import make_valid  # sửa đa giác lỗi (tự cắt nhau)
from shapely.geometry import LineString, mapping, shape  # đối tượng đường, đổi sang/từ GeoJSON
from shapely.ops import linemerge, polygonize, unary_union  # ghép đường, tạo đa giác, hợp vùng

import sys  # danh sách thư mục Python tìm module
from pathlib import Path  # làm việc với đường dẫn
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "chung"))  # thêm scripts/chung/ để import được common.py, hinh_hoc.py
from common import (bang, RAW, REF, BRONZE, BBOX, BASE_YEAR, CRS_M, DISTRICTS, SPEED_KMH,  # tham số dùng chung
                    get_logger, register, save_csv, today_utc, norm_name, bronze_moi_nhat)  # hàm tiện ích

S = "1c_tra_cuu_khong_gian"  # tên bước
log = get_logger(S)  # bộ ghi log
NGAY = today_utc()  # ngày nạp
DICH = BRONZE / "source_system=tra_cuu_khong_gian" / f"ingestion_date={NGAY}"  # thư mục Bronze đích
BBOX1K = (BBOX[0] - .02, BBOX[1] - .02, BBOX[2] + .02, BBOX[3] + .02)  # hộp bao nới rộng cho lưới 1 km (giống 1b)
to_m = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True)  # bộ đổi toạ độ kinh/vĩ độ -> mét UTM 49N


def sang_met(g):  # đổi một hình học từ kinh/vĩ độ sang mét UTM 49N
    return shapely.transform(g, lambda xy: np.column_stack(to_m.transform(xy[:, 0], xy[:, 1])))  # áp phép đổi toạ độ cho mọi đỉnh


def key(s):  # khoá so khớp tên: tên chuẩn hoá, bỏ hết khoảng trắng
    return norm_name(s).replace(" ", "")  # ví dụ "Phường Hòa An" -> "hòaan"


def ghi_csv(df, ten, ghi_chu):  # ghi một bảng CSV vào thư mục Bronze tra cứu không gian
    p = DICH / f"{ten}.csv"  # đường dẫn đích
    df.to_csv(p, index=False, encoding="utf-8")  # ghi CSV UTF-8 không BOM
    register(p, S, rows=len(df), note=ghi_chu)  # đăng ký vào sổ
    log.info("Bronze tra_cuu_khong_gian %-16s %9d dong", ten, len(df))  # in tiến độ


# ======================= 1. RANH GIỚI 56 XÃ =======================
def quan_he_thanh_da_giac(rel):  # ghép các "way" của một quan hệ OSM thành đa giác (vòng ngoài trừ vòng trong)
    ngoai, trong = [], []  # danh sách đường vòng ngoài, vòng trong (lỗ)
    for m in rel.get("members", []):  # duyệt thành viên của quan hệ
        if m.get("type") != "way" or "geometry" not in m:  # chỉ lấy đoạn đường có toạ độ
            continue  # bỏ qua thành viên khác
        toa_do = [(p["lon"], p["lat"]) for p in m["geometry"] if p]  # danh sách (kinh độ, vĩ độ)
        if len(toa_do) >= 2:  # cần ít nhất 2 điểm mới thành đường
            (trong if m.get("role") == "inner" else ngoai).append(LineString(toa_do))  # xếp vào vòng trong hay ngoài
    if not ngoai:  # không có vòng ngoài
        return None  # không tạo được đa giác
    polys = list(polygonize(linemerge(ngoai)))  # nối các đoạn rồi khép thành đa giác
    if not polys:  # không khép được
        return None  # bỏ
    g = unary_union(polys)  # hợp các mảnh
    if trong:  # có lỗ
        lo = list(polygonize(linemerge(trong)))  # khép vòng trong thành đa giác lỗ
        if lo:  # nếu khép được
            g = g.difference(unary_union(lo))  # khoét lỗ
    return make_valid(g)  # sửa đa giác nếu lỗi


def doc_osm():  # đọc ranh giới OSM mốc 31/12/2024: trả về các xã (cấp 8) đã gán quận
    data = json.load(open(RAW / "osm" / "admin_2024-12-31.json", encoding="utf-8"))  # file truy vấn Overpass
    rows = []  # danh sách đơn vị
    for r in (e for e in data["elements"] if e["type"] == "relation"):  # duyệt các quan hệ hành chính
        g = quan_he_thanh_da_giac(r)  # dựng đa giác
        if g is not None and not g.is_empty:  # dựng được
            t = r.get("tags", {})  # thẻ OSM
            rows.append(dict(osm_id=r["id"], cap=t.get("admin_level"), ten=t.get("name", ""), geom=g))  # lưu đơn vị
    df = pd.DataFrame(rows)  # thành bảng
    muon = {key(d): d for d in DISTRICTS}  # 7 quận/huyện cần tìm
    quan = df[(df.cap == "6") & df.ten.map(key).isin(muon)]  # các quận/huyện (cấp 6) khớp tên
    xa = df[df.cap == "8"].copy()  # các xã/phường (cấp 8)

    def thuoc_quan(g):  # tìm quận chứa điểm đại diện của xã
        pt = g.representative_point()  # một điểm chắc chắn nằm trong xã
        for _, d in quan.iterrows():  # duyệt các quận
            if d.geom.contains(pt):  # quận chứa điểm
                return muon[key(d.ten)]  # trả tên quận chuẩn
        return None  # không thuộc quận nào trong 7 quận
    xa["quan_huyen"] = xa.geom.map(thuoc_quan)  # gán quận cho từng xã
    return xa[xa.quan_huyen.notna()]  # chỉ giữ xã thuộc Đà Nẵng cũ


def doc_gadm():  # đọc ranh giới GADM 4.1 của Đà Nẵng từ Bronze
    p = bronze_moi_nhat("ranh_gioi") / "gadm41_da_nang_cap3.geojson"  # file Bronze mới nhất
    feats = json.load(open(p, encoding="utf-8"))["features"]  # danh sách đơn vị
    return pd.DataFrame([dict(gid=f["properties"].get("GID_3"), ten=f["properties"].get("NAME_3"),  # mã và tên đơn vị
                              quan_huyen=f["properties"].get("NAME_2"), geom=make_valid(shape(f["geometry"])))  # quận và hình học
                         for f in feats])  # (tiếp) duyệt mọi đơn vị


def khop(ref, cand, ham_quan):  # so khớp 56 xã với một nguồn theo (tên xã, quận)
    out = {}  # mã trạm -> dòng nguồn khớp
    for _, r in ref.iterrows():  # duyệt 56 xã
        c = cand[cand.ten.map(key) == key(r.ten_xa_phuong)]  # các đơn vị cùng tên
        if len(c) > 1:  # trùng tên ở nhiều quận
            c = c[c.quan_huyen.map(ham_quan) == key(r.quan_huyen.replace("Quận ", "").replace("Huyện ", ""))]  # lọc thêm theo quận
        if len(c):  # có kết quả
            out[r.ma_tram] = c.iloc[0]  # lấy dòng đầu
    return out  # trả từ điển kết quả


def ranh_gioi(ref):  # chọn ranh giới cho 56 xã và ghi bảng QA
    osm, gadm = doc_osm(), doc_gadm()  # đọc hai nguồn
    m_osm = khop(ref, osm, lambda s: key(str(s).replace("Quận ", "").replace("Huyện ", "")))  # khớp với OSM
    m_gadm = khop(ref, gadm, key)  # khớp với GADM
    chinh = "OSM" if len(m_osm) >= len(m_gadm) else "GADM"  # nguồn khớp nhiều xã hơn là nguồn chính (A12)
    log.info("Khop 56 xa: OSM=%d, GADM=%d -> nguon chinh %s", len(m_osm), len(m_gadm), chinh)  # in kết quả
    feats, qa = [], []  # đặc trưng GeoJSON đầu ra; bảng QA
    for _, r in ref.iterrows():  # duyệt 56 xã theo thứ tự danh mục
        go = m_osm[r.ma_tram].geom if r.ma_tram in m_osm else None  # hình học OSM (nếu có)
        gg = m_gadm[r.ma_tram].geom if r.ma_tram in m_gadm else None  # hình học GADM (nếu có)
        if chinh == "OSM":  # nguồn chính OSM
            g, src = (go, "OSM 2024-12-31") if go is not None else (gg, "GADM 4.1 (du phong)")  # dùng OSM, thiếu thì GADM
        else:  # nguồn chính GADM
            g, src = (gg, "GADM 4.1") if gg is not None else (go, "OSM 2024-12-31 (du phong)")  # dùng GADM, thiếu thì OSM
        iou = np.nan  # độ trùng khớp hai nguồn (giao / hợp)
        if go is not None and gg is not None:  # có cả hai nguồn
            a, b = sang_met(go), sang_met(gg)  # đổi sang mét
            iou = a.intersection(b).area / a.union(b).area  # tỷ lệ diện tích giao trên hợp
        dt = sang_met(g).area / 1e6 if g is not None else np.nan  # diện tích km² trên UTM 49N
        qa.append(dict(ma_tram=r.ma_tram, ten_xa_phuong=r.ten_day_du, quan_huyen=r.quan_huyen,  # dòng QA của xã
                       co_osm=go is not None, co_gadm=gg is not None, nguon_dung=src if g is not None else "THIEU",  # (tiếp) nguồn
                       dien_tich_km2=round(dt, 3), iou_osm_gadm=round(iou, 3) if iou == iou else np.nan,  # (tiếp) diện tích, IoU
                       osm_id=int(m_osm[r.ma_tram].osm_id) if r.ma_tram in m_osm else None,  # (tiếp) mã OSM
                       gadm_id=m_gadm[r.ma_tram].gid if r.ma_tram in m_gadm else None))  # (tiếp) mã GADM
        if g is not None:  # có ranh giới
            feats.append({"type": "Feature", "geometry": mapping(g),  # đặc trưng GeoJSON
                          "properties": dict(ma_tram=r.ma_tram, ten_xa_phuong=r.ten_xa_phuong, ten_day_du=r.ten_day_du,  # thuộc tính
                                             loai_dv=r.loai_dv, quan_huyen=r.quan_huyen, nguon_ranh_gioi=src,  # (tiếp)
                                             dien_tich_km2=round(dt, 3))})  # (tiếp)
    qa = pd.DataFrame(qa)  # bảng QA
    log.info("Thieu ranh gioi: %d xa; IoU OSM-GADM trung vi %.3f", (qa.nguon_dung == "THIEU").sum(), qa.iou_osm_gadm.median())  # thống kê
    save_csv(qa, bang("qa_boundaries.csv"), S, note=f"nguon chinh {chinh} (A12)")  # ghi bảng QA
    p = DICH / "ranh_gioi_56_xa.geojson"  # file ranh giới đầu ra
    json.dump({"type": "FeatureCollection", "features": feats, "meta": {"nguon_chinh": chinh}},  # nội dung GeoJSON
              open(p, "w", encoding="utf-8"), ensure_ascii=False)  # ghi file
    register(p, S, rows=len(feats), note=f"ranh gioi 56 xa, nguon chinh {chinh}")  # đăng ký
    return feats  # trả danh sách đặc trưng cho phần 2


# ======================= 2. ĐỒ THỊ ĐƯỜNG =======================
def do_thi_duong():  # dựng đồ thị đường xe máy từ OSM (A6)
    d = json.load(open(RAW / "osm" / "roads_current.json", encoding="utf-8"))  # file truy vấn Overpass các tuyến đường
    ways = [e for e in d["elements"] if e["type"] == "way" and "geometry" in e]  # chỉ lấy đoạn đường có toạ độ
    nut, xs, ys = {}, [], []  # mã nút OSM -> chỉ số; toạ độ x, y của nút
    ei, ej, el, et = [], [], [], []  # cạnh: nút đầu, nút cuối, độ dài (m), thời gian (phút)
    theo_loai = {}  # tổng chiều dài theo loại đường (km)
    for w in ways:  # duyệt từng đoạn đường
        hw = w.get("tags", {}).get("highway", "road")  # loại đường OSM
        v = SPEED_KMH.get(hw.replace("_link", ""), 20) / 3.6  # tốc độ xe máy (m/giây); nhánh nối "_link" lấy như đường chính
        geom = w["geometry"]  # danh sách điểm của đoạn
        ids = w.get("nodes") or [f"{round(g['lon'], 7)},{round(g['lat'], 7)}" for g in geom]  # mã nút OSM (hoặc toạ độ làm mã)
        x, y = to_m.transform(np.array([g["lon"] for g in geom]), np.array([g["lat"] for g in geom]))  # đổi toạ độ sang mét
        idx = []  # chỉ số nút của đoạn
        for nid, xx, yy in zip(ids, x, y):  # duyệt từng điểm
            if nid not in nut:  # nút mới
                nut[nid] = len(xs)  # cấp chỉ số mới
                xs.append(xx)  # lưu x
                ys.append(yy)  # lưu y
            idx.append(nut[nid])  # ghi chỉ số nút
        seg = np.hypot(np.diff(x), np.diff(y))  # độ dài từng đoạn nhỏ giữa hai điểm liên tiếp
        ei += idx[:-1]  # nút đầu các cạnh
        ej += idx[1:]  # nút cuối các cạnh
        el += list(seg)  # độ dài các cạnh
        et += list(seg / v / 60.0)  # thời gian đi qua cạnh (phút)
        theo_loai[hw] = theo_loai.get(hw, 0) + seg.sum() / 1000  # cộng chiều dài theo loại
    n = len(xs)  # số nút
    xs, ys = np.array(xs), np.array(ys)  # đổi sang mảng
    ei, ej, el, et = map(np.array, (ei, ej, el, et))  # đổi sang mảng
    A = coo_matrix((np.ones(len(ei)), (ei, ej)), shape=(n, n)).tocsr()  # ma trận kề của đồ thị
    _, nhan = connected_components(A, directed=False)  # nhãn thành phần liên thông của từng nút
    giu = nhan == np.bincount(nhan).argmax()  # chỉ giữ thành phần lớn nhất (đảo nhỏ bị bỏ)
    log.info("Do thi: %d nut, %d canh, %.0f km; thanh phan lon nhat %.1f%% nut", n, len(ei), el.sum() / 1000, 100 * giu.mean())  # thống kê
    doi = -np.ones(n, int)  # bảng đổi chỉ số nút cũ -> mới
    doi[giu] = np.arange(giu.sum())  # nút được giữ đánh số lại 0..k-1
    m = giu[ei] & giu[ej]  # cạnh có cả hai đầu được giữ
    e = pd.DataFrame({"i": np.minimum(doi[ei[m]], doi[ej[m]]), "j": np.maximum(doi[ei[m]], doi[ej[m]]),  # cạnh vô hướng (đầu nhỏ trước)
                      "dai_m": el[m], "phut": et[m]})  # (tiếp) độ dài và thời gian
    e = e[e.i != e.j].groupby(["i", "j"], as_index=False).min()  # bỏ cạnh vòng về chính nó; cạnh trùng giữ bản ngắn nhất
    e["dai_m"] += 1e-6  # cộng số rất nhỏ để không có cạnh dài 0 (thuật toán ma trận thưa bỏ giá trị 0)
    e["phut"] += 1e-9  # tương tự cho thời gian
    nodes = pd.DataFrame({"nut": np.arange(giu.sum()), "x_m": xs[giu], "y_m": ys[giu]})  # bảng nút
    qa = [dict(kiem_tra="So doan duong OSM", gia_tri=len(ways)),  # bảng QA mạng đường
          dict(kiem_tra="So nut / canh do thi", gia_tri=f"{n} / {len(ei)}"),  # (tiếp)
          dict(kiem_tra="Tong chieu dai duong (km)", gia_tri=round(el.sum() / 1000, 1)),  # (tiếp)
          dict(kiem_tra="Ty le nut thuoc thanh phan lien thong lon nhat (%)", gia_tri=round(100 * giu.mean(), 2))]  # (tiếp)
    for k, v in sorted(theo_loai.items(), key=lambda kv: -kv[1]):  # chiều dài theo loại, dài nhất trước
        qa.append(dict(kiem_tra=f"Chieu dai duong loai {k} (km)", gia_tri=round(v, 1)))  # thêm dòng
    save_csv(pd.DataFrame(qa), bang("qa_network.csv"), S, note=f"moc OSM {d.get('osm3s', {}).get('timestamp_osm_base')} (A6)")  # ghi QA
    return nodes, e  # trả bảng nút và bảng cạnh


# ======================= 3. LƯỚI PIXEL 100 m =======================
def doc_luoi(path, bbox):  # trả về kích thước và phép biến đổi toạ độ của cửa sổ raster
    with rasterio.open(path) as ds:  # mở raster
        win = from_bounds(*bbox, transform=ds.transform).round_offsets().round_lengths()  # cửa sổ pixel
        return (int(win.height), int(win.width)), ds.window_transform(win)  # (số hàng, số cột), phép biến đổi


def main():  # hàm chính
    if DICH.exists():  # đã nạp hôm nay
        shutil.rmtree(DICH)  # xoá để nạp lại
    DICH.mkdir(parents=True)  # tạo thư mục đích
    ref = pd.read_csv(REF / "danh_sach_56_tram.csv", encoding="utf-8-sig")  # danh mục 56 trạm/xã
    feats = ranh_gioi(ref)  # phần 1: ranh giới
    nodes, edges = do_thi_duong()  # phần 2: đồ thị đường
    ghi_csv(nodes, "duong_nut", "nut do thi duong (UTM 49N)")  # ghi bảng nút
    ghi_csv(edges, "duong_canh", "canh do thi duong: do dai (m), thoi gian xe may (phut) (A6)")  # ghi bảng cạnh

    # --- phần 3: mọi pixel 100 m trong hộp bao ---
    (H, W), tr = doc_luoi(RAW / "worldpop" / "pop100m" / f"vnm_pop_{BASE_YEAR}_CN_100m_R2025A_v1.tif", BBOX)  # lưới 100 m chuẩn (giống 1b)
    rows, cols = np.indices((H, W))  # chỉ số hàng/cột mọi pixel
    d = pd.DataFrame({"row": rows.ravel(), "col": cols.ravel()})  # bảng pixel
    d["cell_id"] = [f"R{r:04d}C{c:04d}" for r, c in zip(d.row, d.col)]  # mã ô dạng R0123C0456
    d["lon"] = tr.c + (d.col + 0.5) * tr.a  # kinh độ tâm pixel = gốc + (cột + 0,5) x bề rộng pixel
    d["lat"] = tr.f + (d.row + 0.5) * tr.e  # vĩ độ tâm pixel (tr.e âm vì hàng tăng về phía nam)
    d["x_m"], d["y_m"] = to_m.transform(d.lon.values, d.lat.values)  # toạ độ mét UTM 49N
    d["ma_tram"] = None  # xã chứa tâm pixel, mặc định rỗng (= ngoài 56 xã)
    for f in feats:  # duyệt 56 xã theo thứ tự danh mục; xã đầu tiên chứa tâm pixel được nhận
        m = shapely.contains_xy(shape(f["geometry"]), d.lon.values, d.lat.values) & d.ma_tram.isna().values  # pixel nằm trong xã và chưa có chủ
        d.loc[m, "ma_tram"] = f["properties"]["ma_tram"]  # gán mã xã
    _, t1 = doc_luoi(RAW / "worldpop" / "pop1km" / f"vnm_pop_{BASE_YEAR}_CN_1km_R2025A_UA_v1.tif", BBOX1K)  # lưới 1 km (giống 1b)
    d["k1_row"] = np.floor((d.lat.values - t1.f) / t1.e).astype(int)  # hàng của ô 1 km chứa tâm pixel
    d["k1_col"] = np.floor((d.lon.values - t1.c) / t1.a).astype(int)  # cột của ô 1 km chứa tâm pixel
    tiles = sorted(glob.glob(str(RAW / "terrain" / "z11_*.tif")))  # các mảnh ảnh độ cao AWS Terrain Tiles
    srcs = [rasterio.open(t) for t in tiles]  # mở từng mảnh
    dem, dtr = merge(srcs)  # ghép thành một ảnh; dtr = phép biến đổi toạ độ của ảnh ghép
    crs = srcs[0].crs  # hệ toạ độ của ảnh (Web Mercator)
    for s_ in srcs:  # đóng các file
        s_.close()  # giải phóng
    dem = dem[0].astype(float)  # lấy kênh đầu, đổi sang số thực
    dem[dem < -100] = np.nan  # giá trị vô lý (dưới -100 m) coi là thiếu
    tx, ty = Transformer.from_crs("EPSG:4326", crs, always_xy=True).transform(d.lon.values, d.lat.values)  # toạ độ pixel trong hệ của ảnh độ cao
    rr = np.floor((ty - dtr.f) / dtr.e).astype(int).clip(0, dem.shape[0] - 1)  # hàng tương ứng trên ảnh độ cao
    cc = np.floor((tx - dtr.c) / dtr.a).astype(int).clip(0, dem.shape[1] - 1)  # cột tương ứng
    k = np.cos(np.deg2rad(16.05))  # hệ số co giãn Mercator tại vĩ độ Đà Nẵng
    dzdy, dzdx = np.gradient(dem, abs(dtr.e) * k, dtr.a * k)  # độ dốc theo hai hướng (m/m)
    d["elev_m"] = dem[rr, cc]  # độ cao tại tâm pixel
    d["slope_deg"] = np.degrees(np.arctan(np.hypot(dzdx, dzdy)))[rr, cc]  # độ dốc (độ)
    kc, nut = cKDTree(nodes[["x_m", "y_m"]].values).query(d[["x_m", "y_m"]].values)  # nút đường gần nhất của từng pixel
    d["nut_duong"] = nut  # chỉ số nút
    d["kc_ra_duong_m"] = kc  # khoảng cách đường chim bay từ tâm pixel ra nút đó (đi bộ, A6)
    log.info("Pixel: %d (trong 56 xa: %d, ngoai: %d)", len(d), d.ma_tram.notna().sum(), d.ma_tram.isna().sum())  # thống kê
    ghi_csv(d, "o_luoi_100m", "moi pixel 100 m trong hop bao; ma_tram rong = ngoai 56 xa; do cao, nut duong gan nhat")  # ghi bảng
    log.info("Xong. Bronze tra cuu khong gian: %s", DICH)  # thông báo


if __name__ == "__main__":  # chạy trực tiếp
    main()  # gọi hàm chính
