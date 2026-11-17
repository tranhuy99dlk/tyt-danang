# -*- coding: utf-8 -*-
"""10 - Tong hop kiem tra chat luong + doi chieu dan so chinh thuc + tu dien du lieu.

Dau ra: outputs/tables/qa_summary.csv, qa_official_population.csv, tu_dien_du_lieu.csv
"""
import json

import numpy as np
import pandas as pd

from common import ROOT, RAW, REF, INTERIM, TAB, BBOX, BASE_YEAR, YEARS, PANEL, norm_name, get_logger, save_csv

S = "10_quality"
log = get_logger(S)


def main():
    grid = pd.read_parquet(INTERIM / "grid_100m.parquet")
    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    ref = pd.read_csv(REF / "danh_sach_56_tram.csv", encoding="utf-8-sig")
    qb = pd.read_csv(TAB / "qa_boundaries.csv", encoding="utf-8-sig")
    cs = pd.read_csv(TAB / "commune_series.csv", encoding="utf-8-sig")
    qa = []

    def add(nhom, kt, kq, dat, ghi=""):
        qa.append(dict(nhom=nhom, kiem_tra=kt, ket_qua=kq, dat=dat, ghi_chu=ghi))

    # THIEU
    add("Thiếu", "Danh sách trạm đủ 56 dòng", len(ref), len(ref) == 56)
    add("Thiếu", "Xã có ranh giới", int((qb.nguon_dung != "THIEU").sum()), (qb.nguon_dung != "THIEU").all())
    add("Thiếu", "Trạm có toạ độ thực (nhập tay/OSM, không phải ước lượng)", int((~st.uoc_luong).sum()), (~st.uoc_luong).all(),
        "Trạm ước lượng dùng tâm dân cư xã (A5)")
    add("Thiếu", "Xã không có ô dân nào", int(56 - grid.ma_tram.nunique()), grid.ma_tram.nunique() == 56)
    add("Thiếu", "Ô thiếu độ cao", int(grid.elev_m.isna().sum()), grid.elev_m.isna().mean() < 0.01)
    yrs = cs.groupby("ma_tram").year.nunique()
    add("Thiếu", f"Xã đủ {len(YEARS)} năm trong chuỗi ({YEARS[0]}–{YEARS[-1]})", int((yrs == len(YEARS)).sum()), (yrs == len(YEARS)).all())
    # TRUNG
    add("Trùng", "Mã ô lưới duy nhất", int(grid.cell_id.duplicated().sum()), not grid.cell_id.duplicated().any(), "số bản trùng")
    add("Trùng", "Mã trạm duy nhất", int(st.ma_tram.duplicated().sum()), not st.ma_tram.duplicated().any())
    add("Trùng", "Tên xã/phường trùng sau chuẩn hoá", int(ref.ten_xa_phuong.map(norm_name).duplicated().sum()),
        not ref.ten_xa_phuong.map(norm_name).duplicated().any())
    qs = pd.read_csv(TAB / "qa_stations.csv", encoding="utf-8-sig")
    add("Trùng", "Cặp trạm trùng toạ độ (< 50 m)", int(qs.cap_trung_toa_do_50m.sum() // 2), qs.cap_trung_toa_do_50m.sum() == 0)
    add("Trùng", "Trạm có ≥ 2 ứng viên OSM cách nhau > 300 m", int((qs.ung_vien_cach_xa_300m > 0).sum()),
        (qs.ung_vien_cach_xa_300m == 0).all(), "cần kiểm tra thực địa")
    # SAI TOA DO
    inb = st.lon.between(BBOX[0], BBOX[2]) & st.lat.between(BBOX[1], BBOX[3])
    add("Sai toạ độ", "Trạm nằm ngoài hộp bao Đà Nẵng cũ", int((~inb).sum()), inb.all())
    add("Sai toạ độ", "Trạm (nhập tay/OSM) nằm ngoài ranh giới xã của chính nó", int((~st.trong_ranh_gioi_xa).sum()),
        st.trong_ranh_gioi_xa.all(), "có thể đúng (trạm đặt sát ranh) hoặc sai gắn thẻ")
    far = st[st.kc_tram_tam_dan_cu_m > 3000]
    add("Sai toạ độ", "Trạm cách tâm dân cư xã > 3 km", len(far), len(far) == 0, ", ".join(far.ten_tram))
    if "iou_osm_gadm" in qb and qb.iou_osm_gadm.notna().any():
        add("Sai toạ độ", "Ranh giới OSM vs GADM: IoU trung vị", round(qb.iou_osm_gadm.median(), 3), qb.iou_osm_gadm.median() > 0.8)
        add("Sai toạ độ", "Xã có IoU OSM-GADM < 0,7", int((qb.iou_osm_gadm < 0.7).sum()), (qb.iou_osm_gadm < 0.7).sum() == 0,
            ", ".join(qb.loc[qb.iou_osm_gadm < 0.7, "ten_xa_phuong"]))
    # LECH TONG DAN SO / NHAT QUAN
    neg = (grid.filter(like="pop_") < 0).sum().sum()
    add("Lệch tổng", "Giá trị dân số âm", int(neg), neg == 0)
    bad60 = (grid[f"pop60_{BASE_YEAR}"] > grid[f"pop_{BASE_YEAR}"] + 1e-6).sum()
    add("Lệch tổng", "Ô có dân 60+ > tổng dân", int(bad60), bad60 == 0)
    sh = grid[f"pop60_{BASE_YEAR}"].sum() / grid[f"pop_{BASE_YEAR}"].sum()
    add("Lệch tổng", f"Tỷ lệ 60+ toàn 56 xã năm {BASE_YEAR}", round(100 * sh, 2), 5 < 100 * sh < 30, "% - kiểm tra hợp lý")
    v = TAB / "validation_downscale_A4.csv"
    if v.exists():
        vv = pd.read_csv(v, encoding="utf-8-sig")
        for _, r in vv.iterrows():
            add("Lệch tổng", f"Hạ cấp 1 km→100 m năm {int(r.nam)}: MAPE theo xã (%)", r.mape_xa_pct, r.mape_xa_pct < 5)

    # doi chieu so lieu chinh thuc NQ 1251
    off = pd.read_csv(REF / "dan_so_chinh_thuc_nq1251.csv", encoding="utf-8-sig")
    key2ma = dict(zip(ref.ten_xa_phuong.map(norm_name), ref.ma_tram))
    rows = []
    for yr in sorted({BASE_YEAR - 1, BASE_YEAR}):
        cy = cs[cs.year == yr].set_index("ma_tram")["pop"]
        for _, r in off.iterrows():
            mas = [key2ma.get(norm_name(x)) for x in r.thanh_phan.split(";")]
            if None in mas:
                continue
            wp = cy.loc[mas].sum()
            rows.append(dict(nam_worldpop=yr, doi_tuong=r.doi_tuong, loai=r.loai, dan_chinh_thuc=r.dan_so,
                             dan_worldpop=round(wp), lech_pct=round(100 * (wp / r.dan_so - 1), 1), nguon=r.nguon_url))
    oc = pd.DataFrame(rows)
    save_csv(oc, TAB / "qa_official_population.csv", S, note="moc so lieu NQ 1251 chua xac dinh nam (ho so de an)")
    for yr, d in oc.groupby("nam_worldpop"):
        mape = d.lech_pct.abs().mean()
        add("Lệch tổng", f"WorldPop {yr} so với số liệu NQ 1251 ({len(d)} đơn vị/nhóm): MAPE (%)", round(mape, 1), mape < 20,
            "ngưỡng chấp nhận ±20% (A11); lệch có hệ thống cho thấy khác khái niệm dân số")
        add("Lệch tổng", f"WorldPop {yr} so với NQ 1251: lệch trung vị có dấu (%)", round(d.lech_pct.median(), 1), abs(d.lech_pct.median()) < 20)
    qdf = pd.DataFrame(qa)
    qdf["dat"] = qdf.dat.map({True: "ĐẠT", False: "CẦN XEM", np.True_: "ĐẠT", np.False_: "CẦN XEM"})
    save_csv(qdf, TAB / "qa_summary.csv", S)
    log.info("QA: %d kiem tra, %d CAN XEM", len(qdf), (qdf.dat == "CẦN XEM").sum())

    # ---- TU DIEN DU LIEU ----
    WP = "WorldPop Global2 R2025A (CC BY 4.0)"
    OSM = "OpenStreetMap qua Overpass (ODbL 1.0)"
    D = []

    def fld(ds, name, kieu, y_nghia, nguon, quy_tac, nhay):
        D.append(dict(tap_du_lieu=ds, ten_truong=name, kieu=kieu, y_nghia=y_nghia, nguon=nguon, quy_tac_chat_luong=quy_tac, muc_nhay_cam=nhay))
    ds = "data/reference/danh_sach_56_tram.csv"
    for n, k, y, q in (("ma_tram", "chuỗi", "Mã trạm/xã nội bộ TYT01–TYT56", "duy nhất, không rỗng"),
                       ("quan_huyen", "chuỗi", "Quận/huyện trước 01/01/2025", "∈ 7 quận/huyện"),
                       ("loai_dv", "chuỗi", "Phường hoặc Xã", "∈ {Phường, Xã}"),
                       ("ten_xa_phuong", "chuỗi", "Tên xã/phường (không tiền tố)", "duy nhất sau chuẩn hoá"),
                       ("ten_tram", "chuỗi", "Tên trạm y tế", "không rỗng")):
        fld(ds, n, k, y, "Người dùng cung cấp (chat 15/11/2026)", q, "Công khai")
    ds = "data/interim/stations_56.csv"
    for n, k, y, q in (("lon, lat", "số thực (WGS84)", "Toạ độ trạm", "trong hộp bao; trong ranh giới xã"),
                       ("x_m, y_m", "số thực (UTM 49N, m)", "Toạ độ phẳng để đo khoảng cách", "suy từ lon/lat"),
                       ("nguon_toa_do", "chuỗi", "NHAP TAY (A13) / OSM 2024-12-31 / OSM current / UOC LUONG (A5)", "không rỗng"),
                       ("osm_id", "chuỗi", "type/id đối tượng OSM", "rỗng khi ước lượng"),
                       ("trong_ranh_gioi_xa", "bool", "Toạ độ nằm trong ranh giới xã", "True kỳ vọng"),
                       ("kc_tram_tam_dan_cu_m", "số thực (m)", "Khoảng cách trạm → tâm dân cư xã (trọng số dân năm cơ sở)", "≤ 3.000 m kỳ vọng"),
                       ("uoc_luong", "bool", "Vị trí là ước lượng (không có toạ độ nhập tay hợp lệ và không có trên OSM)", "False kỳ vọng")):
        fld(ds, n, k, y, "Nhóm nhập tay (data/raw/location-tyt.xlsx), kiểm tra theo congdulieu.vn – dữ liệu cập nhật 12/12/2024 (A13); " + OSM, q, "Công khai")
    ds = "data/interim/communes_56.geojson"
    for n, k, y, q in (("geometry", "đa giác WGS84", "Ranh giới xã mốc 31/12/2024", "hợp lệ, không chồng lấn"),
                       ("dien_tich_km2", "số thực", "Diện tích tính trên UTM 49N", "> 0"),
                       ("nguon_ranh_gioi", "chuỗi", "OSM 2024-12-31 hoặc GADM 4.1 (dự phòng)", "không rỗng")):
        fld(ds, n, k, y, OSM + "; GADM 4.1 (học thuật, không phân phối lại)", q, "Công khai")
    ds = "data/interim/grid_100m.parquet"
    for n, k, y, q, nh in (("cell_id", "chuỗi", "Mã ô 100 m (hàng/cột trong cửa sổ hộp bao)", "duy nhất", "Công khai"),
                           ("lon, lat, x_m, y_m", "số thực", "Tâm ô", "trong 1 trong 56 xã", "Công khai"),
                           ("ma_tram", "chuỗi", "Xã chứa tâm ô", "∈ 56 mã", "Công khai"),
                           ("pop_" + "/".join(str(y) for y in YEARS), "số thực (người)", "Dân số ước tính trong ô (WorldPop 100 m constrained)", "≥ 0; không NaN", "Thấp (mô hình, tổng hợp)"),
                           (f"share60_{BASE_YEAR}", "số thực [0,1]", "Tỷ lệ 60+ lấy từ ô 1 km chứa ô (A3)", "0–1", "Thấp"),
                           (f"pop60_{BASE_YEAR}", "số thực (người)", f"Dân 60+ ước tính = pop_{BASE_YEAR} × share60_{BASE_YEAR}", f"≤ pop_{BASE_YEAR}", "Trung bình (nhóm dễ tổn thương, chỉ công bố tổng hợp)"),
                           ("elev_m, slope_deg", "số thực", "Độ cao, độ dốc tại tâm ô", "elev ≥ -10", "Công khai")):
        fld(ds, n, k, y, WP + "; AWS Terrain Tiles", q, nh)
    ds = str(PANEL.relative_to(ROOT))
    for n, k, y, q in (("cell_id, ma_tram, year", "chuỗi/số nguyên", "Khoá ô × năm", "duy nhất theo (cell_id, year)"),
                       ("pop, pop60", "số thực", "Giá trị tốt nhất có (100 m trực tiếp hoặc hạ cấp A4)", "≥ 0"),
                       ("pop_ds, pop60_ds", "số thực", "Chuỗi hạ cấp từ 1 km (A4) – dùng để kiểm định A4 và làm chuỗi học của bước dự báo 08; không dùng cho KPI", "≥ 0"),
                       ("nguon", "chuỗi", "100m_truc_tiep / ha_cap_1km_A4", "không rỗng")):
        fld(ds, n, k, y, WP, q, "Thấp")
    ds = "data/interim/grid_access.parquet"
    for n, k, y, q in (("dist_m", "số thực (m)", "Khoảng cách đường chim bay tới trạm gần nhất", "≥ 0"),
                       ("tram_gan_nhat", "chuỗi", "Mã trạm gần nhất", "∈ 56 mã"),
                       ("net_dist_m, net_time_min", "số thực", "Khoảng cách/thời gian theo mạng đường (A6)", "≥ dist_m"),
                       ("detour", "số thực", "Hệ số vòng = net_dist_m / max(dist_m, 50)", "≥ 1 kỳ vọng"),
                       ("nhom", "chuỗi", "Phường / Xã đồng bằng-ven đô / Xã miền núi (địa hình, A7)", "không rỗng")):
        fld(ds, n, k, y, "Tính từ WorldPop + OSM", q, "Thấp")
    ds = "outputs/tables/greedy_sites.csv"
    fld(ds, "lon, lat, xa, dan_moi_duoc_phu", "số", "Vị trí đề xuất và số dân được phủ mới", "Tính bởi 09_greedy.py",
        "vị trí là ô dân cư thật; kiểm tra quỹ đất trước khi công bố", "Nội bộ – chưa công bố (có thể ảnh hưởng giá đất)")
    # ---- vung thu nhan (CSV tho cho Spark) va san pham cua Spark (11a_landing.py, 11_spark_pipeline.py) ----
    QR = "vi phạm → vùng quarantine kèm mã lý do"
    for ds, rows in (
        ("data/landing/pop_100m_raw.csv", (
            ("year", "số nguyên", "Năm của raster WorldPop 100 m", f"∈ {YEARS[0]}–{YEARS[-1]}; {QR}", WP, "Thấp"),
            ("row, col", "số nguyên", "Hàng/cột pixel trong cửa sổ hộp bao; khoá cùng year", f"không rỗng, duy nhất; {QR}", WP, "Công khai"),
            ("pop_raw", "số thực (người)", "Giá trị pixel THÔ; −99999 là mã NoData của WorldPop", f"≥ 0; {QR} (GIA_TRI_AM_NODATA)", WP, "Thấp (mô hình, tổng hợp)"))),
        ("data/landing/cells_dim.csv", (
            ("row, col, cell_id", "số nguyên/chuỗi", "Khoá ô lưới 100 m", f"không rỗng, duy nhất; {QR}", WP, "Công khai"),
            ("lon, lat, x_m, y_m", "số thực", "Tâm ô (WGS84 và UTM 49N)", f"trong hộp bao; {QR}", WP, "Công khai"),
            ("ma_tram", "chuỗi", "Xã/phường chứa tâm ô; rỗng = ngoài 56 xã", f"∈ 56 mã; rỗng → quarantine (NGOAI_56_XA)", "Ranh giới GADM 4.1 (A12)", "Công khai"),
            ("k1_row, k1_col", "số nguyên", "Ô 1 km chứa ô 100 m (để nối tỷ lệ 60+)", "không rỗng", WP, "Công khai"),
            ("elev_m, slope_deg", "số thực", "Độ cao, độ dốc tại tâm ô", "elev ≥ −10; rỗng coi như 0 khi phân loại A7", "AWS Terrain Tiles (Mapzen)", "Công khai"))),
        ("data/landing/age_1km_raw.csv", (
            ("year, k1_row, k1_col, bien", "số nguyên/chuỗi", "Khoá ô 1 km × năm × biến (pop hoặc t_60…t_90)", f"duy nhất; bien ∈ 8 giá trị; {QR}", WP, "Thấp"),
            ("gia_tri", "số thực (người)", "Giá trị pixel 1 km THÔ; −99999 là NoData", f"≥ 0; {QR}", WP, "Trung bình (nhóm tuổi, chỉ công bố tổng hợp)"))),
        ("data/landing/stations_raw.csv", (
            ("dong_excel", "số nguyên", "Số dòng trong file Excel gốc", "duy nhất", "Nhóm nhập tay, kiểm tra theo congdulieu.vn 12/12/2024 (A13)", "Công khai"),
            ("cot1, cot2", "chuỗi", "Tên quận/huyện (dòng nhóm) và tên xã/phường", f"cot2 khớp danh mục 56 trạm; {QR}", "Nhóm nhập tay, kiểm tra theo congdulieu.vn 12/12/2024 (A13)", "Công khai"),
            ("cot3", "chuỗi", "Toạ độ dạng 'vĩ độ, kinh độ'", f"đúng định dạng, trong hộp bao; {QR}", "Nhóm nhập tay, kiểm tra theo congdulieu.vn 12/12/2024 (A13)", "Công khai (cơ sở y tế công)"))),
        ("data/landing/net_dist.csv", (
            ("cell_id", "chuỗi", "Khoá ô lưới", f"duy nhất, thuộc ô có dân; {QR}", "05_network.py", "Công khai"),
            ("net_dist_m, net_time_min, net_station", "số thực/chuỗi", "Đường đi ngắn nhất theo mạng đường OSM tới trạm, thời gian xe máy, trạm đích (A6)", f"≥ 0, hữu hạn; {QR}", OSM, "Thấp"))),
        ("data/spark/quarantine/dong_loi (Parquet)", (
            ("nguon", "chuỗi", "Tên file CSV thô chứa dòng lỗi", "không rỗng", "11_spark_pipeline.py", "Thấp"),
            ("ly_do", "chuỗi", "Mã lý do, nhiều lý do nối bằng ';'", "không rỗng", "11_spark_pipeline.py", "Thấp"),
            ("khoa, ban_ghi", "chuỗi", "Khoá và nguyên văn dòng lỗi (JSON)", "không rỗng", "11_spark_pipeline.py", "Thấp"))),
        ("outputs/tables/kpi_tong_hop_spark.csv", (
            ("chi_so, gia_tri, don_vi, loai, ghi_chu", "chuỗi/số", f"KPI chính và KPI phụ năm {BASE_YEAR} tính bằng Spark", "khớp pandas trong ngưỡng đối soát", "11_spark_pipeline.py", "Công khai (số tổng hợp)"),)),
        ("outputs/tables/kpi_theo_nam_spark.csv", (
            ("year, dan, dan_60, ty_le_trong_*km_pct, dan_tren_5km, kc_trung_vi_m, dan_binh_quan_moi_tram", "số", f"KPI toàn 56 xã từng năm {YEARS[0]}–{YEARS[-1]}, giữ nguyên 56 trạm (A14)", "0–100 với tỷ lệ; ≥ 0", "11_spark_pipeline.py", "Công khai (số tổng hợp)"),)),
        ("outputs/tables/doi_soat_spark.csv", (
            ("phep_doi_soat, spark, pandas_hoac_nguon, ket_qua, ghi_chu", "chuỗi/số", "Phép đối soát: bảo toàn số dòng/tổng dân và Spark so với pandas", "mọi dòng phải ĐẠT", "11_spark_pipeline.py", "Nội bộ"),)),
    ):
        for n, k, y, q, src, nh in rows:
            fld(ds, n, k, y, src, q, nh)
    ds = "outputs/tables/forecast_commune_2030.csv"
    for n, k, y, q in ((f"dan_{BASE_YEAR}, dan60_{BASE_YEAR}", "số thực (người)", "Giá trị năm cuối của chuỗi học (WorldPop, ước tính)", "> 0"),
                       ("dan_2030, dan60_2030", "số thực (người)", "DỰ BÁO của mô hình thắng backtest (08_forecast.py, A9)", "nằm trong khoảng lo80–hi80"),
                       ("dan_2030_lo80, dan_2030_hi80", "số thực (người)", "Khoảng 80% thực nghiệm từ sai số backtest ở h = 5", "lo ≤ dự báo ≤ hi"),
                       ("dan_2030_worldpop", "số thực (người)", "Số CHIẾU 2030 của WorldPop (chỉ để đối chiếu)", "≥ 0"),
                       ("lech_du_bao_vs_worldpop_pct", "số thực (%)", "Lệch giữa dự báo và số chiếu WorldPop", "kỳ vọng nhỏ")):
        fld(ds, n, k, y, "Tính từ " + WP + " – là dự báo, không phải số đo", q, "Thấp (số tổng hợp theo xã)")
    dd = pd.DataFrame(D)
    save_csv(dd, TAB / "tu_dien_du_lieu.csv", S)


if __name__ == "__main__":
    main()
