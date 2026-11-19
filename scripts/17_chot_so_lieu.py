# -*- coding: utf-8 -*-
"""17 - CHOT SO LIEU: kiem tra moi file ket qua khop run_manifest.csv, roi rut cac con so chinh dung trong bao cao
kem file nguon, cach lay va SHA-256 cua file luc ghi.

Khong tinh so moi: chi DOC cac bang da co. Chay CUOI CUNG, sau run_all.sh va run_spark.sh (buoc 11, 16, 15).
Dau ra:
  outputs/tables/kiem_tra_run_manifest.csv  1 dong / file trong outputs/ va data/interim/: co trong so? SHA-256 con khop?
  outputs/tables/so_lieu_chot.csv           1 dong / con so chot: nhom, chi so, gia tri, don vi, file, cach lay, SHA-256 file
Neu co file lech SHA-256 hoac chua dang ky, log ghi CAN XEM va bang so_lieu_chot ghi canh bao o cot trang_thai_file.
Chay: cd scripts && python 17_chot_so_lieu.py
"""
import pandas as pd

from common import ROOT, TAB, INTERIM, FIG, REF, RUN_MANIFEST, BASE_YEAR, get_logger, save_csv, sha256

S = "17_chot_so_lieu"
log = get_logger(S)


def doc(ten):
    return pd.read_csv(TAB / ten, encoding="utf-8-sig")


def main():
    man = pd.read_csv(RUN_MANIFEST, encoding="utf-8")
    man = man.drop_duplicates("file", keep="last").set_index("file")

    # ---- 1. kiem tra so ket qua ----
    rows = []
    files = sorted(list(TAB.glob("*.csv")) + list(FIG.glob("*.png")) + [p for p in INTERIM.glob("*") if p.is_file() and p.name != ".gitkeep"])
    for p in files:
        rel = str(p.relative_to(ROOT))
        if p.name in ("so_lieu_chot.csv", "kiem_tra_run_manifest.csv"):
            continue
        if rel not in man.index:
            rows.append(dict(file=rel, co_trong_so=False, sha256_khop=None, script="", created_utc="", trang_thai="CẦN XEM – chưa đăng ký"))
            continue
        m = man.loc[rel]
        khop = sha256(p) == m.sha256
        rows.append(dict(file=rel, co_trong_so=True, sha256_khop=khop, script=m.script, created_utc=m.created_utc,
                         trang_thai="ĐẠT" if khop else "CẦN XEM – file đổi sau khi đăng ký"))
    for rel in man.index:
        if rel.startswith(("outputs/", "data/interim/")) and not (ROOT / rel).exists():
            rows.append(dict(file=rel, co_trong_so=True, sha256_khop=None, script=man.loc[rel].script,
                             created_utc=man.loc[rel].created_utc, trang_thai="CẦN XEM – có trong sổ nhưng không còn file"))
    kt = pd.DataFrame(rows)
    save_csv(kt, TAB / "kiem_tra_run_manifest.csv", S, note="doi chieu SHA-256 moi file ket qua voi run_manifest.csv")
    xau = kt[kt.trang_thai != "ĐẠT"]
    log.info("Kiem tra run_manifest: %d file, %d DAT, %d CAN XEM", len(kt), int((kt.trang_thai == "ĐẠT").sum()), len(xau))
    for _, r in xau.iterrows():
        log.warning("CAN XEM %s: %s", r.file, r.trang_thai)
    tt_file = dict(zip(kt.file, kt.trang_thai))

    # ---- 2. so lieu chot ----
    out = []

    def chot(nhom, chi_so, gia_tri, don_vi, file, cach_lay):
        rel = file if "/" in file else f"outputs/tables/{file}"
        m = man.loc[rel] if rel in man.index else None
        out.append(dict(nhom=nhom, chi_so=chi_so, gia_tri=gia_tri, don_vi=don_vi, file=rel, cach_lay=cach_lay,
                        script=m.script if m is not None else "", created_utc=m.created_utc if m is not None else "",
                        sha256_file=m.sha256 if m is not None else "",
                        trang_thai_file=tt_file.get(rel, "ĐẠT" if rel.startswith("data/reference/") else "CẦN XEM – chưa đăng ký")))

    B = BASE_YEAR
    k = doc("kpi_tong_hop_spark.csv")
    for _, r in k.iterrows():
        chot(f"KPI {B} ({r.loai})", r.chi_so, r.gia_tri, r.don_vi, "kpi_tong_hop_spark.csv", f"dòng chi_so = '{r.chi_so}'")

    g = doc("greedy_before_after.csv").set_index("K")
    for K in (0, 5):
        chot("Thêm trạm", f"Tỷ lệ dân trong 3 km với {K} điểm đề xuất", g.loc[K, "ty_le_3km"], "%", "greedy_before_after.csv", f"K = {K}, cột ty_le_3km")
        chot("Thêm trạm", f"Dân cách trên 5 km với {K} điểm đề xuất", round(g.loc[K, "dan_tren_5km"]), "người", "greedy_before_after.csv", f"K = {K}, cột dan_tren_5km")
    u = doc("ung_vien_gop_tram.csv")
    chot("Bớt trạm (A15)", "TYT có từ 6 CSKCB trong 2 km (ứng viên rà soát)", len(u), "trạm", "ung_vien_gop_tram.csv", "số dòng")
    chot("Bớt trạm (A15)", "…trong đó đã bị NQ 1251 gộp", int(u.xa_bi_gop_nq1251.sum()), "trạm", "ung_vien_gop_tram.csv", "xa_bi_gop_nq1251 = True")
    chua = u[~u.xa_bi_gop_nq1251].ten_tram.str.replace("Trạm Y tế ", "").tolist()
    chot("Bớt trạm (A15)", "…chưa gộp, cần rà soát: " + ", ".join(chua), len(chua), "trạm", "ung_vien_gop_tram.csv", "xa_bi_gop_nq1251 = False")
    q15 = doc("doi_soat_nq1251_quy_tac.csv").iloc[0]
    chot("Bớt trạm (A15)", f"Quy tắc A15 khớp NQ 1251 (TP / dự đoán) – lạc quan vì ngưỡng chọn sau", f"{q15.dung_TP}/{q15.so_xa_du_doan}", "xã",
         "doi_soat_nq1251_quy_tac.csv", "dòng đầu, cột dung_TP và so_xa_du_doan")

    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    xa3 = st[st.kc_tram_tam_dan_cu_m > 3000]
    chot("Đặt sai chỗ", "Trạm cách tâm dân cư xã trên 3 km: " + ", ".join(xa3.ten_tram.str.replace("Trạm Y tế ", "")), len(xa3), "trạm",
         "data/interim/stations_56.csv", "kc_tram_tam_dan_cu_m > 3000")
    for _, r in xa3.iterrows():
        chot("Đặt sai chỗ", f"Khoảng cách {r.ten_tram} – tâm dân cư xã", r.kc_tram_tam_dan_cu_m, "m", "data/interim/stations_56.csv", f"ma_tram = {r.ma_tram}")
    x = doc("kpi_theo_xa_spark.csv").sort_values("ty_le_trong_3km_pct")
    lo = x.iloc[0]
    chot("Đặt sai chỗ", f"Xã có tỷ lệ dân trong 3 km thấp nhất: {lo.ten_day_du}", round(lo.ty_le_trong_3km_pct, 2), "%", "kpi_theo_xa_spark.csv", "min ty_le_trong_3km_pct")
    chot("Đặt sai chỗ", f"{lo.ten_day_du}: tỷ lệ dân trong 3 km theo đường đi", round(lo.ty_le_trong_3km_mang_duong_pct, 2), "%", "kpi_theo_xa_spark.csv", f"ma_tram = {lo.ma_tram}")
    ng = int((~st.trong_ranh_gioi_xa).sum())
    chot("Đặt sai chỗ", "Trạm nằm ngoài ranh giới xã của chính nó (lệch nhỏ, do ranh giới GADM – không phải bằng chứng)", ng, "trạm",
         "data/interim/stations_56.csv", f"trong_ranh_gioi_xa = False; xa nhất {st.kc_ngoai_ranh_gioi_m.max():.0f} m")

    v = doc("tram_vung_phuc_vu_spark.csv")
    vmin, vmax = v.loc[v.dan_phuc_vu.idxmin()], v.loc[v.dan_phuc_vu.idxmax()]
    chot("Phân bổ tải", f"Dân vùng phục vụ ít nhất: {vmin.ten_tram}", round(vmin.dan_phuc_vu), "người", "tram_vung_phuc_vu_spark.csv", "min dan_phuc_vu")
    chot("Phân bổ tải", f"Dân vùng phục vụ nhiều nhất: {vmax.ten_tram}", round(vmax.dan_phuc_vu), "người", "tram_vung_phuc_vu_spark.csv", "max dan_phuc_vu")
    chot("Phân bổ tải", "Trung vị dân vùng phục vụ", round(v.dan_phuc_vu.median()), "người", "tram_vung_phuc_vu_spark.csv", "median dan_phuc_vu")
    chot("Phân bổ tải", "Chênh lệch nhiều nhất / ít nhất", round(vmax.dan_phuc_vu / vmin.dan_phuc_vu, 1), "lần", "tram_vung_phuc_vu_spark.csv", "max / min dan_phuc_vu")

    n = doc("kpi_theo_nam_spark.csv").set_index("year")
    y0, y1 = n.index.min(), n.index.max()
    for y in (y0, y1):
        chot("Theo năm", f"Tổng dân 56 xã năm {y}", round(n.loc[y, "dan"]), "người", "kpi_theo_nam_spark.csv", f"year = {y}, cột dan")
        chot("Theo năm", f"Tỷ lệ dân trong 3 km năm {y}", round(n.loc[y, "ty_le_trong_3km_pct"], 2), "%", "kpi_theo_nam_spark.csv", f"year = {y}")
    a30 = doc("access_2030_total.csv").iloc[0]
    chot("Dự báo 2030", "Tỷ lệ dân trong 3 km năm 2030 (dự báo, giữ 56 trạm)", a30.ty_le_3km_2030, "%", "access_2030_total.csv", "cột ty_le_3km_2030")
    chot("Dự báo 2030", "Dân cách trên 5 km năm 2030 (dự báo)", round(a30.dan_tren_5km_2030), "người", "access_2030_total.csv", "cột dan_tren_5km_2030")
    f30 = doc("forecast_total_2030.csv").set_index("chi_so").gia_tri
    for key, ten in (("dan_2030", "Tổng dân 2030 (dự báo)"), ("dan_2030_lo80", "…cận dưới khoảng 80%"), ("dan_2030_hi80", "…cận trên khoảng 80%"),
                     ("dan60_2030", "Dân 60+ năm 2030 (dự báo)")):
        chot("Dự báo 2030", ten, round(f30[key]), "người", "forecast_total_2030.csv", f"chi_so = {key}")

    w = doc("white_zones.csv")
    tr = w[w.loai_vung.str.startswith("trang")]
    chot("Nguyên nhân", "Cụm dân cách trạm trên 5 km", len(tr), "cụm", "white_zones.csv", "loai_vung = trang_>5km")
    chot("Nguyên nhân", "…mang cả địa hình và dân cư phân tán", int(tr.nguyen_nhan.str.contains("Địa hình").sum()), "cụm", "white_zones.csv",
         "nguyen_nhan chứa 'Địa hình' (giả định A8)")

    tt = pd.read_csv(REF / "tt30_2024_goi_dich_vu.csv", encoding="utf-8-sig")
    chot("CSKCB không thay TYT", "Gói dịch vụ TYT theo TT 30/2024 mà cơ sở khác không thay được (đánh giá của nhóm, A16)",
         f"{int((tt.benh_vien_tuyen_tren_thay_the.str.strip() == 'Không').sum())}/{len(tt)}", "gói", "data/reference/tt30_2024_goi_dich_vu.csv",
         "benh_vien_tuyen_tren_thay_the = Không")
    cs = doc("cskcb_danang.csv")
    chot("CSKCB không thay TYT", "CSKCB trong danh sách", len(cs), "cơ sở", "cskcb_danang.csv", "số dòng")
    for cap, sl in cs.cap.value_counts().items():
        chot("CSKCB không thay TYT", f"…{cap}", int(sl), "cơ sở", "cskcb_danang.csv", f"cap = {cap}")
    hv = doc("tyt_cskcb_theo_nhom.csv").set_index("nhom_dia_ban").loc["Huyện Hòa Vang"]
    chot("CSKCB không thay TYT", "Hòa Vang: TYT không có CSKCB nào trong 2 km", f"{int(hv.so_tram_khong_co_cskcb_2km)}/{int(hv.so_tram)}", "trạm", "tyt_cskcb_theo_nhom.csv", "nhom_dia_ban = Huyện Hòa Vang")
    chot("CSKCB không thay TYT", "Hòa Vang: TYT không có CSKCB nào trong 3 km", f"{int(hv.so_tram_khong_co_cskcb_3km)}/{int(hv.so_tram)}", "trạm", "tyt_cskcb_theo_nhom.csv", "nhom_dia_ban = Huyện Hòa Vang")
    chot("CSKCB không thay TYT", "Hòa Vang: khoảng cách trung bình tới CSKCB gần nhất", round(hv.kc_cskcb_gan_nhat_tb_m), "m", "tyt_cskcb_theo_nhom.csv", "kc_cskcb_gan_nhat_tb_m")
    chot("CSKCB không thay TYT", "Hòa Vang: khoảng cách xa nhất tới CSKCB gần nhất", round(hv.kc_cskcb_gan_nhat_max_m), "m", "tyt_cskcb_theo_nhom.csv", "kc_cskcb_gan_nhat_max_m")

    sd = doc("spark_so_dong_theo_nguon.csv")
    chot("Chất lượng dữ liệu", "Dòng lỗi dữ liệu (quarantine)", int(sd.so_dong_loi_du_lieu.sum()), "dòng", "spark_so_dong_theo_nguon.csv", "tổng so_dong_loi_du_lieu")
    chot("Chất lượng dữ liệu", "Dòng ngoài phạm vi (excluded – không phải lỗi)", int(sd.so_dong_ngoai_pham_vi.sum()), "dòng", "spark_so_dong_theo_nguon.csv", "tổng so_dong_ngoai_pham_vi")
    ds = doc("doi_soat_spark.csv")
    chot("Chất lượng dữ liệu", "Phép đối soát Spark đạt", f"{int((ds.ket_qua == 'ĐẠT').sum())}/{len(ds)}", "phép", "doi_soat_spark.csv", "ket_qua = ĐẠT")
    qa = doc("qa_summary.csv")
    chot("Chất lượng dữ liệu", "Kiểm tra chất lượng cần xem", f"{int((qa.dat != 'ĐẠT').sum())}/{len(qa)}", "kiểm tra", "qa_summary.csv", "dat ≠ ĐẠT")

    so = pd.DataFrame(out)
    save_csv(so, TAB / "so_lieu_chot.csv", S, note="con so chot cho bao cao kem file nguon va SHA-256 luc ghi")
    log.info("Chot %d con so tu %d file; file nguon CAN XEM: %d", len(so), so.file.nunique(), int((so.trang_thai_file != "ĐẠT").sum()))


if __name__ == "__main__":
    main()
