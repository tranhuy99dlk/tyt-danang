# -*- coding: utf-8 -*-
"""15 - TYT trong tuong quan voi cac co so kham chua benh (CSKCB) khac tren dia ban.

CAU HOI QUAN LY: nen them tram hay bot tram? Hai cau hoi nay von hay bi tron lan:
    (1) O DAU THIEU TIEP CAN    -> dua tren khoang cach dan den TYT   (buoc 06, 07, 09)
    (2) O DAU TRAM BI TRUNG LAP -> dua tren mat do CSKCB khac quanh TYT (buoc nay)

NGUON DU LIEU (thay cho ban OSM dung truoc 17/11/2026):
    data/raw/danh-sach-cskcb.xlsx - danh sach CSKCB do So Y te TP Da Nang cong bo, nguoi dung
    cung cap. Co san truong Tuyen CMKT, Hang benh vien, Cap CMKT va toa do, nen do duoc TRUC TIEP
    cap chuyen mon ky thuat cua tung co so - dieu ma OSM khong lam duoc (OSM chi co amenity).

PHAN CAP THEO LUAT KHAM BENH, CHUA BENH 2023 (Luat 15/2023/QH15, Dieu 104, hieu luc 01/01/2024):
    he thong KCB chia 03 CAP chuyen mon ky thuat: cap ban dau - cap co ban - cap chuyen sau.
    TYT xa/phuong thuoc cap ban dau. Script dem co so theo cot "Cap CMKT" cua chinh file nguon.

GIA DINH A15 (do nhom dat ra, da SUA ngay 17/11/2026 - xem log so sanh cac phuong an duoi):
    Ung vien ra soat gop tram = TYT co >= 6 CSKCB trong ban kinh 2 km duong chim bay.
    Nguong 6 co so / 2 km la lua chon cua nhom, chon vi cho ket qua doi soat tot nhat voi
    NQ 1251 trong 5 phuong an thu (xem outputs/tables/doi_soat_nq1251_quy_tac.csv).
    Ban A15 cu (>= 3 co so/1 km VA dan duoi trung vi VA TYT khac <= 1,5 km) van duoc tinh va
    bao cao trong cung bang de nguoi doc tu doi chieu - nhom KHONG bo nguong cu mot cach im lang.

CANH BAO khi doc ket qua (#Truthmode):
  - Danh sach nguon co 30 co so, deu nam o khu vuc trung tam/do thi. Day la danh sach do nguoi
    dung cung cap, Claude CHUA tu doi chieu voi cong bo goc cua So Y te; neu danh sach thieu co so
    nao o Hoa Vang thi muc chenh lech trung tam - mien nui bi phong dai. Phai neu la han che.
  - Co so cap co ban / chuyen sau KHONG thay the TYT: Thong tu 30/2024/TT-BYT giao TYT 15 nhom
    goi dich vu y te co ban, phan lon la du phong va quan ly suc khoe THEO DIA BAN (tiem chung mo
    rong, quan ly thai, quan ly benh khong lay nhiem, ho so suc khoe dien tu, phong chong dich).
    Benh vien tiep nhan nguoi TU DEN; TYT phai di tim nguoi theo danh sach cu tru. Xem
    data/reference/tt30_2024_goi_dich_vu.csv (cot benh_vien_tuyen_tren_thay_the).
  - Vi vay dau ra la DANH SACH RA SOAT, khong phai danh sach de nghi dong cua tram.

Dau ra:
  outputs/tables/cskcb_danang.csv            (danh sach CSKCB da lam sach + toa do)
  outputs/tables/tyt_cskcb.csv               (56 tram, khoang cach va so dem theo cap CMKT)
  outputs/tables/tyt_cskcb_theo_nhom.csv     (tong hop 3 nhom dia ban)
  outputs/tables/ung_vien_gop_tram.csv       (ung vien ra soat theo A15 moi)
  outputs/tables/doi_soat_nq1251_quy_tac.csv (so sanh 5 quy tac du doan voi NQ 1251)
  outputs/tables/doi_soat_nq1251_theo_xa.csv (phan loai TP/FP/FN/TN tung xa)
"""
import numpy as np
import pandas as pd
from scipy.stats import hypergeom

from common import INTERIM, RAW, REF, TAB, get_logger, register, save_csv, to_metric

S = "15_tuyen_tren"
log = get_logger(S)

BK = [1000, 2000, 3000, 5000]      # ban kinh dem (m)
A15_BK = 2000                      # A15 moi: ban kinh 2 km
A15_NGUONG = 6                     # A15 moi: >= 6 CSKCB trong 2 km
MIN_KCB_1KM = 3                    # A15 cu: >= 3 CSKCB trong 1 km
MAX_TYT_GAN_M = 1500               # A15 cu: TYT khac <= 1,5 km

# NQ 1251/NQ-UBTVQH15 (hieu luc 01/01/2025) - sao lai tu scripts/07_diagnose.py de doi chieu doc lap
NQ1251_GROUPS = [["Thanh Khê Đông", "Hòa Khê"], ["Xuân Hà", "Tam Thuận"], ["Thạc Gián", "Vĩnh Trung"],
                 ["Chính Gián", "Tân Chính"], ["An Hải Đông", "An Hải Tây"],
                 ["Phước Ninh", "Nam Dương", "Bình Hiên"], ["Bình Thuận", "Hòa Thuận Đông"],
                 ["Hải Châu I", "Hải Châu II"]]
NQ1251_XA = {x for g in NQ1251_GROUPS for x in g}


def doc_cskcb():
    """Doc danh sach CSKCB cua So Y te, tach toa do 'lat, lon' thanh hai cot so."""
    f = RAW / "danh-sach-cskcb.xlsx"
    d = pd.read_excel(f, sheet_name=0)
    d.columns = [str(c).strip() for c in d.columns]
    d = d.rename(columns={"Tên": "ten_cs", "Mã": "ma_cs", "Tuyến CMKT": "tuyen",
                          "Hạng bệnh viện": "hang", "Cấp CMKT": "cap", "Điểm": "diem",
                          "Địa chỉ": "dia_chi", "tọa độ": "toa_do"})
    d = d[d.ten_cs.notna()].copy()
    tach = d.toa_do.astype(str).str.split(",", n=1, expand=True)
    d["lat"] = pd.to_numeric(tach[0], errors="coerce")
    d["lon"] = pd.to_numeric(tach[1], errors="coerce")
    thieu = int((d.lat.isna() | d.lon.isna()).sum())
    if thieu:
        log.warning("CAN XEM: %d ban ghi CSKCB khong doc duoc toa do -> bi loai", thieu)
    d = d[d.lat.notna() & d.lon.notna()]
    register(f, S, rows=len(d), note="dau vao: danh sach CSKCB So Y te TP Da Nang (nguoi dung cung cap)")
    return d[["ma_cs", "ten_cs", "tuyen", "hang", "cap", "diem", "dia_chi", "lat", "lon"]].reset_index(drop=True)


def danh_gia(ten_qt, du_doan, thuc_te, N=56):
    """Bang nhap nhang + kiem dinh sieu boi (hypergeometric) cho mot quy tac du doan."""
    tp = int((du_doan & thuc_te).sum())
    fp = int((du_doan & ~thuc_te).sum())
    fn = int((~du_doan & thuc_te).sum())
    tn = int((~du_doan & ~thuc_te).sum())
    k, K = tp + fp, int(thuc_te.sum())
    prec = tp / k if k else np.nan
    rec = tp / K if K else np.nan
    f1 = 2 * prec * rec / (prec + rec) if k and tp else 0.0
    return {"quy_tac": ten_qt, "so_xa_du_doan": k, "so_xa_NQ1251_gop": K,
            "dung_TP": tp, "bao_dong_gia_FP": fp, "bo_sot_FN": fn, "dung_am_TN": tn,
            "do_chinh_xac_precision_pct": round(100 * prec, 1) if k else np.nan,
            "do_bao_phu_recall_pct": round(100 * rec, 1) if K else np.nan,
            "f1_pct": round(100 * f1, 1),
            "ky_vong_TP_neu_chon_ngau_nhien": round(k * K / N, 1),
            "p_value_hypergeom": f"{hypergeom.sf(tp - 1, N, K, k):.3g}" if k else "n/a"}


def main():
    cs = doc_cskcb()
    save_csv(cs.round(6), TAB / "cskcb_danang.csv", S,
             note="Danh sach CSKCB So Y te TP Da Nang da lam sach (tuyen, hang, cap CMKT, toa do)")
    log.info("CSKCB doc duoc: %d co so", len(cs))
    for col in ["tuyen", "cap", "hang"]:
        log.info("  %s: %s", col, cs[col].value_counts().to_dict())

    tt30 = pd.read_csv(REF / "tt30_2024_goi_dich_vu.csv", encoding="utf-8-sig")
    register(REF / "tt30_2024_goi_dich_vu.csv", S, rows=len(tt30),
             note="dau vao: 15 nhom goi dich vu y te co ban cua TYT theo TT 30/2024/TT-BYT")
    thay_duoc = int((tt30.benh_vien_tuyen_tren_thay_the.astype(str).str.strip().str.lower() == "có").sum())
    log.info("TT 30/2024/TT-BYT: %d nhom goi dich vu TYT; nhom co so khac co the thay the: %d/%d",
             len(tt30), thay_duoc, len(tt30))

    # doi chieu doc lap: 8 nhom gop hardcode o tren vs file tham chieu da dan nguon
    nq = pd.read_csv(REF / "nq1251_sap_xep.csv", encoding="utf-8-sig")
    register(REF / "nq1251_sap_xep.csv", S, rows=len(nq),
             note="dau vao: 8 don vi gop + 3 dieu chinh mot phan theo NQ 1251/NQ-UBTVQH15 ngay 24/10/2024")
    tu_file = set()
    for s in nq[nq.so_xa_nhap >= 2].cac_xa_nhap_vao:
        tu_file.update(x.strip() for x in str(s).split("+"))
    if tu_file != NQ1251_XA:
        log.warning("LECH: danh sach xa gop trong code vs file tham chieu khac nhau: %s",
                    tu_file.symmetric_difference(NQ1251_XA))
    else:
        log.info("Doi chieu NQ 1251: %d xa gop thanh %d don vi - code va file tham chieu KHOP",
                 len(NQ1251_XA), int((nq.so_xa_nhap >= 2).sum()))
    log.info("NQ 1251 con co %d dieu chinh mot phan dien tich (khong lap don vi moi), quy tac A15 khong mo hinh hoa",
             int((nq.so_xa_nhap == 0).sum()))

    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    vpv = pd.read_csv(TAB / "tram_vung_phuc_vu_spark.csv", encoding="utf-8-sig")[
        ["ma_tram", "dan_phuc_vu", "dan60_phuc_vu"]]
    kpi = pd.read_csv(TAB / "kpi_theo_xa_spark.csv", encoding="utf-8-sig")[
        ["ma_tram", "ten_day_du", "quan_huyen", "nhom", "dan",
         "ty_le_trong_3km_pct", "ty_le_trong_3km_mang_duong_pct"]]

    sx, sy = to_metric(st.lon.values, st.lat.values)
    fx, fy = to_metric(cs.lon.values, cs.lat.values)

    # khoang cach giua cac TYT voi nhau
    T = np.sqrt((sx[:, None] - sx[None, :]) ** 2 + (sy[:, None] - sy[None, :]) ** 2)
    np.fill_diagonal(T, np.inf)
    st["tyt_gan_nhat"] = st.ten_tram.values[T.argmin(1)]
    st["tyt_gan_nhat_m"] = T.min(1).round(0)

    # khoang cach TYT -> CSKCB
    D = np.sqrt((sx[:, None] - fx[None, :]) ** 2 + (sy[:, None] - fy[None, :]) ** 2)
    j = D.argmin(1)
    st["cskcb_gan_nhat"] = cs.ten_cs.values[j]
    st["cskcb_gan_nhat_cap"] = cs.cap.values[j]
    st["cskcb_gan_nhat_tuyen"] = cs.tuyen.values[j]
    st["kc_cskcb_gan_nhat_m"] = D.min(1).round(0)
    for r in BK:
        st[f"cskcb_{r // 1000}km"] = (D <= r).sum(1)
    for cap, hau_to in [("Cấp cơ bản", "co_ban"), ("Cấp chuyên sâu", "chuyen_sau"), ("Cấp ban đầu", "ban_dau")]:
        m = (cs.cap == cap).values
        st[f"cap_{hau_to}_2km"] = (D[:, m] <= 2000).sum(1) if m.any() else 0
    m_cs = (cs.cap == "Cấp chuyên sâu").values
    st["cap_chuyen_sau_5km"] = (D[:, m_cs] <= 5000).sum(1) if m_cs.any() else 0

    out = (st[["ma_tram", "ten_tram", "lon", "lat", "tyt_gan_nhat", "tyt_gan_nhat_m",
               "cskcb_gan_nhat", "cskcb_gan_nhat_tuyen", "cskcb_gan_nhat_cap", "kc_cskcb_gan_nhat_m",
               "cskcb_1km", "cskcb_2km", "cskcb_3km", "cskcb_5km",
               "cap_co_ban_2km", "cap_chuyen_sau_2km", "cap_ban_dau_2km", "cap_chuyen_sau_5km"]]
           .merge(kpi, on="ma_tram", how="left").merge(vpv, on="ma_tram", how="left"))
    out["ten_xa"] = out.ten_day_du.str.replace("Phường |Xã ", "", regex=True)
    out["xa_bi_gop_nq1251"] = out.ten_xa.isin(NQ1251_XA)
    out["nhom_dia_ban"] = np.where(out.quan_huyen.str.contains("Hòa Vang"), "Huyện Hòa Vang",
                                   np.where(out.quan_huyen.isin(["Quận Hải Châu", "Quận Thanh Khê"]),
                                            "Quận lõi (Hải Châu, Thanh Khê)", "Quận ven"))

    nguong_dan = out.dan_phuc_vu.median()
    out["ung_vien_A15_moi"] = out[f"cskcb_{A15_BK // 1000}km"] >= A15_NGUONG
    out["ung_vien_A15_cu"] = ((out.cskcb_1km >= MIN_KCB_1KM) & (out.dan_phuc_vu < nguong_dan)
                              & (out.tyt_gan_nhat_m <= MAX_TYT_GAN_M))

    # --- doi soat voi NQ 1251 -------------------------------------------------
    thuc_te = out.xa_bi_gop_nq1251.values
    qt = {
        f"A15 moi: >= {A15_NGUONG} CSKCB trong 2 km": out.ung_vien_A15_moi.values,
        f"A15 cu: >= {MIN_KCB_1KM} CSKCB/1km + dan < trung vi + TYT khac <= {MAX_TYT_GAN_M} m": out.ung_vien_A15_cu.values,
        f"Chi >= {MIN_KCB_1KM} CSKCB trong 1 km": (out.cskcb_1km >= MIN_KCB_1KM).values,
        "CSKCB gan nhat <= 1000 m": (out.kc_cskcb_gan_nhat_m <= 1000).values,
        f">= {A15_NGUONG} CSKCB/2km VA TYT khac <= {MAX_TYT_GAN_M} m":
            ((out.cskcb_2km >= A15_NGUONG) & (out.tyt_gan_nhat_m <= MAX_TYT_GAN_M)).values,
    }
    bang = pd.DataFrame([danh_gia(k, v, thuc_te) for k, v in qt.items()])
    save_csv(bang, TAB / "doi_soat_nq1251_quy_tac.csv", S,
             note="Doi soat quy tac du doan gop tram voi NQ 1251 (17 xa bi gop thanh 8 don vi tu 01/01/2025)")

    du_doan = out.ung_vien_A15_moi.values
    out["phan_loai_nq1251"] = np.select(
        [du_doan & thuc_te, du_doan & ~thuc_te, ~du_doan & thuc_te],
        ["TP - du doan dung", "FP - du doan gop nhung NQ 1251 giu", "FN - NQ 1251 gop nhung quy tac bo sot"],
        default="TN - ca hai deu giu")

    save_csv(out.drop(columns=["ten_xa"]).sort_values("cskcb_2km", ascending=False).round(2),
             TAB / "tyt_cskcb.csv", S,
             note=f"A15: khoang cach va so CSKCB quanh moi TYT theo cap CMKT; ung_vien_A15_moi = >= {A15_NGUONG} CSKCB/{A15_BK // 1000}km")

    g = (out.groupby("nhom_dia_ban")
         .agg(so_tram=("ma_tram", "count"),
              kc_cskcb_gan_nhat_tb_m=("kc_cskcb_gan_nhat_m", "mean"),
              kc_cskcb_gan_nhat_min_m=("kc_cskcb_gan_nhat_m", "min"),
              kc_cskcb_gan_nhat_max_m=("kc_cskcb_gan_nhat_m", "max"),
              cskcb_2km_tb=("cskcb_2km", "mean"), cskcb_2km_max=("cskcb_2km", "max"),
              so_tram_khong_co_cskcb_2km=("cskcb_2km", lambda s: int((s == 0).sum())),
              so_tram_khong_co_cskcb_3km=("cskcb_3km", lambda s: int((s == 0).sum())),
              tyt_gan_nhat_tb_m=("tyt_gan_nhat_m", "mean"), tyt_gan_nhat_min_m=("tyt_gan_nhat_m", "min"),
              dan_phuc_vu_tb=("dan_phuc_vu", "mean"), ty_le_3km_tb=("ty_le_trong_3km_pct", "mean"),
              so_ung_vien_A15_moi=("ung_vien_A15_moi", "sum"))
         .reset_index().round(2))
    save_csv(g, TAB / "tyt_cskcb_theo_nhom.csv", S, note="A15: tong hop CSKCB quanh TYT theo nhom dia ban")

    ct = out[["ma_tram", "ten_day_du", "quan_huyen", "nhom_dia_ban", "dan", "dan_phuc_vu",
              "cskcb_1km", "cskcb_2km", "kc_cskcb_gan_nhat_m", "tyt_gan_nhat_m",
              "xa_bi_gop_nq1251", "ung_vien_A15_moi", "phan_loai_nq1251"]]
    save_csv(ct.sort_values(["phan_loai_nq1251", "cskcb_2km"], ascending=[True, False]).round(2),
             TAB / "doi_soat_nq1251_theo_xa.csv", S,
             note="Phan loai tung xa: quy tac A15 moi vs thuc te NQ 1251")

    uv = out[out.ung_vien_A15_moi].copy()
    uv["trang_thai_nq1251"] = np.where(uv.xa_bi_gop_nq1251,
                                       "Da gop tu 01/01/2025 theo NQ 1251",
                                       "Chua gop - de nghi So Y te ra soat")
    uv["ly_do"] = (uv.cskcb_2km.astype(str) + " CSKCB trong 2 km (trong do "
                   + uv.cap_co_ban_2km.astype(str) + " cap co ban, " + uv.cap_chuyen_sau_2km.astype(str)
                   + " cap chuyen sau); CSKCB gan nhat " + uv.kc_cskcb_gan_nhat_m.astype(int).astype(str)
                   + " m; TYT lan can " + uv.tyt_gan_nhat_m.astype(int).astype(str) + " m")
    save_csv(uv[["ma_tram", "ten_tram", "quan_huyen", "nhom_dia_ban", "dan_phuc_vu",
                 "cskcb_2km", "cap_co_ban_2km", "cap_chuyen_sau_2km", "kc_cskcb_gan_nhat_m",
                 "tyt_gan_nhat", "tyt_gan_nhat_m", "ty_le_trong_3km_pct",
                 "xa_bi_gop_nq1251", "trang_thai_nq1251", "ly_do"]]
             .sort_values("cskcb_2km", ascending=False).round(2),
             TAB / "ung_vien_gop_tram.csv", S,
             note=f"A15 moi: ung vien RA SOAT (khong phai de nghi dong cua); nguong {A15_NGUONG} CSKCB trong {A15_BK // 1000} km")

    # --- log ket qua ---------------------------------------------------------
    for _, r in g.iterrows():
        log.info("Nhom %s: %d tram, CSKCB gan nhat TB %.0f m (min %.0f, max %.0f), TB %.1f CSKCB/2km, %d tram khong co CSKCB nao trong 2 km",
                 r.nhom_dia_ban, r.so_tram, r.kc_cskcb_gan_nhat_tb_m, r.kc_cskcb_gan_nhat_min_m,
                 r.kc_cskcb_gan_nhat_max_m, r.cskcb_2km_tb, r.so_tram_khong_co_cskcb_2km)
    log.info("Trung vi dan vung phuc vu: %.0f nguoi", nguong_dan)
    for _, r in bang.iterrows():
        log.info("Quy tac [%s]: chon %d xa, dung %d, precision %s%%, recall %s%%, ky vong ngau nhien %.1f, p=%s",
                 r.quy_tac, r.so_xa_du_doan, r.dung_TP, r.do_chinh_xac_precision_pct,
                 r.do_bao_phu_recall_pct, r.ky_vong_TP_neu_chon_ngau_nhien, r.p_value_hypergeom)
    for pl, sub in out.groupby("phan_loai_nq1251"):
        log.info("%s (%d xa): %s", pl, len(sub), ", ".join(sub.ten_day_du.tolist()))
    log.info("Ung vien A15 moi: %d/56 tram, trong do %d da bi NQ 1251 gop, %d chua gop",
             len(uv), int(uv.xa_bi_gop_nq1251.sum()), int((~uv.xa_bi_gop_nq1251).sum()))


if __name__ == "__main__":
    main()
