# -*- coding: utf-8 -*-
"""BƯỚC 4 · TỔNG HỢP – phần 4.3 KHUYẾN NGHỊ (Bước 07b, 07c, 07d của luồng dữ liệu), gọi từ b4_tong_hop.py.

4.3a Kịch bản gộp trạm theo NQ 1251: xét mọi cách giữ lại 1 trạm mỗi nhóm gộp (384 tổ hợp), so độ phủ 1/2/3 km.
4.3b Điểm đặt mới: Spark chọn ứng viên (ô đông dân nhất mỗi khối 300 × 300 m bằng row_number trên cửa sổ), driver chạy
     thuật toán tham lam bao phủ tối đa (A10) – bài toán tổ hợp lặp không song song hoá tốt nên chạy trên driver.
4.3c TYT cạnh 30 CSKCB: ma trận 56 × 30 bằng broadcast join; ứng viên RÀ SOÁT gộp trạm (A15) – không phải đề nghị đóng
     trạm (A16); đối chiếu quy tắc với danh sách xã NQ 1251 đã gộp (p-value lạc quan, xem A15).
"""
import itertools  # sinh mọi tổ hợp (kịch bản NQ 1251)

import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu trên driver
from scipy.sparse import csr_matrix  # ma trận thưa (bài toán bao phủ)
from scipy.spatial import cKDTree  # cây láng giềng gần nhất
from scipy.stats import hypergeom  # kiểm định siêu bội (đối chiếu quy tắc A15 với NQ 1251)

from common import BASE_YEAR, MAIN_KM, NQ1251_GROUPS, norm_name, wquantile  # tham số và hàm dùng chung
from hinh_hoc import utm49n  # đổi toạ độ sang mét

S = "b4_khuyen_nghi"  # tên phần ghi trong sổ run_manifest
R = MAIN_KM * 1000  # 3 km đổi ra mét
KMAX = 10  # số điểm đặt mới tối đa xét trong thuật toán tham lam
A15_NGUONG, MIN_KCB_1KM, MAX_TYT_GAN_M = 6, 3, 1500  # ngưỡng A15 mới (>= 6 CSKCB/2 km) và A15 cũ (>= 3/1 km, TYT khác <= 1,5 km)


def chay(nc, d2, d3, g3, g5):  # chạy phần 4.3; d2 = kết quả Bước 2, d3 = kết quả Bước 3, g3/g5 = KPI theo xã / vùng phục vụ (4.1)
    F, W, log = nc.F, nc.Window, nc.log  # công cụ Spark
    B = BASE_YEAR  # năm hiện trạng
    a, tram, tp, cs = d3["a"], d3["tram"], d3["tp"], d3["cs"]  # bảng Silver từ Bước 3

    # ================= 4.3a KỊCH BẢN GỘP TRẠM THEO NQ 1251 (56 -> 47) – Bước 07b =================
    g = d3["o_dan"].select("cell_id", "row", "col", "x_m", "y_m", "lon", "lat", "nut_duong", "kc_ra_duong_m", "ten_day_du", "ma_tram")  # mọi ô có dân
    g = (g.join(d3["fact"].filter(F.col("year") == B).select("cell_id", "pop", "pop60"), "cell_id", "left").fillna(0, ["pop", "pop60"])  # dân 2024 (0 nếu thiếu)
         .join(a.select("cell_id", "dist_m"), "cell_id", "left").orderBy("row", "col").toPandas())  # khoảng cách tới trạm; theo thứ tự hàng/cột
    g["dist_m"] = cKDTree(tp[["x_m", "y_m"]].values).query(g[["x_m", "y_m"]].values)[0]  # khoảng cách cho cả ô không có dân 2024
    w = g["pop"].values  # trọng số dân
    st = tp.copy()  # bản sao bảng trạm
    st["k"] = st.ten_tram.str.replace(r"^Trạm Y tế (phường|xã) ", "", regex=True).map(norm_name)  # tên xã chuẩn hoá từ tên trạm
    k2id = dict(zip(st.k, st.ma_tram))  # tên -> mã trạm
    nhom_gop = [[k2id[norm_name(n)] for n in grp] for grp in NQ1251_GROUPS]  # 8 nhóm mã trạm bị gộp
    co_dinh = [s for s in st.ma_tram if not any(s in grp for grp in nhom_gop)]  # trạm không thuộc nhóm nào
    xy = st.set_index("ma_tram")[["x_m", "y_m"]]  # toạ độ theo mã trạm
    d0 = g.dist_m.values  # khoảng cách hiện trạng (56 trạm)
    kb = []  # kết quả từng tổ hợp
    for combo in itertools.product(*nhom_gop):  # mọi cách giữ lại 1 trạm mỗi nhóm (2x2x2x2x2x3x2x2 = 384)
        d, _ = cKDTree(xy.loc[co_dinh + list(combo)].values).query(g[["x_m", "y_m"]].values)  # khoảng cách khi chỉ còn 47 trạm
        kb.append(dict(giu_lai=";".join(combo), ty_le_1km=100 * w[d <= 1000].sum() / w.sum(), ty_le_2km=100 * w[d <= 2000].sum() / w.sum(),  # % trong 1, 2 km
                       ty_le_3km=100 * w[d <= 3000].sum() / w.sum(), kc_trung_vi_m=wquantile(d, w, 0.5),  # % trong 3 km, trung vị
                       dan_tang_kc=w[d > d0 + 1].sum(), dan_mat_3km=w[(d0 <= 3000) & (d > 3000)].sum(), dan_mat_1km=w[(d0 <= 1000) & (d > 1000)].sum()))  # dân bị ảnh hưởng
    kb = pd.DataFrame(kb)  # bảng tổ hợp
    goc = dict(kich_ban="56 trạm (hiện trạng)", ty_le_1km=100 * w[d0 <= 1000].sum() / w.sum(), ty_le_2km=100 * w[d0 <= 2000].sum() / w.sum(),  # hiện trạng
               ty_le_3km=100 * w[d0 <= 3000].sum() / w.sum(), kc_trung_vi_m=wquantile(d0, w, 0.5), dan_mat_3km=0, dan_mat_1km=0, dan_tang_kc=0, giu_lai="")  # (tiếp)
    tot = kb.sort_values(["ty_le_1km", "kc_trung_vi_m"], ascending=[False, True]).iloc[0].to_dict()  # tổ hợp tốt nhất
    xau = kb.sort_values(["ty_le_1km", "kc_trung_vi_m"], ascending=[True, False]).iloc[0].to_dict()  # tổ hợp kém nhất
    tot["kich_ban"], xau["kich_ban"] = "47 trạm - giữ trạm tốt nhất mỗi nhóm", "47 trạm - giữ trạm kém nhất mỗi nhóm"  # đặt nhãn
    sc = pd.DataFrame([goc, tot, xau])  # 3 kịch bản
    sc["so_to_hop_da_xet"] = [1, len(kb), len(kb)]  # số tổ hợp đã xét
    sc = sc[["kich_ban", "giu_lai", "ty_le_1km", "ty_le_2km", "ty_le_3km", "kc_trung_vi_m", "dan_mat_1km", "dan_mat_3km", "dan_tang_kc", "so_to_hop_da_xet"]].round(2)  # thứ tự cột
    nc.xuat("scenario_nq1251.csv", sc, S, note="kich ban giu 1 tram moi nhom gop NQ 1251")  # vào hàng chờ (Bước 5 ghi)
    nc.gold(nc.tao_df(sc).coalesce(1), "gold/kich_ban_nq1251")  # vào hàng chờ Gold (Bước 5 ghi)

    # ================= 4.3b ĐIỂM ĐẶT MỚI – THAM LAM BAO PHỦ TỐI ĐA (A10) – Bước 07c =================
    uv = (nc.tao_df(g[["cell_id", "row", "col", "x_m", "y_m", "lon", "lat", "ma_tram", "ten_day_du", "pop"]])  # mọi ô có dân
          .withColumn("br", F.floor(F.col("row") / 3)).withColumn("bc", F.floor(F.col("col") / 3))  # khối 300 m x 300 m chứa ô
          .withColumn("_r", F.row_number().over(W.partitionBy("br", "bc").orderBy(F.desc("pop"), "row", "col")))  # xếp hạng ô theo dân trong khối
          .filter("_r = 1").orderBy("br", "bc").toPandas())  # ứng viên = ô đông dân nhất mỗi khối
    phu0 = g.dist_m.values <= R  # ô đã được 56 trạm phủ trong 3 km
    log.info("4. Ung vien: %d; o chua phu 3 km: %d (%.0f nguoi)", len(uv), int((~phu0).sum()), w[~phu0].sum())  # in

    def tham_lam(wv):  # chọn tối đa KMAX ứng viên, mỗi bước chọn ứng viên phủ thêm nhiều dân nhất
        chua = np.where(~phu0)[0]  # chỉ số ô chưa được phủ
        lan_can = cKDTree(g[["x_m", "y_m"]].values[chua]).query_ball_point(uv[["x_m", "y_m"]].values, R)  # ô chưa phủ trong 3 km của từng ứng viên
        hang = np.repeat(np.arange(len(lan_can)), [len(n) for n in lan_can])  # hàng ma trận = ứng viên
        cot = np.concatenate([np.asarray(n, int) for n in lan_can]) if len(hang) else np.array([], int)  # cột = ô chưa phủ
        Mx = csr_matrix((np.ones(len(hang)), (hang, cot)), shape=(len(lan_can), len(chua)))  # ma trận phủ thưa
        wu = wv[chua].astype(float).copy()  # dân của các ô chưa phủ
        chon = []  # kết quả (chỉ số ứng viên, dân phủ thêm)
        for _ in range(KMAX):  # tối đa KMAX vòng
            loi = Mx @ wu  # dân phủ thêm của từng ứng viên
            j = int(loi.argmax())  # ứng viên tốt nhất
            if loi[j] <= 0:  # không còn ai để phủ
                break  # dừng
            chon.append((j, float(loi[j])))  # ghi nhận
            wu[Mx[j].indices] = 0.0  # ô vừa được phủ không tính lại
        return chon  # danh sách lựa chọn

    duong_cong, diem = [], []  # đường cong độ phủ; bảng điểm đề xuất
    for muc_tieu, wv in ((f"Dân {B}", w), (f"Dân 60+ {B}", g["pop60"].values)):  # hai mục tiêu: toàn dân, dân 60+
        chon = tham_lam(wv)  # chạy tham lam
        tong_w = wv.sum()  # tổng trọng số
        cum = wv[phu0].sum()  # đã phủ ban đầu
        duong_cong.append(dict(muc_tieu=muc_tieu, K=0, ty_le_3km_pct=100 * cum / tong_w, dan_moi_duoc_phu=0))  # điểm K = 0
        for kk, (j, loi) in enumerate(chon, 1):  # từng điểm được chọn
            cum += loi  # cộng dồn
            duong_cong.append(dict(muc_tieu=muc_tieu, K=kk, ty_le_3km_pct=100 * cum / tong_w, dan_moi_duoc_phu=loi))  # điểm K = kk
            if muc_tieu == f"Dân {B}":  # chỉ ghi vị trí của mục tiêu toàn dân
                c = uv.iloc[j]  # ứng viên
                diem.append(dict(thu_tu=kk, cell_id=c.cell_id, lon=round(c.lon, 5), lat=round(c.lat, 5), xa=c.ten_day_du,  # dòng điểm đề xuất
                                 dan_moi_duoc_phu=round(loi), ty_le_3km_sau_pct=round(100 * cum / tong_w, 2)))  # (tiếp)
    nc.xuat("greedy_curve.csv", pd.DataFrame(duong_cong).round(3), S, note="do phu 3 km theo so diem moi K (A10)")  # vào hàng chờ (Bước 5 ghi)
    sites = pd.DataFrame(diem)  # bảng điểm đề xuất
    sites["x_m"], sites["y_m"] = utm49n(sites.lon.values, sites.lat.values)  # toạ độ mét
    dd, ii = cKDTree(tp[["x_m", "y_m"]].values).query(sites[["x_m", "y_m"]].values)  # trạm hiện hữu gần nhất
    sites["kc_toi_tram_hien_huu_m"] = dd.round(0)  # khoảng cách tới trạm hiện hữu
    sites["tram_hien_huu_gan_nhat"] = tp.ten_tram.values[ii]  # tên trạm hiện hữu gần nhất
    nc.xuat("greedy_sites.csv", sites, S, note="diem dat moi de xuat (tham lam, R = 3 km, A10)")  # vào hàng chờ (Bước 5 ghi)
    nc.gold(nc.tao_df(sites).coalesce(1), "gold/fact_optimal_facility_placement")  # vào hàng chờ Gold (Bước 5 ghi)
    ba = []  # trước / sau khi thêm K điểm
    for K in (0, 1, 3, 5, 10):  # các mức K
        if K > len(sites):  # không đủ điểm
            continue  # bỏ qua
        pts = np.vstack([tp[["x_m", "y_m"]].values] + ([sites[["x_m", "y_m"]].values[:K]] if K else []))  # trạm hiện có + K điểm mới
        d, _ = cKDTree(pts).query(g[["x_m", "y_m"]].values)  # khoảng cách chim bay mới
        w60 = g["pop60"].values  # dân 60+
        kc_n, _, _ = d3["do_thi"].dijkstra(pts, "dai")  # Dijkstra với bộ trạm mới
        dn = kc_n[g.nut_duong.values] + g.kc_ra_duong_m.values  # khoảng cách đường đi mới của từng ô
        ok = np.isfinite(dn)  # ô tới được
        ba.append(dict(K=K, so_tram=len(pts), ty_le_2km=100 * w[d <= 2000].sum() / w.sum(), ty_le_3km=100 * w[d <= 3000].sum() / w.sum(),  # dòng kết quả
                       ty_le_5km=100 * w[d <= 5000].sum() / w.sum(), ty_le60_3km=100 * w60[d <= 3000].sum() / w60.sum(),  # (tiếp)
                       dan_tren_5km=w[d > 5000].sum(), dan60_tren_5km=w60[d > 5000].sum(), kc_trung_vi_m=wquantile(d, w, 0.5),  # (tiếp)
                       ty_le_3km_mang_duong=100 * w[ok & (dn <= 3000)].sum() / w.sum(), ty_le_5km_mang_duong=100 * w[ok & (dn <= 5000)].sum() / w.sum()))  # (tiếp)
    ba = pd.DataFrame(ba).round(2)  # bảng trước/sau
    nc.xuat("greedy_before_after.csv", ba, S, note="truoc / sau khi them K diem de xuat")  # vào hàng chờ (Bước 5 ghi)
    for _, r in ba.iterrows():  # in
        log.info("4. K=%d: 3 km %.2f%%, 5 km %.2f%%, dan > 5 km %.0f", r.K, r.ty_le_3km, r.ty_le_5km, r.dan_tren_5km)  # dòng log

    # ================= 4.3c TYT VÀ CƠ SỞ KHÁM CHỮA BỆNH (A15, A16, A17) – Bước 07d =================
    tc = (tram.select("ma_tram", "x_m", "y_m")  # 56 TYT
          .crossJoin(F.broadcast(cs.select("ma_cs", "ten_cs", "tuyen", "cap", F.col("x_m").alias("cx"), F.col("y_m").alias("cy"))))  # x 30 CSKCB (broadcast)
          .withColumn("d", F.sqrt((F.col("x_m") - F.col("cx")) ** 2 + (F.col("y_m") - F.col("cy")) ** 2)))  # ma trận khoảng cách TYT - CSKCB
    nc.gold(tc.select("ma_tram", "ma_cs", "cap", F.col("d").alias("kc_m")), "gold/fact_khoang_cach_tyt_cskcb")  # vào hàng chờ Gold (Bước 5 ghi) ma trận 56 x 30
    dem = tc.groupBy("ma_tram").agg(  # tổng hợp quanh từng TYT
        F.min(F.struct("d", "ten_cs", "tuyen", "cap")).alias("m"),  # CSKCB gần nhất
        *[F.sum(F.when(F.col("d") <= r, 1).otherwise(0)).alias(f"cskcb_{r // 1000}km") for r in (1000, 2000, 3000, 5000)],  # số CSKCB trong 1/2/3/5 km
        *[F.sum(F.when((F.col("d") <= 2000) & (F.col("cap") == c), 1).otherwise(0)).alias(f"cap_{h}_2km")  # số CSKCB từng cấp trong 2 km
          for c, h in (("Cấp cơ bản", "co_ban"), ("Cấp chuyên sâu", "chuyen_sau"), ("Cấp ban đầu", "ban_dau"))],  # (tiếp)
        F.sum(F.when((F.col("d") <= 5000) & (F.col("cap") == "Cấp chuyên sâu"), 1).otherwise(0)).alias("cap_chuyen_sau_5km"))  # chuyên sâu trong 5 km
    tt = (tram.select("ma_tram", "x_m", "y_m").alias("p").crossJoin(tram.select("ma_tram", "ten_tram", "x_m", "y_m").alias("q"))  # TYT x TYT
          .filter(F.col("p.ma_tram") != F.col("q.ma_tram"))  # bỏ cặp trùng chính nó
          .withColumn("d", F.sqrt((F.col("p.x_m") - F.col("q.x_m")) ** 2 + (F.col("p.y_m") - F.col("q.y_m")) ** 2))  # khoảng cách
          .groupBy(F.col("p.ma_tram").alias("ma_tram")).agg(F.min(F.struct("d", F.col("q.ten_tram").alias("ten"))).alias("t")))  # TYT khác gần nhất
    o = (tram.select("ma_tram", "ten_tram", "lon", "lat").join(dem, "ma_tram").join(tt, "ma_tram")  # ghép
         .select("ma_tram", "ten_tram", "lon", "lat", F.col("t.ten").alias("tyt_gan_nhat"), F.round("t.d").alias("tyt_gan_nhat_m"),  # cột kết quả
                 F.col("m.ten_cs").alias("cskcb_gan_nhat"), F.col("m.tuyen").alias("cskcb_gan_nhat_tuyen"), F.col("m.cap").alias("cskcb_gan_nhat_cap"),  # (tiếp)
                 F.round("m.d").alias("kc_cskcb_gan_nhat_m"), "cskcb_1km", "cskcb_2km", "cskcb_3km", "cskcb_5km",  # (tiếp)
                 "cap_co_ban_2km", "cap_chuyen_sau_2km", "cap_ban_dau_2km", "cap_chuyen_sau_5km").toPandas())  # (tiếp) -> pandas
    o = (o.merge(g3[["ma_tram", "ten_day_du", "quan_huyen", "nhom", "dan", "ty_le_trong_3km_pct", "ty_le_trong_3km_mang_duong_pct"]], on="ma_tram", how="left")  # + KPI xã
         .merge(g5[["ma_tram", "dan_phuc_vu", "dan60_phuc_vu"]], on="ma_tram", how="left"))  # + dân vùng phục vụ
    nq_xa = {x for grp in NQ1251_GROUPS for x in grp}  # 17 xã bị gộp theo NQ 1251
    tu_file = set()  # tên xã gộp đọc từ file tham chiếu
    for s_ in d2["nq_sx"][pd.to_numeric(d2["nq_sx"].so_xa_nhap) >= 2].cac_xa_nhap_vao:  # các dòng gộp từ 2 xã trở lên
        tu_file.update(x.strip() for x in str(s_).split("+"))  # tách tên xã
    if tu_file == nq_xa:  # danh sách trong code khớp file tham chiếu
        log.info("4. Doi chieu NQ 1251: %d xa gop thanh %d don vi - code va file tham chieu KHOP", len(nq_xa), len(NQ1251_GROUPS))  # in
    else:  # lệch
        log.warning("LECH: danh sach xa gop trong code vs file tham chieu: %s", tu_file.symmetric_difference(nq_xa))  # cảnh báo
    nc.kiem("Danh sach xa gop NQ 1251: code vs data/reference/nq1251_sap_xep.csv", len(tu_file), len(nq_xa), tu_file == nq_xa)  # đối soát
    o["ten_xa"] = o.ten_day_du.str.replace("Phường |Xã ", "", regex=True)  # tên xã bỏ tiền tố
    o["xa_bi_gop_nq1251"] = o.ten_xa.isin(nq_xa)  # xã có bị gộp không
    o["nhom_dia_ban"] = np.where(o.quan_huyen.str.contains("Hòa Vang"), "Huyện Hòa Vang",  # 3 nhóm địa bàn theo quận
                                 np.where(o.quan_huyen.isin(["Quận Hải Châu", "Quận Thanh Khê"]), "Quận lõi (Hải Châu, Thanh Khê)", "Quận ven"))  # (tiếp)
    nguong_dan = o.dan_phuc_vu.median()  # trung vị dân vùng phục vụ
    o["ung_vien_A15_moi"] = o.cskcb_2km >= A15_NGUONG  # A15 mới: >= 6 CSKCB trong 2 km
    o["ung_vien_A15_cu"] = (o.cskcb_1km >= MIN_KCB_1KM) & (o.dan_phuc_vu < nguong_dan) & (o.tyt_gan_nhat_m <= MAX_TYT_GAN_M)  # A15 cũ

    def danh_gia(ten, du_doan, thuc_te, N=56):  # bảng nhầm lẫn + kiểm định siêu bội cho một quy tắc
        tp_ = int((du_doan & thuc_te).sum()); fp = int((du_doan & ~thuc_te).sum())  # đúng dương, báo động giả
        fn = int((~du_doan & thuc_te).sum()); tn = int((~du_doan & ~thuc_te).sum())  # bỏ sót, đúng âm
        kd, K_ = tp_ + fp, int(thuc_te.sum())  # số xã dự đoán, số xã thực sự gộp
        pr = tp_ / kd if kd else np.nan  # độ chính xác
        rc = tp_ / K_ if K_ else np.nan  # độ bao phủ
        f1 = 2 * pr * rc / (pr + rc) if kd and tp_ else 0.0  # F1
        return {"quy_tac": ten, "so_xa_du_doan": kd, "so_xa_NQ1251_gop": K_, "dung_TP": tp_, "bao_dong_gia_FP": fp, "bo_sot_FN": fn,  # dòng kết quả
                "dung_am_TN": tn, "do_chinh_xac_precision_pct": round(100 * pr, 1) if kd else np.nan,  # (tiếp)
                "do_bao_phu_recall_pct": round(100 * rc, 1) if K_ else np.nan, "f1_pct": round(100 * f1, 1),  # (tiếp)
                "ky_vong_TP_neu_chon_ngau_nhien": round(kd * K_ / N, 1),  # (tiếp)
                "p_value_hypergeom": f"{hypergeom.sf(tp_ - 1, N, K_, kd):.3g}" if kd else "n/a"}  # (tiếp) xác suất trùng ngẫu nhiên
    thuc = o.xa_bi_gop_nq1251.values  # nhãn thực tế
    quy_tac = {f"A15 moi: >= {A15_NGUONG} CSKCB trong 2 km": o.ung_vien_A15_moi.values,  # 5 quy tắc đem so
               f"A15 cu: >= {MIN_KCB_1KM} CSKCB/1km + dan < trung vi + TYT khac <= {MAX_TYT_GAN_M} m": o.ung_vien_A15_cu.values,  # (tiếp)
               f"Chi >= {MIN_KCB_1KM} CSKCB trong 1 km": (o.cskcb_1km >= MIN_KCB_1KM).values,  # (tiếp)
               "CSKCB gan nhat <= 1000 m": (o.kc_cskcb_gan_nhat_m <= 1000).values,  # (tiếp)
               f">= {A15_NGUONG} CSKCB/2km VA TYT khac <= {MAX_TYT_GAN_M} m": ((o.cskcb_2km >= A15_NGUONG) & (o.tyt_gan_nhat_m <= MAX_TYT_GAN_M)).values}  # (tiếp)
    bang_qt = pd.DataFrame([danh_gia(k_, v_, thuc) for k_, v_ in quy_tac.items()])  # bảng đối soát quy tắc
    nc.xuat("doi_soat_nq1251_quy_tac.csv", bang_qt, S, note="p-value lac quan vi nguong chon sau khi biet NQ 1251 (A15)")  # vào hàng chờ (Bước 5 ghi)
    dd_ = o.ung_vien_A15_moi.values  # dự đoán theo A15 mới
    o["phan_loai_nq1251"] = np.select([dd_ & thuc, dd_ & ~thuc, ~dd_ & thuc],  # phân loại từng xã
                                      ["TP - du doan dung", "FP - du doan gop nhung NQ 1251 giu", "FN - NQ 1251 gop nhung quy tac bo sot"],  # (tiếp)
                                      default="TN - ca hai deu giu")  # (tiếp)
    nc.xuat("tyt_cskcb.csv", o.drop(columns=["ten_xa"]).sort_values("cskcb_2km", ascending=False).round(2), S,  # vào hàng chờ (Bước 5 ghi)
             note=f"khoang cach va so CSKCB quanh moi TYT; ung_vien_A15_moi = >= {A15_NGUONG} CSKCB/2km (danh sach ra soat)")  # (tiếp)
    nc.gold(nc.tao_df(o.drop(columns=["ten_xa"])).coalesce(1), "gold/tyt_cskcb")  # vào hàng chờ Gold (Bước 5 ghi)
    gg = (o.groupby("nhom_dia_ban").agg(so_tram=("ma_tram", "count"), kc_cskcb_gan_nhat_tb_m=("kc_cskcb_gan_nhat_m", "mean"),  # tổng hợp theo nhóm địa bàn
                                        kc_cskcb_gan_nhat_min_m=("kc_cskcb_gan_nhat_m", "min"), kc_cskcb_gan_nhat_max_m=("kc_cskcb_gan_nhat_m", "max"),  # (tiếp)
                                        cskcb_2km_tb=("cskcb_2km", "mean"), cskcb_2km_max=("cskcb_2km", "max"),  # (tiếp)
                                        so_tram_khong_co_cskcb_2km=("cskcb_2km", lambda s: int((s == 0).sum())),  # (tiếp)
                                        so_tram_khong_co_cskcb_3km=("cskcb_3km", lambda s: int((s == 0).sum())),  # (tiếp)
                                        tyt_gan_nhat_tb_m=("tyt_gan_nhat_m", "mean"), tyt_gan_nhat_min_m=("tyt_gan_nhat_m", "min"),  # (tiếp)
                                        dan_phuc_vu_tb=("dan_phuc_vu", "mean"), ty_le_3km_tb=("ty_le_trong_3km_pct", "mean"),  # (tiếp)
                                        so_ung_vien_A15_moi=("ung_vien_A15_moi", "sum")).reset_index().round(2))  # (tiếp)
    nc.xuat("tyt_cskcb_theo_nhom.csv", gg, S, note="CSKCB quanh TYT theo nhom dia ban (A15, A17)")  # vào hàng chờ (Bước 5 ghi)
    nc.xuat("doi_soat_nq1251_theo_xa.csv", o[["ma_tram", "ten_day_du", "quan_huyen", "nhom_dia_ban", "dan", "dan_phuc_vu", "cskcb_1km", "cskcb_2km", "kc_cskcb_gan_nhat_m",  # bảng theo xã
                "tyt_gan_nhat_m", "xa_bi_gop_nq1251", "ung_vien_A15_moi", "phan_loai_nq1251"]]  # (tiếp)
             .sort_values(["phan_loai_nq1251", "cskcb_2km"], ascending=[True, False]).round(2), S,  # (tiếp)
             note="phan loai tung xa: quy tac A15 moi vs NQ 1251")  # (tiếp)
    u = o[o.ung_vien_A15_moi].copy()  # ứng viên rà soát
    u["trang_thai_nq1251"] = np.where(u.xa_bi_gop_nq1251, "Da gop tu 01/01/2025 theo NQ 1251", "Chua gop - de nghi So Y te ra soat")  # trạng thái
    u["ly_do"] = (u.cskcb_2km.astype(str) + " CSKCB trong 2 km (trong do " + u.cap_co_ban_2km.astype(str) + " cap co ban, "  # lý do dạng chữ
                  + u.cap_chuyen_sau_2km.astype(str) + " cap chuyen sau); CSKCB gan nhat " + u.kc_cskcb_gan_nhat_m.astype(int).astype(str)  # (tiếp)
                  + " m; TYT lan can " + u.tyt_gan_nhat_m.astype(int).astype(str) + " m")  # (tiếp)
    nc.xuat("ung_vien_gop_tram.csv", u[["ma_tram", "ten_tram", "quan_huyen", "nhom_dia_ban", "dan_phuc_vu", "cskcb_2km", "cap_co_ban_2km", "cap_chuyen_sau_2km",  # vào hàng chờ (Bước 5 ghi)
                "kc_cskcb_gan_nhat_m", "tyt_gan_nhat", "tyt_gan_nhat_m", "ty_le_trong_3km_pct", "xa_bi_gop_nq1251", "trang_thai_nq1251", "ly_do"]]  # (tiếp)
             .sort_values("cskcb_2km", ascending=False).round(2), S,  # (tiếp)
             note=f"ung vien RA SOAT (khong phai de nghi dong cua); nguong {A15_NGUONG} CSKCB trong 2 km (A15, A16)")  # (tiếp)
    log.info("4. Ung vien A15: %d/56 tram, %d da bi NQ 1251 gop, %d chua gop: %s", len(u), int(u.xa_bi_gop_nq1251.sum()),  # in
             int((~u.xa_bi_gop_nq1251).sum()), ", ".join(u[~u.xa_bi_gop_nq1251].ten_tram))  # (tiếp)

    return dict(g=g, sites=sites)  # trả cho 4.4 dự báo và Bước 5
