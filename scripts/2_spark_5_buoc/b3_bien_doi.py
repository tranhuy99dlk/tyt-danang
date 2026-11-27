# -*- coding: utf-8 -*-
"""BƯỚC 3 · BIẾN ĐỔI (Transform) – khung 5 bước của pipeline Spark (nội dung nghiệp vụ: Bước 03 + 04 + 05 của luồng dữ liệu).

3.1 Chuẩn hoá: giữ ô 100 m có dân (> 0 ở ít nhất một năm) trong 56 xã; ghép dân số với ô; tính dân 60+ từng ô theo
    tỷ lệ 60+ của ô 1 km chứa nó (A3); toạ độ trạm/CSKCB đổi sang UTM 49N (mét); phân loại xã theo địa hình (A7).
    -> silver/dim_o_luoi (chia thư mục theo ma_quan), silver/fact_dan_so_o_nam (theo year, ma_quan),
       silver/dim_tram, silver/dim_cskcb, silver/dim_xa
3.2 Nối đa nguồn – khoảng cách đường chim bay: nối chéo ô có dân với 56 TYT và 30 CSKCB bằng BROADCAST (bảng trạm rất nhỏ
    được gửi nguyên bản tới mọi executor, bảng ô lớn không bị shuffle), lấy trạm gần nhất.
3.3 Nối đa nguồn – khoảng cách đường đi: chạy Dijkstra nhiều nguồn trên đồ thị đường OSM (thư viện scipy, trên driver, vì
    Spark không có sẵn thuật toán đường đi ngắn nhất) cho năm hiện trạng 2024, rồi JOIN kết quả theo nút đường vào ô.
3.4 Cột nghiệp vụ: dist_m, net_dist_m, net_time_min, detour (hệ số vòng), nhom (phường / xã đồng bằng / xã miền núi), pop60.
    -> silver/o_luoi_tiep_can_2024 (1 dòng / ô, đủ mọi khoảng cách), silver/fact_o_nam_khoang_cach
Bảng Silver được GHI NGAY rồi đọc lại (NguCanh.luu) như một điểm lưu: Bước 4 tính từ Parquet đã lưu, không phải tính lại cả
chuỗi từ CSV thô; nếu chạy hỏng giữa chừng thì dữ liệu sạch đã lưu vẫn còn. Bảng Gold và bảng CSV báo cáo do Bước 5 ghi.
"""
import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu trên driver
from scipy.sparse import coo_matrix  # ma trận thưa biểu diễn đồ thị
from scipy.sparse.csgraph import dijkstra  # thuật toán đường đi ngắn nhất
from scipy.spatial import cKDTree  # cây tìm láng giềng gần nhất

from common import BASE_YEAR, YEARS, WALK_KMH  # tham số dùng chung
from hinh_hoc import cac_vong, kc_toi_bien, trong_da_giac, utm49n, utm49n_cap  # hàm hình học numpy

S = "b3_bien_doi"  # tên bước ghi trong sổ run_manifest
N_PHUONG, N_NUI, N_DB = "Phường", "Xã miền núi (địa hình)", "Xã đồng bằng/ven đô"  # tên 3 nhóm địa bàn (A7)
NGUON_NHAP_TAY = "NHAP TAY (data/raw/location-tyt.xlsx)"  # nhãn nguồn toạ độ trạm nhập tay (A13)
DI_BO = WALK_KMH / 3.6  # tốc độ đi bộ (m/giây)


class DoThi:  # đồ thị đường trên driver: dùng lại ở Bước 4 (đánh giá điểm đề xuất)
    def __init__(self, nut_pd, canh_pd):  # nhận bảng nút và bảng cạnh (pandas)
        self.n = int(nut_pd.nut.max()) + 1  # số nút (đánh số 0..n-1)
        self.x = np.full(self.n, np.nan)  # toạ độ x theo chỉ số nút
        self.y = np.full(self.n, np.nan)  # toạ độ y theo chỉ số nút
        self.x[nut_pd.nut.values] = nut_pd.x_m.values  # điền toạ độ x
        self.y[nut_pd.nut.values] = nut_pd.y_m.values  # điền toạ độ y
        co = np.isfinite(self.x)  # nút có toạ độ (nút bị loại ở Bước 2 thì không)
        self.ids = np.where(co)[0]  # chỉ số các nút có toạ độ
        self.cay = cKDTree(np.c_[self.x[co], self.y[co]])  # cây tìm nút gần nhất
        self.i, self.j = canh_pd.i.values, canh_pd.j.values  # đầu và cuối của mỗi cạnh
        self.w = {"dai": canh_pd.dai_m.values, "phut": canh_pd.phut.values}  # hai loại trọng số: mét và phút

    def nut_gan(self, xy):  # nút đường gần nhất của các điểm (x, y)
        d, k = self.cay.query(xy)  # khoảng cách và vị trí trong cây
        return d, self.ids[k]  # đổi vị trí về chỉ số nút thật

    def dijkstra(self, xy, trong_so="dai"):  # Dijkstra nhiều nguồn: mỗi nút -> nguồn gần nhất (theo mét hoặc phút)
        ds, ns = self.nut_gan(xy)  # nút gần nhất của từng nguồn (trạm) và khoảng cách đi bộ ra đó
        S_ = len(xy)  # số nguồn
        off = ds if trong_so == "dai" else ds / DI_BO / 60  # chi phí đoạn đi bộ từ trạm ra đường (mét hoặc phút)
        hang = np.r_[self.i, self.j, self.n + np.arange(S_), ns]  # hàng của ma trận: cạnh hai chiều + cạnh nguồn ảo -> nút
        cot = np.r_[self.j, self.i, ns, self.n + np.arange(S_)]  # cột tương ứng
        val = np.r_[self.w[trong_so], self.w[trong_so], off + 1e-6, off + 1e-6]  # trọng số (cộng số rất nhỏ để không bằng 0)
        G = coo_matrix((val, (hang, cot)), shape=(self.n + S_, self.n + S_)).tocsr()  # ma trận kề dạng thưa
        kc, _, nguon = dijkstra(G, directed=False, indices=self.n + np.arange(S_), min_only=True, return_predecessors=True)  # chạy Dijkstra từ mọi nguồn ảo
        return kc[:self.n], nguon[:self.n] - self.n, ds  # chi phí tới từng nút, chỉ số nguồn gần nhất, khoảng cách trạm ra đường


def chay(nc, d1):  # chạy Bước 3; d1 = kết quả Bước 2 (bảng sạch)
    F, T, log = nc.F, nc.T, nc.log  # công cụ Spark
    D = T.DoubleType()  # kiểu số thực
    B = BASE_YEAR  # năm hiện trạng
    dm, o_luoi, pop, k1 = d1["dm"], d1["o_luoi"], d1["pop"], d1["k1"]  # bảng sạch từ Bước 2

    # ================= 3.1 CHUẨN HOÁ (Bước 03 của luồng dữ liệu) =================
    co_dan = pop.groupBy("row", "col").agg(F.max("pop").alias("_mx")).filter("_mx > 0").select("row", "col")  # pixel có dân > 0 ở ít nhất một năm
    o_dan = (o_luoi.join(co_dan, ["row", "col"])  # chỉ giữ ô có dân (ô toàn 0 bị bỏ khỏi phân tích, không phải lỗi)
             .join(F.broadcast(dm.select("ma_tram", "ma_quan", "quan_huyen", "ten_day_du", "loai_dv")), "ma_tram"))  # gắn quận, tên xã
    o_dan = nc.luu(o_dan, "silver/dim_o_luoi", "ma_quan")  # ghi Silver chia thư mục theo quận rồi đọc lại
    n_o = o_dan.count()  # số ô có dân
    n_dong_o_0 = pop.join(co_dan, ["row", "col"], "left_anti").count()  # số dòng dân số sạch thuộc ô toàn 0
    log.info("3. O co dan trong 56 xa: %d; dong dan so thuoc o toan 0 (bo khoi phan tich): %d", n_o, n_dong_o_0)  # in

    # T1: tỷ lệ 60+ của ô 1 km -> dân 60+ của ô 100 m (A3)
    tuoi = (k1.filter(F.col("year").between(YEARS[0], YEARS[-1]))  # chỉ các năm phân tích
            .groupBy("year", "k1_row", "k1_col")  # gom theo ô 1 km x năm (phép biến đổi rộng: có shuffle)
            .agg(F.sum(F.when(F.col("bien") == "pop", F.col("gia_tri"))).alias("pop_1km"),  # tổng dân ô 1 km
                 F.sum(F.when(F.col("bien") != "pop", F.col("gia_tri"))).alias("pop60_1km"))  # tổng 7 nhóm tuổi = dân 60+
            .cache())  # giữ trong bộ nhớ
    tuoi_B = tuoi.filter(F.col("year") == B).select("k1_row", "k1_col", F.col("pop_1km").alias("popB"), F.col("pop60_1km").alias("pop60B"))  # năm hiện trạng
    ob = o_dan.join(tuoi_B, ["k1_row", "k1_col"], "left").fillna(0, ["popB", "pop60B"])  # gắn số liệu 1 km năm hiện trạng cho từng ô
    ty_le_xa = ob.filter("popB > 0").groupBy("ma_tram").agg((F.sum("pop60B") / F.sum("popB")).alias("share_xa"))  # tỷ lệ 60+ trung bình xã (dùng khi ô 1 km = 0)
    ob = (ob.join(F.broadcast(ty_le_xa), "ma_tram", "left")  # gắn tỷ lệ trung bình xã
          .withColumn("share_base", F.when(F.col("popB") > 0, F.col("pop60B") / F.col("popB")).otherwise(F.col("share_xa"))))  # tỷ lệ 60+ gốc của ô
    fact = (pop.join(ob.select("row", "col", "cell_id", "ma_tram", "ma_quan", "k1_row", "k1_col", "share_base"), ["row", "col"])  # dân số ô x năm + thông tin ô
            .join(tuoi, ["year", "k1_row", "k1_col"], "left").fillna(0, ["pop_1km", "pop60_1km"])  # gắn số liệu 1 km cùng năm
            .withColumn("share60", F.when(F.col("pop_1km") > 0, F.col("pop60_1km") / F.col("pop_1km")).otherwise(F.col("share_base")))  # tỷ lệ 60+ năm đó
            .withColumn("pop60", F.col("pop") * F.col("share60"))  # dân 60+ = dân x tỷ lệ (phép biến đổi hẹp)
            .select("cell_id", "ma_tram", "ma_quan", "year", "pop", "pop60", "share60"))  # cột của bảng sự kiện
    n_fact_truoc = fact.count()  # số dòng trước khi ghi
    fact = nc.luu(fact.repartition("year"), "silver/fact_dan_so_o_nam", ["year", "ma_quan"])  # ghi Silver chia thư mục theo năm và quận
    n_fact = fact.count()  # số dòng đọc lại
    nc.kiem("Dan so sach = dong bang su kien + dong thuoc o toan 0", n_fact + n_dong_o_0, d1["n_pop_sach"], n_fact + n_dong_o_0 == d1["n_pop_sach"])  # đối soát
    nc.kiem("So dong bang su kien doc lai = truoc khi ghi", n_fact, n_fact_truoc, n_fact == n_fact_truoc, "silver/fact_dan_so_o_nam")  # đối soát
    s_fact = float(fact.agg(F.sum("pop")).first()[0])  # tổng dân bảng sự kiện
    nc.kiem("Tong dan bang su kien = tong dan sach", round(s_fact), round(d1["s_sach"]), abs(s_fact - d1["s_sach"]) <= 1e-6 * d1["s_sach"])  # đối soát

    # Trạm y tế: đổi toạ độ sang UTM 49N; trạm thiếu toạ độ hợp lệ -> tâm dân cư của xã (A5)
    f_utm = F.udf(utm49n_cap, T.StructType([T.StructField("x", D), T.StructField("y", D)]))  # UDF đổi toạ độ
    co_B = fact.filter(F.col("year") == B).join(o_dan.select("cell_id", "lon", "lat", "x_m", "y_m"), "cell_id")  # dân 2024 của từng ô kèm toạ độ
    tam = co_B.withColumn("w", F.col("pop") + 1e-9).groupBy("ma_tram").agg(  # tâm dân cư từng xã (trọng số = dân 2024)
        (F.sum(F.col("x_m") * F.col("w")) / F.sum("w")).alias("cx"), (F.sum(F.col("y_m") * F.col("w")) / F.sum("w")).alias("cy"),  # tâm theo mét
        (F.sum(F.col("lon") * F.col("pop")) / F.sum("pop")).alias("lon_tam"), (F.sum(F.col("lat") * F.col("pop")) / F.sum("pop")).alias("lat_tam"))  # tâm theo độ
    tram = d1["tyt"].withColumn("nguon_toa_do", F.lit(NGUON_NHAP_TAY)).withColumn("uoc_luong", F.lit(False))  # trạm có toạ độ nhập tay hợp lệ
    thieu = dm.join(tram.select("ma_tram"), "ma_tram", "left_anti")  # trạm trong danh mục nhưng không có toạ độ hợp lệ
    if thieu.count():  # có trạm thiếu
        tram_uoc = (thieu.join(tam, "ma_tram").select("ma_tram", "ten_tram", "quan_huyen", "loai_dv", "ten_day_du",  # dùng tâm dân cư làm vị trí ước lượng
                                                       F.col("lon_tam").alias("lon"), F.col("lat_tam").alias("lat"),  # (tiếp)
                                                       F.lit(None).cast("int").alias("dong_excel"),  # không có dòng Excel
                                                       F.lit("UOC LUONG: tam dan cu xa (A5)").alias("nguon_toa_do"), F.lit(True).alias("uoc_luong")))  # (tiếp) nhãn
        log.warning("CAN XEM: %d tram khong co toa do hop le -> dung tam dan cu xa (A5)", thieu.count())  # cảnh báo
        tram = tram.unionByName(tram_uoc)  # ghép vào danh sách trạm
    tram = (tram.withColumn("_xy", f_utm("lon", "lat")).withColumn("x_m", F.col("_xy.x")).withColumn("y_m", F.col("_xy.y")).drop("_xy")  # toạ độ UTM
            .join(tam.select("ma_tram", "cx", "cy"), "ma_tram", "left")  # gắn tâm dân cư
            .withColumn("kc_tram_tam_dan_cu_m", F.round(F.sqrt((F.col("x_m") - F.col("cx")) ** 2 + (F.col("y_m") - F.col("cy")) ** 2)))  # khoảng cách trạm - tâm dân cư
            .drop("cx", "cy"))  # bỏ cột phụ
    # kiểm tra hình học trên driver (56 trạm): trạm có nằm trong ranh giới xã của chính nó, cách biên bao xa
    gj = d1["ranh_gioi"]  # GeoJSON ranh giới 56 xã (Bước 1 đã đọc và phân tích)
    vong = {f["properties"]["ma_tram"]: cac_vong(f["geometry"]) for f in gj["features"]}  # các vòng toạ độ (độ) theo xã
    vong_m = {k: [np.column_stack(utm49n(v[:, 0], v[:, 1])) for v in vs] for k, vs in vong.items()}  # các vòng đổi sang mét
    tp = tram.toPandas()  # 56 dòng -> pandas
    tp["trong_ranh_gioi_xa"] = [trong_da_giac(r.lon, r.lat, vong.get(r.ma_tram, [])) for r in tp.itertuples()]  # điểm trong đa giác?
    tp["kc_ngoai_ranh_gioi_m"] = [0.0 if r.trong_ranh_gioi_xa else round(kc_toi_bien(r.x_m, r.y_m, vong_m.get(r.ma_tram, [])), 1)  # nằm ngoài: cách biên bao xa (m)
                                  for r in tp.itertuples()]  # (tiếp)
    xy = tp[["x_m", "y_m"]].values  # toạ độ 56 trạm
    dd = np.hypot(xy[:, None, 0] - xy[None, :, 0], xy[:, None, 1] - xy[None, :, 1])  # ma trận khoảng cách trạm - trạm
    np.fill_diagonal(dd, np.inf)  # bỏ khoảng cách với chính nó
    tp["cap_trung_toa_do_50m"] = (dd < 50).any(1).astype(int)  # trạm có trạm khác cách dưới 50 m (nghi trùng toạ độ)
    tp = tp.sort_values("ma_tram").reset_index(drop=True)  # sắp theo mã trạm
    tram = nc.luu(nc.tao_df(tp), "silver/dim_tram")  # ghi Silver và đọc lại
    n_tram = tram.count()  # số trạm
    log.info("3. Tram: %d (nhap tay %d, uoc luong %d); ngoai ranh gioi xa: %d; cach tam dan cu > 3 km: %d", n_tram,  # in thống kê
             int((~tp.uoc_luong).sum()), int(tp.uoc_luong.sum()), int((~tp.trong_ranh_gioi_xa).sum()), int((tp.kc_tram_tam_dan_cu_m > 3000).sum()))  # (tiếp)
    try:  # nếu máy có pyproj (chạy trên máy) thì đối chiếu công thức UTM tự viết với pyproj
        from pyproj import Transformer  # thư viện đổi toạ độ chuẩn
        px, py = Transformer.from_crs("EPSG:4326", "EPSG:32649", always_xy=True).transform(tp.lon.values, tp.lat.values)  # toạ độ theo pyproj
        lech = float(np.hypot(px - tp.x_m.values, py - tp.y_m.values).max())  # lệch lớn nhất (m)
        nc.kiem("Toa do tram UTM: cong thuc tu viet vs pyproj, lech lon nhat (m)", round(lech, 4), 0.0, lech <= 0.01)  # đối soát
    except ImportError:  # trong container không có pyproj
        log.info("3. Khong co pyproj -> bo qua doi chieu UTM")  # bỏ qua

    # CSKCB: đổi toạ độ sang UTM
    cs = (d1["cskcb"].withColumn("_xy", f_utm("lon", "lat")).withColumn("x_m", F.col("_xy.x")).withColumn("y_m", F.col("_xy.y")).drop("_xy"))  # thêm toạ độ mét
    cs = nc.luu(cs, "silver/dim_cskcb")  # ghi Silver
    cs_pd = cs.orderBy("ma_cs").toPandas()  # 30 dòng -> pandas
    nc.xuat("cskcb_danang.csv", cs_pd[["ma_cs", "ten_cs", "tuyen", "hang", "cap", "diem", "dia_chi", "lat", "lon"]].round(6), S,  # danh sách CSKCB đã làm sạch vào hàng chờ
             note="CSKCB So Y te da kiem tra (tuyen, hang, cap CMKT, toa do) (A17)")  # (tiếp)

    # ================= 3.2 NỐI ĐA NGUỒN: KHOẢNG CÁCH ĐƯỜNG CHIM BAY (BROADCAST JOIN, Bước 04) =================
    def gan_nhat(diem, ma, ten_kc, ten_ma):  # mỗi ô -> điểm gần nhất trong `diem` (bảng nhỏ)
        return (o_dan.select("cell_id", "x_m", "y_m")  # bảng ô (lớn)
                .crossJoin(F.broadcast(diem.select(F.col(ma).alias("_ma"), F.col("x_m").alias("sx"), F.col("y_m").alias("sy"))))  # nối chéo với bảng nhỏ đã broadcast
                .withColumn("d", F.sqrt((F.col("x_m") - F.col("sx")) ** 2 + (F.col("y_m") - F.col("sy")) ** 2))  # khoảng cách Euclid 2D (mét)
                .groupBy("cell_id").agg(F.min(F.struct("d", "_ma")).alias("m"))  # giữ cặp có khoảng cách nhỏ nhất của mỗi ô
                .select("cell_id", F.col("m.d").alias(ten_kc), F.col("m._ma").alias(ten_ma)))  # tách thành hai cột
    gan = gan_nhat(tram, "ma_tram", "dist_m", "tram_gan_nhat")  # trạm y tế gần nhất
    nc.ke_hoach(gan, f"plan_khoang_cach_{B}.txt", "khoang cach o -> tram gan nhat (noi cheo voi bang tram da broadcast)")  # minh chứng broadcast trong kế hoạch thực thi
    gan = gan.join(gan_nhat(cs, "ma_cs", "kc_cskcb_m", "cskcb_gan_nhat"), "cell_id")  # CSKCB gần nhất
    if bool(tp.uoc_luong.any()):  # có trạm ước lượng -> thêm bản "chỉ trạm có toạ độ thực" (độ nhạy A5)
        gan = gan.join(gan_nhat(tram.filter(~F.col("uoc_luong")), "ma_tram", "dist_m_chi_thuc", "_t"), "cell_id").drop("_t")  # khoảng cách tới trạm thật
    else:  # mọi trạm đều có toạ độ thực
        gan = gan.withColumn("dist_m_chi_thuc", F.col("dist_m"))  # trùng với dist_m
    gan = nc.luu(gan, "silver/fact_khoang_cach_chim_bay")  # ghi Silver
    log.info("3. Buoc 04: %d o x (%d TYT + %d CSKCB) cap khoang cach (broadcast join)", n_o, n_tram, len(cs_pd))  # in

    # ================= 3.3 NỐI ĐA NGUỒN: KHOẢNG CÁCH ĐƯỜNG ĐI (DIJKSTRA, NĂM 2024, Bước 05) =================
    dt = DoThi(d1["nut"].toPandas(), d1["canh"].toPandas())  # dựng đồ thị trên driver
    kc_t, src_t, ds_t = dt.dijkstra(tp[["x_m", "y_m"]].values, "dai")  # mét tới TYT gần nhất theo đường
    tg_t, _, _ = dt.dijkstra(tp[["x_m", "y_m"]].values, "phut")  # phút tới TYT gần nhất theo đường
    kc_c, src_c, _ = dt.dijkstra(cs_pd[["x_m", "y_m"]].values, "dai")  # mét tới CSKCB gần nhất
    tg_c, _, _ = dt.dijkstra(cs_pd[["x_m", "y_m"]].values, "phut")  # phút tới CSKCB gần nhất
    nn = pd.DataFrame({"nut": np.arange(dt.n), "d_tyt": kc_t, "t_tyt": tg_t, "d_cs": kc_c, "t_cs": tg_c,  # bảng chi phí từng nút
                       "tram_duong": tp.ma_tram.values[np.clip(src_t, 0, len(tp) - 1)],  # trạm gần nhất theo đường
                       "cs_duong": cs_pd.ma_cs.values[np.clip(src_c, 0, len(cs_pd) - 1)]})  # CSKCB gần nhất theo đường
    nn = nn.replace([np.inf, -np.inf], np.nan)  # nút không tới được (vô cùng) -> rỗng
    nut_df = nc.tao_df(nn)  # đưa bảng nút lên Spark
    mang = (o_dan.select("cell_id", "nut_duong", "kc_ra_duong_m").join(nut_df, F.col("nut_duong") == F.col("nut"))  # JOIN ô với nút đường gần nhất
            .select("cell_id",  # khoá ô
                    (F.col("d_tyt") + F.col("kc_ra_duong_m")).alias("net_dist_m"),  # mét = chi phí tới nút + đoạn đi bộ từ ô ra nút
                    (F.col("t_tyt") + F.col("kc_ra_duong_m") / DI_BO / 60).alias("net_time_min"),  # phút = chi phí tới nút + phút đi bộ
                    F.col("tram_duong").alias("net_station"), F.col("kc_ra_duong_m").alias("snap_cell_m"),  # trạm gần nhất theo đường, đoạn ra đường
                    (F.col("d_cs") + F.col("kc_ra_duong_m")).alias("kc_cskcb_duong_m"),  # mét tới CSKCB theo đường
                    (F.col("t_cs") + F.col("kc_ra_duong_m") / DI_BO / 60).alias("tg_cskcb_phut"),  # phút tới CSKCB
                    F.col("cs_duong").alias("cskcb_gan_nhat_duong")))  # CSKCB gần nhất theo đường
    mang = nc.luu(mang, "silver/fact_khoang_cach_duong_di")  # ghi Silver
    n_khong_toi = mang.filter(F.col("net_dist_m").isNull()).count()  # số ô không tới được trạm theo đường
    log.info("3. Buoc 05: Dijkstra %d nut; tram cach nut duong trung vi %.0f m, max %.0f m; o khong toi duoc: %d",  # in
             dt.n, np.median(ds_t), ds_t.max(), n_khong_toi)  # (tiếp)

    # ================= 3.4 CỘT NGHIỆP VỤ =================
    # T4: phân loại xã theo địa hình (A7) – trọng số dân 2024
    dan_B = fact.filter(F.col("year") == B).select("cell_id", "pop", "pop60", "share60")  # dân năm hiện trạng
    xa = (o_dan.join(dan_B, "cell_id", "left").fillna(0, ["pop"]).withColumn("w", F.col("pop") + 1e-9)  # ô kèm dân 2024 (0 nếu thiếu)
          .groupBy("ma_tram").agg((F.sum(F.coalesce("slope_deg", F.lit(0.0)) * F.col("w")) / F.sum("w")).alias("doc_tb_theo_dan"),  # độ dốc TB theo dân
                                  (F.sum(F.coalesce("elev_m", F.lit(0.0)) * F.col("w")) / F.sum("w")).alias("cao_tb_theo_dan"))  # độ cao TB theo dân
          .join(F.broadcast(dm.select("ma_tram", "ten_day_du", "quan_huyen", "loai_dv", "ma_quan")), "ma_tram")  # gắn thông tin xã
          .withColumn("mien_nui_dia_hinh", (F.col("loai_dv") == "Xã") & ((F.col("doc_tb_theo_dan") > 5) | (F.col("cao_tb_theo_dan") > 50)))  # quy tắc A7
          .withColumn("nhom", F.when(F.col("loai_dv") == "Phường", N_PHUONG).when(F.col("mien_nui_dia_hinh"), N_NUI).otherwise(N_DB)))  # 3 nhóm
    xa = nc.luu(xa, "silver/dim_xa")  # ghi Silver

    # Bảng phân tích năm 2024: 1 dòng / ô có dân năm 2024, đủ mọi khoảng cách
    a = (o_dan.select("cell_id", "row", "col", "lon", "lat", "x_m", "y_m", "ma_tram", "ma_quan", "elev_m", "slope_deg")  # thông tin ô
         .join(dan_B, "cell_id")  # dân 2024
         .join(gan, "cell_id").join(mang, "cell_id", "left")  # khoảng cách chim bay và đường đi
         .join(F.broadcast(xa.select("ma_tram", "nhom", "quan_huyen", "ten_day_du")), "ma_tram")  # nhóm địa bàn
         .withColumn("detour", F.col("net_dist_m") / F.greatest(F.col("dist_m"), F.lit(50.0))))  # hệ số vòng = đường đi / chim bay (tối thiểu 50 m)
    a = nc.luu(a, f"silver/o_luoi_tiep_can_{B}", "ma_quan")  # ghi Silver
    fy = nc.luu(fact.join(gan.select("cell_id", "dist_m", "tram_gan_nhat"), "cell_id").repartition("year"),  # bảng ô x năm kèm khoảng cách (giữ 56 trạm qua các năm, A14)
                "silver/fact_o_nam_khoang_cach", ["year", "ma_quan"])  # (tiếp) chia thư mục theo năm và quận

    # ---------- đối soát và kiểm tra của Bước 3 ----------
    op = o_dan.select("cell_id", "x_m", "y_m").toPandas().merge(gan.select("cell_id", "dist_m", "tram_gan_nhat").toPandas(), on="cell_id")  # ô + kết quả Spark
    dk, ik = cKDTree(tp[["x_m", "y_m"]].values).query(op[["x_m", "y_m"]].values)  # tính lại bằng cây KDTree (cách độc lập)
    lech = float(np.abs(dk - op.dist_m.values).max())  # lệch lớn nhất (m)
    giong = float((tp.ma_tram.values[ik] == op.tram_gan_nhat.values).mean() * 100)  # % ô cùng trạm gần nhất
    nc.kiem("Khoang cach toi tram gan nhat: Spark (broadcast join) vs KDTree, lech lon nhat (m)", round(lech, 4), 0.0, lech <= 0.5)  # đối soát
    nc.kiem("Ty le o co cung tram gan nhat (%)", round(giong, 3), 100.0, giong >= 99.9, "khac nhau chi khi hai tram cach deu")  # đối soát
    n_am = a.filter(F.col("net_dist_m") < F.col("dist_m") - 0.01).count()  # ô có đường đi ngắn hơn đường chim bay (vô lý)
    nc.kiem("So o co duong di ngan hon duong chim bay", n_am, 0, n_am == 0, "bat dang thuc tam giac")  # đối soát
    tong = a.agg(F.sum("pop").alias("p"), F.sum("pop60").alias("p60")).first()  # tổng dân, dân 60+ năm 2024
    xa_70 = o_dan.join(dan_B, "cell_id", "left").agg(F.sum(F.when(F.col("kc_ra_duong_m") > 500, 1).otherwise(0)).alias("n"),  # ô cách đường > 500 m
                                                     F.sum(F.when(F.col("kc_ra_duong_m") > 500, F.col("pop")).otherwise(0)).alias("p")).first()  # dân của các ô đó
    n_share_xa = ob.filter("popB <= 0").count()  # ô dùng tỷ lệ 60+ trung bình xã (ô 1 km = 0)
    qg = [dict(kiem_tra="So o 100 m co dan trong 56 xa", gia_tri=n_o),  # bảng qa_grid
          dict(kiem_tra=f"Tong dan 56 xa nam {B} (100 m)", gia_tri=round(tong.p)),  # (tiếp)
          dict(kiem_tra=f"Tong dan 60+ 56 xa nam {B} (A3)", gia_tri=round(tong.p60)),  # (tiếp)
          dict(kiem_tra="O dung ty le 60+ trung binh xa (o 1 km = 0)", gia_tri=n_share_xa),  # (tiếp)
          dict(kiem_tra="Dong dan so sach thuoc o toan 0 (bo khoi phan tich)", gia_tri=n_dong_o_0),  # (tiếp)
          dict(kiem_tra="O luoi cach duong > 500 m (so o)", gia_tri=int(xa_70.n)),  # (tiếp)
          dict(kiem_tra=f"O luoi cach duong > 500 m (dan {B})", gia_tri=round(float(xa_70.p or 0))),  # (tiếp)
          dict(kiem_tra="Tram cach nut duong gan nhat > 200 m", gia_tri=int((ds_t > 200).sum())),  # (tiếp)
          dict(kiem_tra="O khong toi duoc tram theo duong", gia_tri=n_khong_toi)]  # (tiếp)
    nc.xuat("qa_grid.csv", pd.DataFrame(qg), S, note="kiem tra luoi dan so va mang duong sau chuan hoa")  # vào hàng chờ
    qs = tp.merge(pd.DataFrame({"ma_tram": tp.ma_tram, "kc_toi_nut_duong_m": ds_t.round(1)}), on="ma_tram")  # thêm khoảng cách trạm ra đường
    qs = qs.rename(columns={"ten_day_du": "ten", "trong_ranh_gioi_xa": "trong_ranh_gioi", "nguon_toa_do": "nguon"})  # giữ tên cột của bản cũ (quy tắc 5)
    nc.xuat("qa_stations.csv", qs[["ma_tram", "ten", "trong_ranh_gioi", "nguon", "cap_trung_toa_do_50m",  # cột cũ (bỏ các cột ghép OSM vì không còn bước ghép OSM)
                                   "kc_ngoai_ranh_gioi_m", "kc_tram_tam_dan_cu_m", "kc_toi_nut_duong_m"]],  # cột mới thêm ở cuối
            S, note="kiem tra toa do 56 tram (A5, A13)")  # vào hàng chờ
    nc.xuat("tram_y_te_56.csv", tp[["ma_tram", "ten_tram", "ten_day_du", "quan_huyen", "loai_dv", "lon", "lat", "x_m", "y_m", "nguon_toa_do", "uoc_luong",  # bảng trạm cho báo cáo
                                    "trong_ranh_gioi_xa", "kc_ngoai_ranh_gioi_m", "kc_tram_tam_dan_cu_m", "cap_trung_toa_do_50m", "dong_excel"]],  # (tiếp)
            S, note="56 tram: toa do, nam trong ranh gioi xa, khoang cach toi tam dan cu (A5, A13)")  # vào hàng chờ
    nc.qa_them("Thiếu", "Trạm có toạ độ thực (nhập tay, không phải ước lượng)", int((~tp.uoc_luong).sum()), bool((~tp.uoc_luong).all()), "trạm ước lượng dùng tâm dân cư xã (A5)")  # QA
    n_xa_co_o = int(o_dan.select("ma_tram").distinct().count())  # số xã có ít nhất một ô có dân (đếm một lần)
    nc.qa_them("Thiếu", "Xã không có ô dân nào", 56 - n_xa_co_o, n_xa_co_o == 56)  # QA
    n_thieu_cao = int(o_dan.filter(F.col("elev_m").isNull()).count())  # số ô có dân thiếu độ cao (đếm một lần)
    nc.qa_them("Thiếu", "Ô có dân thiếu độ cao", n_thieu_cao, n_thieu_cao <= 0.01 * n_o)  # QA
    nc.qa_them("Trùng", "Cặp trạm trùng toạ độ (< 50 m)", int(tp.cap_trung_toa_do_50m.sum() // 2), int(tp.cap_trung_toa_do_50m.sum()) == 0)  # QA
    nc.qa_them("Sai toạ độ", "Trạm nằm ngoài ranh giới xã của chính nó", int((~tp.trong_ranh_gioi_xa).sum()), bool(tp.trong_ranh_gioi_xa.all()),  # QA
               f"xa nhất {tp.kc_ngoai_ranh_gioi_m.max():.0f} m; có thể do ranh giới GADM thô")  # (tiếp)
    xa3 = tp[tp.kc_tram_tam_dan_cu_m > 3000]  # trạm cách tâm dân cư > 3 km
    nc.qa_them("Sai toạ độ", "Trạm cách tâm dân cư xã > 3 km", len(xa3), len(xa3) == 0, ", ".join(xa3.ten_tram))  # QA
    n_60_vuot = int(a.filter(F.col("pop60") > F.col("pop") + 1e-6).count())  # số ô có dân 60+ lớn hơn tổng dân (đếm một lần)
    nc.qa_them("Lệch tổng", f"Ô có dân 60+ > tổng dân (năm {B})", n_60_vuot, n_60_vuot == 0)  # QA
    nc.qa_them("Lệch tổng", f"Tỷ lệ 60+ toàn 56 xã năm {B} (%)", round(100 * tong.p60 / tong.p, 2), 5 < 100 * tong.p60 / tong.p < 30, "kiểm tra hợp lý")  # QA

    return dict(o_dan=o_dan, fact=fact, fy=fy, a=a, tram=tram, tp=tp, cs=cs, cs_pd=cs_pd, xa=xa, do_thi=dt, k1=k1)  # trả cho Bước 4 và Bước 5
