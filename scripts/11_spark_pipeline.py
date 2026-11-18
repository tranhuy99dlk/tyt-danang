# -*- coding: utf-8 -*-
"""11 - PIPELINE SPARK: doc CSV tho voi schema -> kiem tra -> tach loi (quarantine) va dong ngoai pham vi (excluded) -> lam sach
-> bien doi / join -> tong hop KPI -> ghi ket qua -> doi soat.

Dau vao  : data/landing/*.csv (do 11a_landing.py tao tu raster WorldPop, Excel nhap tay, 05_network.py)
           hoac <HDFS_URI>/landing/*.csv neu dat bien moi truong HDFS_URI.
Cac vung : landing (CSV tho) -> curated (Parquet da sach) + quarantine (LOI DU LIEU that, kem ly do)
           + excluded (dong NGOAI PHAM VI: NoData cua WorldPop, ngoai 56 xa... - khong phai loi) -> gold (KPI).
           Khong co HDFS_URI: cac vung nam o data/spark/; co HDFS_URI: nam tren HDFS.
Cac buoc :
  1. DOC voi schema khai bao (StructType, che do PERMISSIVE: dong sai kieu / sai so cot vao _corrupt_record).
  2. KIEM TRA tung dong theo quy tac; dong vi pham kem ma ly do (mot dong co the nhieu ly do), phan 2 loai:
       LOI_DU_LIEU (co it nhat 1 ma loi) -> quarantine/; NGOAI_PHAM_VI (chi mang ma trong MA_NGOAI_PHAM_VI) -> excluded/.
  3. LAM SACH: bo trung, doi ten/kieu, chuan hoa ten xa, doi "lat, lon" thanh so, chieu UTM 49N.
  4. BIEN DOI / JOIN (co y nghia nghiep vu):
       T1 gop 7 nhom tuoi 60-90+ (luoi 1 km) -> ty le 60+ -> dan 60+ tung o 100 m (gia dinh A3)
       T2 noi cheo o luoi x 56 tram (broadcast) -> tram gan nhat + khoang cach duong chim bay
       T3 noi khoang cach mang duong -> he so vong (muc do thuan tien duong di, A6)
       T4 phan loai xa: Phuong / Xa dong bang-ven do / Xa mien nui theo dia hinh (A7)
  5. TONG HOP: KPI chinh (ty le dan trong 3 km) + KPI phu theo tung nam trong YEARS, theo xa, theo nhom, theo tram.
  6. GHI: Parquet (curated, quarantine, excluded, gold) + CSV trong outputs/tables/. Parquet khong co giao dich:
     moi lan doc mot bang da ghi deu kiem tra co _SUCCESS truoc. Ke hoach thuc thi cua truy van KPI chinh
     ghi vao logs/plan_kpi_<nam co so>.txt.
  7. DOI SOAT: bao toan so dong va tong dan (tho = hop le + loi + ngoai pham vi); Spark so voi pandas (buoc 03-06).

Chay trong container Spark (tu thu muc du an):
    bash scripts/run_spark.sh
Hoac trong cum cua hoc phan (JupyterLab / spark-notebook), co HDFS:
    HDFS_URI=hdfs://namenode:9000/mis5007 python scripts/11_spark_pipeline.py
Bien moi truong (doc BEN TRONG container): HDFS_URI (tuy chon), HDFS_REPLICATION (mac dinh 1), SPARK_MASTER (mac dinh local[*]),
SPARK_DRIVER_MEMORY (mac dinh 3g), SPARK_SHUFFLE_PARTITIONS (mac dinh 16), SPARK_UI (mac dinh false),
SPARK_PARTITION_OVERWRITE (mac dinh static; dynamic CHI khi cap nhat vai nam - xem ghi chu o cau hinh Spark).
Cach dat: docs/HUONG_DAN_SU_DUNG.md muc 4.
"""
import math
import os
import sys
import time
from pathlib import Path

import pandas as pd

SCRIPTS = str(Path(__file__).resolve().parent)
sys.path.insert(0, SCRIPTS)
os.environ["PYTHONPATH"] = SCRIPTS + os.pathsep + os.environ.get("PYTHONPATH", "")   # de worker Python import duoc common
from common import (INTERIM, TAB, LANDING, SPARK_OUT, BBOX, BASE_YEAR, YEARS, THRESH_KM, MAIN_KM, WHITE_KM,  # noqa: E402
                    AGE60_BANDS, LOGS, norm_name, get_logger, save_csv)

S = "11_spark_pipeline"
log = get_logger(S)
REL_TOL = 1e-6       # sai so tuong doi cho phep giua Spark va pandas (tong so thuc)
PCT_TOL = 0.011      # pandas lam tron ty le % 2 chu so
DIST_TOL = 0.5       # met
R = MAIN_KM * 1000
RW = WHITE_KM * 1000
CORRUPT = "_corrupt_record"
RE_LATLON = r"^\s*(-?\d+(?:\.\d+)?)\s*[,;]\s*(-?\d+(?:\.\d+)?)\s*$"
N_PHUONG, N_NUI, N_DB = "Phường", "Xã miền núi (địa hình)", "Xã đồng bằng/ven đô"
# Ma ly do KHONG phai loi du lieu: gia tri NoData do nguon cong bo cho o khong co dan, va dong nam ngoai pham vi nghien cuu.
# Dong chi mang cac ma nay -> excluded/ (loai NGOAI_PHAM_VI); co them bat ky ma nao khac -> quarantine/ (loai LOI_DU_LIEU).
MA_NGOAI_PHAM_VI = ("GIA_TRI_AM_NODATA", "NGOAI_56_XA", "NAM_NGOAI_PHAM_VI", "KHONG_THUOC_O_CO_DAN")
LOI, NGOAI = "LOI_DU_LIEU", "NGOAI_PHAM_VI"


def utm49n(lon, lat):
    """Chieu WGS84 -> UTM mui 49 Bac (EPSG:32649) bang chuoi Kruger bac 3; khop pyproj toi mm trong mui."""
    if lon is None or lat is None:
        return None
    a, f, k0, lon0 = 6378137.0, 1 / 298.257223563, 0.9996, 111.0
    n = f / (2 - f)
    A = a / (1 + n) * (1 + n ** 2 / 4 + n ** 4 / 64)
    al = (n / 2 - 2 * n ** 2 / 3 + 5 * n ** 3 / 16, 13 * n ** 2 / 48 - 3 * n ** 3 / 5, 61 * n ** 3 / 240)
    phi, lam = math.radians(lat), math.radians(lon - lon0)
    c = 2 * math.sqrt(n) / (1 + n)
    t = math.sinh(math.atanh(math.sin(phi)) - c * math.atanh(c * math.sin(phi)))
    xi, eta = math.atan2(t, math.cos(lam)), math.atanh(math.sin(lam) / math.sqrt(1 + t * t))
    x = 500000 + k0 * A * (eta + sum(al[j] * math.cos(2 * (j + 1) * xi) * math.sinh(2 * (j + 1) * eta) for j in range(3)))
    y = k0 * A * (xi + sum(al[j] * math.sin(2 * (j + 1) * xi) * math.cosh(2 * (j + 1) * eta) for j in range(3)))
    return (float(x), float(y))


def _ensure_pyspark():
    """Image jupyter/pyspark-notebook chi dat PYTHONPATH cho kernel Jupyter; khi goi bang `docker exec ... python`
    thi tu them $SPARK_HOME/python vao duong dan."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        import glob
        sh = os.environ.get("SPARK_HOME", "")
        paths = [f"{sh}/python"] + glob.glob(f"{sh}/python/lib/py4j-*-src.zip")
        sys.path[:0] = paths
        os.environ["PYTHONPATH"] = os.pathsep.join(paths + [os.environ.get("PYTHONPATH", "")])


def main():
    t0 = time.time()
    _ensure_pyspark()
    hdfs = os.environ.get("HDFS_URI")
    if hdfs:
        os.environ.setdefault("HADOOP_USER_NAME", "root")   # cum HDFS cua lop bat kiem tra quyen
    from pyspark.sql import SparkSession, Window, functions as F, types as T

    spark = (SparkSession.builder.appName("mis5007_tram_y_te")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
             .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
             .config("spark.executorEnv.PYTHONPATH", SCRIPTS)
             .config("spark.sql.maxPlanStringLength", "2000")      # ke hoach nhieu join: khong dung chuoi mo ta dai
             .config("spark.ui.enabled", os.environ.get("SPARK_UI", "false"))
             .config("spark.hadoop.dfs.replication", os.environ.get("HDFS_REPLICATION", "1"))   # cum chi co 1 DataNode
             # static (mac dinh): overwrite xoa ca thu muc roi ghi lai - dung cho moi lan chay day du.
             # dynamic: chi ghi de cac phan vung nam co trong du lieu moi - CHI dung khi cap nhat vai nam; neu YEARS bi thu hep,
             # phan vung cu se con lai va phep doi soat "doc lai = truoc khi ghi" se bao LECH.
             .config("spark.sql.sources.partitionOverwriteMode", os.environ.get("SPARK_PARTITION_OVERWRITE", "static"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    zone = hdfs.rstrip("/") if hdfs else "file://" + str(SPARK_OUT)
    land = f"{zone}/landing" if hdfs else "file://" + str(LANDING)
    log.info("Spark %s, master=%s | landing=%s | cac vung=%s", spark.version, spark.sparkContext.master, land, zone)

    I, D, St = T.IntegerType(), T.DoubleType(), T.StringType()

    def schema(*fields):
        return T.StructType([T.StructField(n, t, True) for n, t in fields] + [T.StructField(CORRUPT, St, True)])

    SCHEMA = {
        "pop_100m_raw": schema(("year", I), ("row", I), ("col", I), ("pop_raw", D)),
        "cells_dim": schema(("row", I), ("col", I), ("cell_id", St), ("lon", D), ("lat", D), ("x_m", D), ("y_m", D),
                            ("ma_tram", St), ("k1_row", I), ("k1_col", I), ("elev_m", D), ("slope_deg", D)),
        "age_1km_raw": schema(("year", I), ("k1_row", I), ("k1_col", I), ("bien", St), ("gia_tri", D)),
        "stations_raw": schema(("dong_excel", I), ("cot1", St), ("cot2", St), ("cot3", St)),
        "net_dist": schema(("cell_id", St), ("net_dist_m", D), ("net_time_min", D), ("snap_cell_m", D), ("net_station", St)),
        "dm_56_tram": schema(("ma_tram", St), ("quan_huyen", St), ("loai_dv", St), ("ten_xa_phuong", St),
                             ("ten_day_du", St), ("ten_tram", St)),
    }

    # ================= 1. DOC VOI SCHEMA =================
    def read(name):
        df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", CORRUPT).schema(SCHEMA[name]).csv(f"{land}/{name}.csv")).cache()
        return df

    counts, quarantines, checks = [], [], []
    jvm, hconf = spark.sparkContext._jvm, spark.sparkContext._jsc.hadoopConfiguration()
    da_ghi = []                                   # moi duong dan Parquet da ghi trong lan chay nay

    def co_success(path):
        """Parquet khong co giao dich: _SUCCESS chi xuat hien khi job ghi xong tron ven."""
        p = jvm.org.apache.hadoop.fs.Path(f"{path}/_SUCCESS")
        return bool(p.getFileSystem(hconf).exists(p))

    def ghi(df, path, part=None, che_do_phan_vung=None):
        w = df.write.mode("overwrite")
        if che_do_phan_vung:
            w = w.option("partitionOverwriteMode", che_do_phan_vung)
        (w.partitionBy(part) if part else w).parquet(f"{zone}/{path}")
        da_ghi.append(f"{zone}/{path}")

    def doc(path, schema=None):
        """Chi doc bang da ghi xong: thieu _SUCCESS (job truoc chet giua chung, thu muc rong/do dang) -> dung ngay."""
        full = f"{zone}/{path}"
        if not co_success(full):
            raise RuntimeError(f"Thieu {full}/_SUCCESS: lan ghi chua xong hoac bi ngat - khong doc du lieu do dang. Chay lai buoc 11.")
        r = spark.read if schema is None else spark.read.schema(schema)
        return r.parquet(full)

    def persist(df, path, part=None):
        """Ghi mot bang ra vung luu tru roi DOC LAI (sau khi kiem tra _SUCCESS): cat chuoi phu thuoc,
        cac buoc sau tinh tu du lieu da luu."""
        ghi(df, path, part)
        return doc(path)

    def ghi_ke_hoach(df, ten_file, mo_ta):
        """Ghi ke hoach thuc thi (explain formatted) cua mot truy van vao logs/ - day du, khong bi cat."""
        import contextlib
        import io
        cu = spark.conf.get("spark.sql.maxPlanStringLength")
        try:
            spark.conf.set("spark.sql.maxPlanStringLength", "100000000")
        except Exception:                          # cau hinh khong doi duoc luc chay: van ghi ban (co the bi cat)
            pass
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            df.explain("formatted")
        try:
            spark.conf.set("spark.sql.maxPlanStringLength", cu)
        except Exception:
            pass
        f = LOGS / ten_file
        f.write_text(f"# {mo_ta}\n# Spark {spark.version}, ghi luc {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n" + buf.getvalue(), encoding="utf-8")
        log.info("5. Ke hoach thuc thi cua %s: %s (%d dong)", mo_ta, f.relative_to(LOGS.parent), buf.getvalue().count("\n"))

    def add(phep, s, p, dat, ghi=""):
        checks.append(dict(phep_doi_soat=phep, spark=s, pandas_hoac_nguon=p, ket_qua="ĐẠT" if dat else "LỆCH", ghi_chu=ghi))

    def with_dup(df, keys):
        """Danh so ban ghi trong moi nhom khoa: > 1 la ban trung."""
        return df.withColumn("_dup", F.row_number().over(Window.partitionBy(*keys).orderBy(F.lit(1))))

    # ================= 2. KIEM TRA + TACH LOI =================
    def split(df, name, raw_cols, key_cols, rules):
        """rules = [(ma_ly_do, dieu_kien)]. Tra ve bang hop le; dong vi pham ghi vao danh sach quarantine."""
        flagged = df.withColumn("_ly_do", F.concat_ws(";", *[F.when(c, F.lit(code)) for code, c in rules])).cache()
        bad = flagged.filter(F.col("_ly_do") != "").withColumn(
            "_loai", F.when(F.exists(F.split("_ly_do", ";"), lambda x: ~x.isin(*MA_NGOAI_PHAM_VI)), F.lit(LOI)).otherwise(F.lit(NGOAI)))
        good = flagged.filter(F.col("_ly_do") == "").drop("_ly_do", CORRUPT)
        q = bad.select(F.lit(name).alias("nguon"), F.col("_loai").alias("loai"), F.col("_ly_do").alias("ly_do"),
                       F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("")) for k in key_cols]).alias("khoa"),
                       F.to_json(F.struct(*[F.col(c) for c in raw_cols + [CORRUPT]])).alias("ban_ghi"))
        n_raw = flagged.count()
        theo_loai = dict(bad.groupBy("_loai").count().collect())
        n_loi, n_ngoai = int(theo_loai.get(LOI, 0)), int(theo_loai.get(NGOAI, 0))
        n_bad = n_loi + n_ngoai
        counts.append(dict(nguon=name, so_dong_tho=n_raw, so_dong_hop_le=n_raw - n_bad, so_dong_cach_ly=n_bad,
                           so_dong_loi_du_lieu=n_loi, so_dong_ngoai_pham_vi=n_ngoai))
        quarantines.append(q)
        log.info("2. %-13s tho %8d | hop le %8d | loi du lieu %5d | ngoai pham vi %8d", name, n_raw, n_raw - n_bad, n_loi, n_ngoai)
        return good, flagged

    bad_type = F.col(CORRUPT).isNotNull()
    yr_ok = F.col("year").between(YEARS[0], YEARS[-1])
    W_, S_, E_, N_ = BBOX

    # --- danh muc 56 tram / xa ---
    dm_raw = read("dm_56_tram")
    dm, _ = split(with_dup(dm_raw, ["ma_tram"]), "dm_56_tram", dm_raw.columns[:-1], ["ma_tram"], [
        ("SAI_KIEU_HOAC_SO_COT", bad_type),
        ("THIEU_KHOA", F.col("ma_tram").isNull() | F.col("ten_xa_phuong").isNull()),
        ("TRUNG_KHOA", F.col("_dup") > 1)])
    dm = dm.drop("_dup").cache()
    dm_codes = F.broadcast(dm.select(F.col("ma_tram").alias("_ma_dm")))

    # --- danh muc o luoi 100 m ---
    cells_raw = read("cells_dim")
    cells, _ = split(with_dup(cells_raw, ["row", "col"]).join(dm_codes, F.col("ma_tram") == F.col("_ma_dm"), "left"),
                     "cells_dim", cells_raw.columns[:-1], ["row", "col"], [
        ("SAI_KIEU_HOAC_SO_COT", bad_type),
        ("THIEU_KHOA", F.col("row").isNull() | F.col("col").isNull() | F.col("cell_id").isNull()),
        ("THIEU_TOA_DO", F.col("lon").isNull() | F.col("lat").isNull() | F.col("x_m").isNull() | F.col("y_m").isNull()),
        ("SAI_TOA_DO_NGOAI_HOP_BAO", ~(F.col("lon").between(W_, E_) & F.col("lat").between(S_, N_))),
        ("TRUNG_KHOA", F.col("_dup") > 1),
        ("NGOAI_56_XA", F.col("ma_tram").isNull()),
        ("MA_XA_KHONG_CO_TRONG_DANH_MUC", F.col("ma_tram").isNotNull() & F.col("_ma_dm").isNull())])
    cells = cells.drop("_dup", "_ma_dm").cache()

    # --- pixel dan so 100 m x nam ---
    pop_raw = read("pop_100m_raw")
    all_cells = F.broadcast(cells_raw.dropDuplicates(["row", "col"]).select("row", "col", F.lit(1).alias("_co_o"), F.col("ma_tram").alias("_ma")))
    ok_cells = F.broadcast(cells.select("row", "col", F.lit(1).alias("_o_ok")))
    pop, pop_flag = split(with_dup(pop_raw, ["year", "row", "col"]).join(all_cells, ["row", "col"], "left").join(ok_cells, ["row", "col"], "left"),
                          "pop_100m_raw", pop_raw.columns[:-1], ["year", "row", "col"], [
        ("SAI_KIEU_HOAC_SO_COT", bad_type),
        ("THIEU_KHOA", F.col("year").isNull() | F.col("row").isNull() | F.col("col").isNull()),
        ("NAM_NGOAI_PHAM_VI", ~yr_ok),
        ("THIEU_GIA_TRI", F.col("pop_raw").isNull() & ~bad_type),
        ("GIA_TRI_AM_NODATA", F.col("pop_raw") < 0),
        ("TRUNG_KHOA", F.col("_dup") > 1),
        ("KHONG_CO_O_LUOI", F.col("_co_o").isNull()),
        ("NGOAI_56_XA", F.col("_co_o").isNotNull() & F.col("_ma").isNull()),
        ("O_LUOI_BI_CACH_LY", F.col("_ma").isNotNull() & F.col("_o_ok").isNull())])
    pop = pop.select("year", "row", "col", F.col("pop_raw").alias("pop"))

    # --- luoi 1 km: tong dan + nhom tuoi ---
    bien_ok = ["pop"] + [f"t_{b}" for b in AGE60_BANDS]
    age_raw = read("age_1km_raw")
    age_ok, _ = split(with_dup(age_raw, ["year", "k1_row", "k1_col", "bien"]), "age_1km_raw", age_raw.columns[:-1],
                      ["year", "k1_row", "k1_col", "bien"], [
        ("SAI_KIEU_HOAC_SO_COT", bad_type),
        ("THIEU_KHOA", F.col("year").isNull() | F.col("k1_row").isNull() | F.col("k1_col").isNull() | F.col("bien").isNull()),
        ("NAM_NGOAI_PHAM_VI", ~yr_ok),
        ("BIEN_KHONG_HOP_LE", ~F.col("bien").isin(bien_ok)),
        ("THIEU_GIA_TRI", F.col("gia_tri").isNull() & ~bad_type),
        ("GIA_TRI_AM_NODATA", F.col("gia_tri") < 0),
        ("TRUNG_KHOA", F.col("_dup") > 1)])

    # --- toa do tram nhap tay ---
    f_norm = F.udf(norm_name, St)
    f_utm = F.udf(utm49n, T.StructType([T.StructField("x", D), T.StructField("y", D)]))
    st_raw = read("stations_raw")
    dm_key = F.broadcast(dm.select(f_norm("ten_xa_phuong").alias("_k"), F.col("ma_tram"), "ten_tram", "quan_huyen", "loai_dv", "ten_day_du"))
    st_p = (st_raw.withColumn("lat", F.regexp_extract("cot3", RE_LATLON, 1).cast(D))
            .withColumn("lon", F.regexp_extract("cot3", RE_LATLON, 2).cast(D))
            .withColumn("_k", f_norm("cot2")).join(dm_key, "_k", "left"))
    st_p = st_p.withColumn("_dup", F.when(F.col("ma_tram").isNotNull(),
                                          F.row_number().over(Window.partitionBy("ma_tram").orderBy("dong_excel"))).otherwise(F.lit(1)))
    is_rec = F.col("cot2").isNotNull() & F.col("cot3").isNotNull()
    st, _ = split(st_p, "stations_raw", st_raw.columns[:-1], ["dong_excel"], [
        ("SAI_KIEU_HOAC_SO_COT", bad_type),
        ("KHONG_PHAI_BAN_GHI_TRAM", ~is_rec),
        ("SAI_DINH_DANG_TOA_DO", is_rec & F.col("lat").isNull()),
        ("NGOAI_HOP_BAO", F.col("lat").isNotNull() & ~(F.col("lon").between(W_, E_) & F.col("lat").between(S_, N_))),
        ("TEN_KHONG_KHOP_DANH_MUC", is_rec & F.col("lat").isNotNull() & F.col("ma_tram").isNull()),
        ("TRUNG_TRAM", F.col("_dup") > 1)])

    # ================= 3. LAM SACH + 4. BIEN DOI / JOIN =================
    # tram: chieu UTM
    st = (st.withColumn("_xy", f_utm("lon", "lat"))
          .select("ma_tram", "ten_tram", "quan_huyen", "loai_dv", "lon", "lat",
                  F.col("_xy.x").alias("x_m"), F.col("_xy.y").alias("y_m"), "dong_excel"))
    st = persist(st, "curated/dim_tram").cache()
    n_st = st.count()

    # o co dan: o hop le co dan > 0 it nhat mot nam (o toan 0 bi bo khoi phan tich, khong phai loi)
    has_pop = pop.groupBy("row", "col").agg(F.max("pop").alias("_mx")).filter("_mx > 0").select("row", "col")
    cells_pop = persist(cells.join(has_pop, ["row", "col"]), "curated/dim_o_luoi").cache()
    n_cells = cells_pop.count()
    n_zero_rows = pop.join(has_pop, ["row", "col"], "left_anti").count()

    # T1: 7 nhom tuoi -> dan 60+ o 1 km -> ty le 60+ (A3)
    age = (age_ok.groupBy("year", "k1_row", "k1_col")
           .agg(F.sum(F.when(F.col("bien") == "pop", F.col("gia_tri"))).alias("pop_1km"),
                F.sum(F.when(F.col("bien") != "pop", F.col("gia_tri"))).alias("pop60_1km")).cache())
    age_b = age.filter(F.col("year") == BASE_YEAR).select("k1_row", "k1_col", F.col("pop_1km").alias("popB"), F.col("pop60_1km").alias("pop60B"))
    cb = cells_pop.join(age_b, ["k1_row", "k1_col"], "left").fillna(0, ["popB", "pop60B"])
    xa_share = (cb.filter("popB > 0").groupBy("ma_tram")
                .agg((F.sum("pop60B") / F.sum("popB")).alias("share_xa")))
    cb = (cb.join(F.broadcast(xa_share), "ma_tram", "left")
          .withColumn("share_base", F.when(F.col("popB") > 0, F.col("pop60B") / F.col("popB")).otherwise(F.col("share_xa"))))
    fact = (pop.join(cb.select("row", "col", "cell_id", "ma_tram", "k1_row", "k1_col", "share_base"), ["row", "col"])
            .join(age, ["year", "k1_row", "k1_col"], "left").fillna(0, ["pop_1km", "pop60_1km"])
            .withColumn("share60", F.when(F.col("pop_1km") > 0, F.col("pop60_1km") / F.col("pop_1km")).otherwise(F.col("share_base")))
            .withColumn("pop60", F.col("pop") * F.col("share60"))
            .select("cell_id", "ma_tram", "year", "pop", "pop60", "share60")).cache()
    n_fact_truoc = fact.count()
    fact = persist(fact.repartition("year"), "curated/fact_dan_so_o_nam", "year").cache()
    n_fact = fact.count()
    log.info("4. T1 bang su kien o x nam: %d dong (%d o co dan x toi da %d nam); %d dong hop le thuoc o toan 0 bi bo",
             n_fact, n_cells, len(YEARS), n_zero_rows)

    # T2: noi cheo o x tram (broadcast) -> tram gan nhat
    pairs = (cells_pop.select("cell_id", "x_m", "y_m")
             .crossJoin(F.broadcast(st.select(F.col("ma_tram").alias("tram"), F.col("x_m").alias("sx"), F.col("y_m").alias("sy"))))
             .withColumn("d", F.sqrt((F.col("x_m") - F.col("sx")) ** 2 + (F.col("y_m") - F.col("sy")) ** 2)))
    near = persist(pairs.groupBy("cell_id").agg(F.min(F.struct("d", "tram")).alias("m"))
                   .select("cell_id", F.col("m.d").alias("dist_m"), F.col("m.tram").alias("tram_gan_nhat")),
                   "curated/o_luoi_tram_gan_nhat").cache()
    log.info("4. T2 khoang cach: %d o x %d tram = %d cap", near.count(), n_st, n_cells * n_st)

    # T3: khoang cach mang duong -> he so vong
    net_raw = read("net_dist")
    pop_cells = F.broadcast(cells_pop.select(F.col("cell_id").alias("_cid")))
    net, _ = split(with_dup(net_raw, ["cell_id"]).join(pop_cells, F.col("cell_id") == F.col("_cid"), "left"),
                   "net_dist", net_raw.columns[:-1], ["cell_id"], [
        ("SAI_KIEU_HOAC_SO_COT", bad_type),
        ("THIEU_KHOA", F.col("cell_id").isNull()),
        ("KHONG_TOI_DUOC_HOAC_AM", F.col("net_dist_m").isNull() | (F.col("net_dist_m") < 0) | F.isnan("net_dist_m")),
        ("TRUNG_KHOA", F.col("_dup") > 1),
        ("KHONG_THUOC_O_CO_DAN", F.col("_cid").isNull())])
    net = persist(net.select("cell_id", "net_dist_m", "net_time_min", "net_station"), "curated/o_luoi_mang_duong")

    # T4: phan loai xa theo dia hinh (A7), trong so dan nam co so
    base = fact.filter(F.col("year") == BASE_YEAR).select("cell_id", "pop", "pop60")
    cls = persist(cells_pop.join(base, "cell_id", "left").fillna(0, ["pop"])
           .withColumn("w", F.col("pop") + 1e-9)
           .groupBy("ma_tram").agg((F.sum(F.coalesce("slope_deg", F.lit(0.0)) * F.col("w")) / F.sum("w")).alias("doc_tb_theo_dan"),
                                   (F.sum(F.coalesce("elev_m", F.lit(0.0)) * F.col("w")) / F.sum("w")).alias("cao_tb_theo_dan"))
           .join(F.broadcast(dm.select("ma_tram", "ten_day_du", "quan_huyen", "loai_dv")), "ma_tram")
           .withColumn("mien_nui_dia_hinh", (F.col("loai_dv") == "Xã") & ((F.col("doc_tb_theo_dan") > 5) | (F.col("cao_tb_theo_dan") > 50)))
           .withColumn("nhom", F.when(F.col("loai_dv") == "Phường", N_PHUONG)
                       .when(F.col("mien_nui_dia_hinh"), N_NUI).otherwise(N_DB)), "curated/dim_xa").cache()

    # bang phan tich nam co so: 1 dong / o co dan
    a = persist(cells_pop.select("cell_id", "ma_tram").join(base, "cell_id").join(near, "cell_id").join(net, "cell_id", "left")
                .join(F.broadcast(cls.select("ma_tram", "nhom", "quan_huyen")), "ma_tram")
                .withColumn("detour", F.col("net_dist_m") / F.greatest(F.col("dist_m"), F.lit(50.0))),
                f"curated/o_luoi_tiep_can_{BASE_YEAR}").cache()
    # bang o x nam kem khoang cach (mang luoi 56 tram giu nguyen qua cac nam - gia dinh A14)
    fy = persist(fact.join(near, "cell_id"), "curated/fact_o_nam_khoang_cach", "year").cache()

    # ================= 5. TONG HOP =================
    def pct(cond, w="pop"):
        return F.sum(F.when(cond, F.col(w)).otherwise(0.0)) / F.sum(w) * 100

    def kpi_agg(df, by):
        return df.groupBy(*by).agg(
            F.sum("pop").alias("dan"), F.sum("pop60").alias("dan_60"),
            *[pct(F.col("dist_m") <= km * 1000).alias(f"ty_le_trong_{km:g}km_pct") for km in THRESH_KM],
            pct(F.col("dist_m") <= R, "pop60").alias("ty_le_60_trong_3km_pct"),
            F.sum(F.when(F.col("dist_m") > RW, F.col("pop")).otherwise(0.0)).alias("dan_tren_5km"),
            F.sum(F.when(F.col("dist_m") > RW, F.col("pop60")).otherwise(0.0)).alias("dan_60_tren_5km"))

    def wmedian(df, by, v, name, w="pop"):
        """Trung vi co trong so = gia tri nho nhat co luy ke trong so >= 50% (giong common.wmedian)."""
        d = df.filter((F.col(w) > 0) & F.col(v).isNotNull())
        part = Window.partitionBy(*by) if by else Window.partitionBy(F.lit(1))
        d = (d.withColumn("_cw", F.sum(w).over(part.orderBy(v, "cell_id").rowsBetween(Window.unboundedPreceding, 0)))
             .withColumn("_tot", F.sum(w).over(part)).filter(F.col("_cw") >= 0.5 * F.col("_tot")))
        return d.groupBy(*by).agg(F.min(v).alias(name))

    def net_agg(df, by):
        return df.groupBy(*by).agg(pct(F.col("net_dist_m") <= R).alias("ty_le_trong_3km_mang_duong_pct"),
                                   pct(F.col("net_dist_m") <= RW).alias("ty_le_trong_5km_mang_duong_pct"),
                                   pct(F.col("net_time_min") <= 15).alias("ty_le_toi_tram_15_phut_pct"))

    def to_pd(df, sort):
        return df.orderBy(*sort).toPandas()

    # G1: theo tung nam trong YEARS (toan 56 xa)
    k_nam = (kpi_agg(fy, ["year"]).join(wmedian(fy, ["year"], "dist_m", "kc_trung_vi_m"), "year")
             .withColumn("so_tram", F.lit(n_st)).withColumn("dan_binh_quan_moi_tram", F.col("dan") / n_st))
    k_nam_pd = to_pd(k_nam, ["year"])
    # G2: theo xa x nam
    k_xn_pd = to_pd(kpi_agg(fy, ["ma_tram", "year"]).join(wmedian(fy, ["ma_tram", "year"], "dist_m", "kc_trung_vi_m"), ["ma_tram", "year"])
                    .join(F.broadcast(cls.select("ma_tram", "ten_day_du", "quan_huyen", "nhom")), "ma_tram"), ["ma_tram", "year"])
    # G3: theo xa, nam co so, kem mang duong
    k_xa_pd = to_pd(kpi_agg(a, ["ma_tram"]).join(wmedian(a, ["ma_tram"], "dist_m", "kc_trung_vi_m"), "ma_tram")
                    .join(net_agg(a, ["ma_tram"]), "ma_tram")
                    .join(wmedian(a, ["ma_tram"], "net_dist_m", "kc_mang_duong_trung_vi_m"), "ma_tram", "left")
                    .join(wmedian(a.filter("dist_m > 500"), ["ma_tram"], "detour", "he_so_vong_trung_vi"), "ma_tram", "left")
                    .join(F.broadcast(cls), "ma_tram"), ["ma_tram"])
    # G4: theo nhom (phuong / xa dong bang / xa mien nui)
    k_nh_pd = to_pd(kpi_agg(a, ["nhom"]).join(wmedian(a, ["nhom"], "dist_m", "kc_trung_vi_m"), "nhom")
                    .join(net_agg(a, ["nhom"]), "nhom")
                    .join(a.groupBy("nhom").agg(F.countDistinct("ma_tram").alias("so_xa")), "nhom"), ["nhom"])
    # G5: vung phuc vu tung tram
    k_tr_pd = to_pd(st.select("ma_tram", "ten_tram", "quan_huyen", "loai_dv")
                    .join(a.groupBy(F.col("tram_gan_nhat").alias("ma_tram"))
                          .agg(F.sum("pop").alias("dan_phuc_vu"), F.sum("pop60").alias("dan60_phuc_vu"), F.avg("dist_m").alias("kc_tb_m"),
                               F.count("*").alias("so_o_luoi")), "ma_tram", "left").fillna(0, ["dan_phuc_vu", "dan60_phuc_vu", "so_o_luoi"]),
                    ["ma_tram"])
    # G6: tong hop nam co so
    kpi_df = kpi_agg(a, []).crossJoin(wmedian(a, [], "dist_m", "kc_trung_vi_m")).crossJoin(net_agg(a, [])) \
        .crossJoin(wmedian(a.filter("dist_m > 500"), [], "detour", "he_so_vong_trung_vi")) \
        .crossJoin(wmedian(a, [], "net_time_min", "thoi_gian_trung_vi_phut"))
    ghi_ke_hoach(kpi_df, f"plan_kpi_{BASE_YEAR}.txt", f"truy van KPI chinh toan 56 xa nam {BASE_YEAR}")
    tp = kpi_df.toPandas().iloc[0]
    nh = k_nh_pd.set_index("nhom")
    gap = (nh.loc[N_PHUONG, "ty_le_trong_3km_pct"] - nh.loc[N_NUI, "ty_le_trong_3km_pct"]) if N_NUI in nh.index else float("nan")
    gap_dt = (nh.loc[N_PHUONG, "ty_le_trong_3km_mang_duong_pct"] - nh.loc[N_NUI, "ty_le_trong_3km_mang_duong_pct"]) if N_NUI in nh.index else float("nan")
    kq = []

    def k(ten, v, dv, loai, ghi=""):
        kq.append(dict(chi_so=ten, gia_tri=v, don_vi=dv, loai=loai, ghi_chu=ghi))
    B = BASE_YEAR
    k(f"Tổng dân 56 xã/phường năm {B} (WorldPop, ước tính)", round(tp.dan), "người", "nền", "A2")
    k(f"Tổng dân 60+ năm {B}", round(tp.dan_60), "người", "nền", "A3")
    k("Số trạm y tế", n_st, "trạm", "nền", "toạ độ nhập tay (A13)")
    k("Tỷ lệ dân trong 3 km đường chim bay tới trạm gần nhất", round(tp.ty_le_trong_3km_pct, 2), "%", "KPI chính", "ngưỡng 3 km là giả định A1")
    k("Tỷ lệ dân trong 3 km theo mạng đường", round(tp.ty_le_trong_3km_mang_duong_pct, 2), "%", "KPI chính (đường đi)", "A6")
    k("Tỷ lệ dân tới trạm trong 15 phút xe máy", round(tp.ty_le_toi_tram_15_phut_pct, 2), "%", "KPI chính (thời gian)", "A6")
    k("Hệ số vòng đường trung vị (mạng đường / chim bay, ô cách trạm > 500 m)", round(tp.he_so_vong_trung_vi, 3), "lần", "thuận tiện đường đi", "A6")
    k("Thời gian trung vị tới trạm (xe máy)", round(tp.thoi_gian_trung_vi_phut, 1), "phút", "thuận tiện đường đi", "A6")
    k("Tỷ lệ dân trong 2 km", round(tp["ty_le_trong_2km_pct"], 2), "%", "độ nhạy", "A1")
    k("Tỷ lệ dân trong 5 km", round(tp["ty_le_trong_5km_pct"], 2), "%", "độ nhạy", "A1")
    k("Dân bình quân mỗi trạm", round(tp.dan / n_st), "người/trạm", "KPI phụ")
    k("Khoảng cách trung vị (theo dân) tới trạm gần nhất", round(tp.kc_trung_vi_m), "m", "KPI phụ")
    k("Dân cách trạm gần nhất trên 5 km (vùng trắng)", round(tp.dan_tren_5km), "người", "KPI phụ")
    k("Dân 60+ cách trạm trên 5 km", round(tp.dan_60_tren_5km), "người", "KPI phụ")
    k("Tỷ lệ dân 60+ trong 3 km", round(tp.ty_le_60_trong_3km_pct, 2), "%", "KPI phụ", "A3")
    for g_ in (N_PHUONG, N_DB, N_NUI):
        if g_ in nh.index:
            k(f"Tỷ lệ dân trong 3 km – {g_}", round(nh.loc[g_, "ty_le_trong_3km_pct"], 2), "%", "KPI phụ", f"{int(nh.loc[g_, 'so_xa'])} đơn vị; A7")
    k("Chênh lệch phường – xã miền núi (3 km, chim bay)", round(gap, 2), "điểm %", "KPI phụ", "A7")
    k("Chênh lệch phường – xã miền núi (3 km, mạng đường)", round(gap_dt, 2), "điểm %", "KPI phụ", "A6, A7")
    kq_pd = pd.DataFrame(kq)

    # ================= 6. GHI KET QUA =================
    qall = quarantines[0]
    for q in quarantines[1:]:
        qall = qall.unionByName(q)
    qall = qall.cache()
    # quarantine/ chi giu LOI DU LIEU that; excluded/ giu dong ngoai pham vi (NoData, ngoai 56 xa...) - khong phai loi.
    # Luon ghi de toan bo (static) ke ca khi chon SPARK_PARTITION_OVERWRITE=dynamic: nguon het loi phai mat phan vung cu.
    ghi(qall.filter(F.col("loai") == LOI), "quarantine/dong_loi", "nguon", "static")
    ghi(qall.filter(F.col("loai") == NGOAI), "excluded/ngoai_pham_vi", "nguon", "static")
    for name, pdf in (("kpi_tong_hop", kq_pd.astype({"gia_tri": "float64"})), ("kpi_theo_nam", k_nam_pd), ("kpi_theo_xa_nam", k_xn_pd),
                      ("kpi_theo_xa", k_xa_pd), ("kpi_theo_nhom", k_nh_pd), ("tram_vung_phuc_vu", k_tr_pd)):
        ghi(spark.createDataFrame(pdf).coalesce(1), f"gold/{name}")
    n_qback = doc("quarantine/dong_loi", qall.schema).count()      # doc theo schema: van dung khi vung rong
    n_eback = doc("excluded/ngoai_pham_vi", qall.schema).count()
    n_success = sum(co_success(p) for p in da_ghi)

    q_sum = to_pd(qall.groupBy("loai", "nguon", "ly_do").count().withColumnRenamed("count", "so_dong"), ["loai", "nguon", "ly_do"])
    q_mau = to_pd(qall.withColumn("_r", F.row_number().over(Window.partitionBy("loai", "nguon", "ly_do").orderBy("khoa")))
                  .filter("_r <= 10").drop("_r"), ["loai", "nguon", "ly_do", "khoa"])
    cnt_pd = pd.DataFrame(counts)
    cnt_pd["ty_le_cach_ly_pct"] = (100 * cnt_pd.so_dong_cach_ly / cnt_pd.so_dong_tho).round(3)
    cnt_pd["ty_le_loi_du_lieu_pct"] = (100 * cnt_pd.so_dong_loi_du_lieu / cnt_pd.so_dong_tho).round(3)
    rd = {c: 3 for c in k_nam_pd.columns if c.endswith("_pct")}
    save_csv(kq_pd, TAB / "kpi_tong_hop_spark.csv", S, note=f"KPI chinh + phu nam {B}, tinh bang Spark")
    save_csv(k_nam_pd.round(rd).round({"kc_trung_vi_m": 1}), TAB / "kpi_theo_nam_spark.csv", S, note="56 tram giu nguyen qua cac nam (A14)")
    save_csv(k_xn_pd.round(3), TAB / "kpi_theo_xa_nam_spark.csv", S, note="xa x nam, 56 tram giu nguyen (A14)")
    save_csv(k_xa_pd.round(3), TAB / "kpi_theo_xa_spark.csv", S, note=f"theo xa nam {B}, kem mang duong")
    save_csv(k_nh_pd.round(3), TAB / "kpi_theo_nhom_spark.csv", S, note="phuong / xa dong bang / xa mien nui (A7)")
    save_csv(k_tr_pd.round(1), TAB / "tram_vung_phuc_vu_spark.csv", S, note="dan phuc vu boi tram gan nhat")
    save_csv(cnt_pd, TAB / "spark_so_dong_theo_nguon.csv", S,
             note="so dong tho / hop le / khong vao bang sach (= loi du lieu + ngoai pham vi) tung nguon")
    save_csv(q_sum, TAB / "spark_quarantine_tong_hop.csv", S,
             note="dong khong vao bang sach theo loai (LOI_DU_LIEU -> quarantine/, NGOAI_PHAM_VI -> excluded/), nguon, ly do")
    save_csv(q_mau, TAB / "spark_quarantine_mau.csv", S, note="toi da 10 dong mau moi (loai, nguon, ly do); ban day du o quarantine/ va excluded/")
    log.info("6. Da ghi curated / quarantine / excluded / gold tai %s (%d bang Parquet) va 9 bang CSV", zone, len(da_ghi))

    # ================= 7. DOI SOAT =================
    # (a) bao toan so dong: tho = hop le + loi + ngoai pham vi (theo dinh nghia) va doc lai tu vung luu tru
    add("Số dòng lỗi dữ liệu ghi ra vùng quarantine = tổng lỗi dữ liệu các nguồn", n_qback, int(cnt_pd.so_dong_loi_du_lieu.sum()),
        n_qback == int(cnt_pd.so_dong_loi_du_lieu.sum()), f"{zone}/quarantine/dong_loi")
    add("Số dòng ngoài phạm vi ghi ra vùng excluded = tổng ngoài phạm vi các nguồn", n_eback, int(cnt_pd.so_dong_ngoai_pham_vi.sum()),
        n_eback == int(cnt_pd.so_dong_ngoai_pham_vi.sum()), f"{zone}/excluded/ngoai_pham_vi (NoData của WorldPop, ngoài 56 xã – không phải lỗi)")
    add("Số bảng Parquet có cờ _SUCCESS = số bảng đã ghi", n_success, len(da_ghi), n_success == len(da_ghi),
        "Parquet không có giao dịch: thiếu _SUCCESS nghĩa là lần ghi bị ngắt")
    n_pop_ok = int(cnt_pd.set_index("nguon").loc["pop_100m_raw", "so_dong_hop_le"])
    add("Pixel × năm hợp lệ = dòng bảng sự kiện + dòng thuộc ô toàn 0", n_fact + n_zero_rows, n_pop_ok, n_fact + n_zero_rows == n_pop_ok)
    add("Số dòng bảng sự kiện đọc lại từ vùng curated = trước khi ghi", n_fact, n_fact_truoc, n_fact == n_fact_truoc, f"{zone}/curated/fact_dan_so_o_nam")
    # (b) bao toan tong dan: dan tho (pixel >= 0) = hop le + cach ly
    pf = pop_flag.filter("pop_raw >= 0").groupBy((F.col("_ly_do") == "").alias("ok")).agg(F.sum("pop_raw").alias("s")).toPandas()
    s_ok, s_bad = float(pf.loc[pf.ok, "s"].sum()), float(pf.loc[~pf.ok, "s"].sum())
    s_raw = float(pop_raw.filter("pop_raw >= 0").agg(F.sum("pop_raw")).first()[0])
    s_fact = float(fact.agg(F.sum("pop")).first()[0])
    add("Tổng dân thô (pixel ≥ 0, mọi năm) = hợp lệ + không vào bảng sạch", s_ok + s_bad, s_raw, abs(s_ok + s_bad - s_raw) <= REL_TOL * s_raw,
        f"hợp lệ {s_ok:.0f}; ngoài phạm vi (ngoài 56 xã) {s_bad:.0f}")
    add("Tổng dân bảng sự kiện = tổng dân hợp lệ", s_fact, s_ok, abs(s_fact - s_ok) <= REL_TOL * s_ok)
    # (c) Spark vs pandas
    add("Số trạm hợp lệ", n_st, 56, n_st == 56)
    sp = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    m = st.toPandas().merge(sp, on="ma_tram", suffixes=("_s", "_p"))
    dxy = float(((m.x_m_s - m.x_m_p) ** 2 + (m.y_m_s - m.y_m_p) ** 2).pow(0.5).max())
    add("Toạ độ trạm sau chiếu UTM: Spark vs pandas/pyproj, lệch lớn nhất (m)", round(dxy, 4), 0.0, len(m) == 56 and dxy <= 0.01)
    g100 = pd.read_parquet(INTERIM / "grid_100m.parquet", columns=["cell_id"])
    add("Số ô lưới có dân trong 56 xã", n_cells, len(g100), n_cells == len(g100))
    agg = fact.groupBy("ma_tram", "year").agg(F.sum("pop").alias("pop"), F.sum("pop60").alias("pop60")).toPandas()
    pdcs = pd.read_csv(TAB / "commune_series.csv", encoding="utf-8-sig")
    mm = agg.merge(pdcs, on=["ma_tram", "year"], suffixes=("_spark", "_pandas"))
    add("Số cặp (xã, năm) khớp khoá", len(mm), len(pdcs), len(mm) == len(pdcs) == len(agg))
    for c in ("pop", "pop60"):
        rel = float(((mm[f"{c}_spark"] - mm[f"{c}_pandas"]).abs() / mm[f"{c}_pandas"].abs().clip(lower=1e-9)).max())
        add(f"Tổng {c} theo xã × năm: sai số tương đối lớn nhất", rel, 0.0, rel <= REL_TOL)
    ga = pd.read_parquet(INTERIM / "grid_access.parquet", columns=["cell_id", "dist_m", "tram_gan_nhat"])
    nd = near.toPandas().merge(ga, on="cell_id", suffixes=("_s", "_p"))
    dmax = float((nd.dist_m_s - nd.dist_m_p).abs().max())
    add("Khoảng cách tới trạm gần nhất: Spark (nối chéo) vs pandas (KDTree), lệch lớn nhất (m)", round(dmax, 4), 0.0,
        len(nd) == len(ga) and dmax <= DIST_TOL)
    same = float((nd.tram_gan_nhat_s == nd.tram_gan_nhat_p).mean() * 100)
    add("Tỷ lệ ô có cùng trạm gần nhất (%)", round(same, 3), 100.0, same >= 99.9, "khác nhau chỉ khi hai trạm cách đều")
    kp = pd.read_csv(TAB / "kpi_by_commune.csv", encoding="utf-8-sig")
    kc = k_xa_pd.merge(kp, on="ma_tram", suffixes=("_s", "_p"))
    for c, tol in (("ty_le_trong_3km_pct", PCT_TOL), ("ty_le_60_trong_3km_pct", PCT_TOL), ("ty_le_trong_3km_mang_duong_pct", PCT_TOL),
                   ("kc_trung_vi_m", 1.0), ("dan_tren_5km", 1.0)):
        d = float((kc[f"{c}_s"] - kc[f"{c}_p"]).abs().max())
        add(f"Theo xã năm {B} – {c}: Spark vs pandas, lệch lớn nhất", round(d, 4), 0.0, len(kc) == 56 and d <= tol, f"ngưỡng {tol}")
    kg = pd.read_csv(TAB / "kpi_by_group.csv", encoding="utf-8-sig")
    kgm = k_nh_pd.merge(kg, on="nhom", suffixes=("_s", "_p"))
    d = float((kgm.ty_le_trong_3km_pct_s - kgm.ty_le_trong_3km_pct_p).abs().max())
    add("Theo nhóm phường/xã – tỷ lệ 3 km: Spark vs pandas, lệch lớn nhất (điểm %)", round(d, 4), 0.0, len(kgm) == len(kg) and d <= PCT_TOL)
    ko = pd.read_csv(TAB / "kpi_overview.csv", encoding="utf-8-sig").set_index("chi_so").gia_tri
    for lab, val, tol in (("Tỷ lệ dân trong 3 km (đường chim bay)", tp.ty_le_trong_3km_pct, PCT_TOL),
                          ("Tỷ lệ dân 60+ trong 3 km", tp.ty_le_60_trong_3km_pct, PCT_TOL),
                          ("Tỷ lệ dân trong 3 km theo mạng đường", tp.ty_le_trong_3km_mang_duong_pct, PCT_TOL),
                          ("Khoảng cách trung vị (theo dân) tới trạm gần nhất", tp.kc_trung_vi_m, 1.0),
                          ("Dân cách trạm gần nhất > 5 km (vùng trắng)", tp.dan_tren_5km, 1.0),
                          ("Dân bình quân mỗi trạm (tổng dân / số trạm)", tp.dan / n_st, 1.0)):
        if lab in ko.index:
            pv = float(ko[lab])
            add(f"Toàn 56 xã – {lab}", round(float(val), 3), pv, abs(float(val) - pv) <= tol, f"ngưỡng {tol}")
        else:
            add(f"Toàn 56 xã – {lab}", round(float(val), 3), None, False, "không thấy chỉ số trong kpi_overview.csv")

    out = pd.DataFrame(checks)
    save_csv(out, TAB / "doi_soat_spark.csv", S)
    for r in checks:
        log.info("7. %s | spark=%s doi_chieu=%s -> %s", r["phep_doi_soat"], r["spark"], r["pandas_hoac_nguon"], r["ket_qua"])
    log.info("Doi soat: %d phep, %d LECH. Thoi gian %.0f giay", len(out), (out.ket_qua == "LỆCH").sum(), time.time() - t0)
    spark.stop()


if __name__ == "__main__":
    main()
