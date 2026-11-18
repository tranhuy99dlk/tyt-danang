# -*- coding: utf-8 -*-
"""16 - SO SANH DINH DANG LUU TRU: cung mot truy van KPI, doc tu 4 cach luu khac nhau; do so file, so byte,
so dong buoc quet that su doc, va ghi ke hoach thuc thi (explain formatted) cua tung cach.

VI SAO CO BUOC NAY
  Ke hoach cua truy van KPI that (logs/plan_kpi_<nam>.txt, buoc 11) doc bang o_luoi_tiep_can_<nam>: bang da cat
  san mot nam luc ghi, KHONG phan vung, va doc qua cache(). Vi vay ke hoach do KHONG co PartitionFilters va KHONG
  co BroadcastHashJoin (chi co BroadcastNestedLoopJoin do crossJoin cac ket qua mot dong). Khong duoc lay log do
  lam bang chung cho "cat tia phan vung" hay "broadcast hash join". Buoc 16 dung mot truy van rieng tren bang su
  kien phan vung theo year de DO va MINH HOA dung cac co che nay:
    (1) cat tia phan vung la dac tinh cua cach chia THU MUC (year=...): CSV chia thu muc cung co;
    (2) "PushedFilters" xuat hien ca voi CSV, nhung CSV van phai doc het file (khong co thong ke min/max);
    (3) Parquet loc theo KHOI (row group): buoc quet tra ca dong le, nut Filter phia sau moi loai;
    (4) cache() truoc khi loc lam mat PartitionFilters: loc chay tren ban trong bo nho, khong con cat thu muc;
    (5) broadcast bang 56 xa -> BroadcastHashJoin: bang lon khong bi shuffle.

TRUY VAN NGHIEP VU (giong kpi_theo_nhom_spark.csv): ty le dan trong 3 km theo nhom xa, nam BASE_YEAR.
  4 phuong an: Parquet phan vung (doc thang) | CSV phang | CSV phan vung theo year | Parquet phan vung qua cache().
  Ca 4 phai cho cung ket qua va khop kpi_theo_nhom_spark.csv (nguong 0,001 diem % vi bang gold lam tron 3 chu so).

DAU VAO : <vung>/curated/fact_o_nam_khoang_cach (Parquet phan vung year), <vung>/curated/dim_xa  - do buoc 11 ghi.
          <vung> = data/spark/ (mac dinh) hoac HDFS_URI neu dat bien moi truong.
DAU RA  : outputs/tables/so_sanh_parquet_csv.csv   (1 dong / phuong an)
          logs/plan_pruning_<nam>.txt               (ke hoach thuc thi day du cua 4 phuong an)
          <vung>/thu_nghiem/csv_phang, <vung>/thu_nghiem/csv_phan_vung  (ban sao CSV de so sanh; ghi de moi lan chay)
CHAY    : sau buoc 11, trong cung container Spark (scripts/run_spark.sh), hoac
          HDFS_URI=hdfs://namenode:9000/mis5007 python scripts/16_so_sanh_dinh_dang.py

GHI CHU (#Truthmode)
  - "dung_luong_file_quet_byte" = tong dung luong cac file Spark CHON de quet (chi so filesSize). Parquet doc theo cot
    nen so byte thuc doc con nho hon; CSV phai doc ca dong nen bang dung so nay.
  - Du lieu nho (12 MB Parquet cho 10 nam): chenh lech thoi gian khong co y nghia nen KHONG do thoi gian; chi do
    so file, so byte, so dong - cac dai luong khong phu thuoc may.
"""
import contextlib
import io
import os
import re
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = str(Path(__file__).resolve().parent)
sys.path.insert(0, SCRIPTS)
os.environ["PYTHONPATH"] = SCRIPTS + os.pathsep + os.environ.get("PYTHONPATH", "")
from common import TAB, LOGS, SPARK_OUT, BASE_YEAR, MAIN_KM, get_logger, save_csv, register  # noqa: E402

S = "16_so_sanh_dinh_dang"
log = get_logger(S)
R = MAIN_KM * 1000
TOL_GOLD = 0.001          # diem %: kpi_theo_nhom_spark.csv lam tron 3 chu so
TOL_GIUA = 1e-9           # diem %: 4 phuong an phai trung nhau gan nhu tuyet doi
COT_NHOM = {"Phường": "ty_le_3km_phuong", "Xã đồng bằng/ven đô": "ty_le_3km_dong_bang",
            "Xã miền núi (địa hình)": "ty_le_3km_mien_nui"}


def _ensure_pyspark():
    """Giong buoc 11: khi goi bang `python` trong image jupyter/pyspark-notebook, tu them $SPARK_HOME/python."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        import glob
        sh = os.environ.get("SPARK_HOME", "")
        paths = [f"{sh}/python"] + glob.glob(f"{sh}/python/lib/py4j-*-src.zip")
        sys.path[:0] = paths
        os.environ["PYTHONPATH"] = os.pathsep.join(paths + [os.environ.get("PYTHONPATH", "")])


def doc_khoi(plan, ten_nut):
    """Tach phan chi tiet cua cac nut co ten bat dau bang ten_nut trong explain('formatted')."""
    khoi = re.split(r"\n(?=\(\d+\) )", plan)
    return [k for k in khoi if re.match(rf"\(\d+\) {ten_nut}", k)]


def dac_trung_ke_hoach(plan):
    """Rut cac dau hieu can doc trong ke hoach: loc phan vung, day bo loc, loai join, so lan shuffle."""
    quet_fact = [k for k in doc_khoi(plan, "Scan") if "fact_o_nam_khoang_cach" in k or "csv_ph" in k]
    k0 = quet_fact[0] if quet_fact else ""
    pf = re.search(r"PartitionFilters: (\[.*\])", k0)
    pu = re.search(r"PushedFilters: (\[.*\])", k0)
    cay = plan.split("\n\n")[0]
    if "BroadcastHashJoin" in cay:
        join = "BroadcastHashJoin"
    elif "SortMergeJoin" in cay:
        join = "SortMergeJoin"
    elif "BroadcastNestedLoopJoin" in cay:
        join = "BroadcastNestedLoopJoin"
    else:
        join = "khong co"
    return dict(partition_filters=pf.group(1) if pf else "",
                pushed_filters=pu.group(1) if pu else "",
                loai_join=join,
                so_exchange=len(re.findall(r"^\(\d+\) Exchange", plan, flags=re.M)),
                doc_qua_bo_nho_dem="InMemoryTableScan" in cay)


def main():
    _ensure_pyspark()
    hdfs = os.environ.get("HDFS_URI")
    if hdfs:
        os.environ.setdefault("HADOOP_USER_NAME", "root")
    from pyspark.sql import SparkSession, functions as F, types as T

    spark = (SparkSession.builder.appName("mis5007_so_sanh_dinh_dang")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
             .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
             .config("spark.ui.enabled", os.environ.get("SPARK_UI", "false"))
             .config("spark.hadoop.dfs.replication", os.environ.get("HDFS_REPLICATION", "1"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    zone = hdfs.rstrip("/") if hdfs else "file://" + str(SPARK_OUT)
    jvm, hconf = spark.sparkContext._jvm, spark.sparkContext._jsc.hadoopConfiguration()
    log.info("Spark %s, master=%s | cac vung=%s | AQE=%s | nguong tu broadcast=%s", spark.version,
             spark.sparkContext.master, zone, spark.conf.get("spark.sql.adaptive.enabled"),
             spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    def hp(p):
        return jvm.org.apache.hadoop.fs.Path(p)

    def co_success(p):
        q = hp(f"{p}/_SUCCESS")
        return bool(q.getFileSystem(hconf).exists(q))

    def doc_parquet(p):
        if not co_success(p):
            raise RuntimeError(f"Thieu {p}/_SUCCESS: chay lai buoc 11 truoc buoc 16.")
        return spark.read.parquet(p)

    def dung_luong(p):
        """Tong dung luong va so file du lieu (part-*), khong tinh .crc / _SUCCESS."""
        q = hp(p)
        it = q.getFileSystem(hconf).listFiles(q, True)
        s = n = 0
        while it.hasNext():
            f = it.next()
            if f.getPath().getName().startswith("part-"):
                s += int(f.getLen())
                n += 1
        return s, n

    def ke_hoach(df):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            df.explain("formatted")
        return buf.getvalue()

    # ---- dau vao + ban sao CSV cung noi dung ----
    p_pq = f"{zone}/curated/fact_o_nam_khoang_cach"
    fact = doc_parquet(p_pq)
    xa = doc_parquet(f"{zone}/curated/dim_xa").select("ma_tram", "nhom")
    p_csv, p_csvp = f"{zone}/thu_nghiem/csv_phang", f"{zone}/thu_nghiem/csv_phan_vung"
    fact.write.mode("overwrite").option("header", True).csv(p_csv)
    fact.write.mode("overwrite").option("header", True).partitionBy("year").csv(p_csvp)
    sch = fact.schema                                            # schema tuong minh, khong suy kieu
    sch_khong_nam = T.StructType([f for f in sch.fields if f.name != "year"])
    fact_csv = spark.read.option("header", True).schema(sch).csv(p_csv)
    fact_csvp = spark.read.option("header", True).schema(sch_khong_nam).csv(p_csvp)   # year lay tu ten thu muc
    log.info("Da ghi ban sao CSV tai %s/thu_nghiem/", zone)
    # LUU Y: cache() mot DataFrame lam MOI truy van co cung ke hoach doc (ke ca DataFrame khac, vi du `fact`) dung ban
    # trong bo nho. Vi vay P4 phai chay CUOI, va bo nho dem duoc tao ngay truoc P4 roi giai phong sau do.

    def truy_van(df):
        return (df.filter((F.col("year") == BASE_YEAR) & (F.col("pop") > 0))           # narrow
                .join(F.broadcast(xa), "ma_tram")                                       # broadcast hash join
                .groupBy("nhom")                                                        # wide (shuffle)
                .agg((F.sum(F.when(F.col("dist_m") <= R, F.col("pop")).otherwise(0.0)) / F.sum("pop") * 100)
                     .alias("ty_le_trong_3km_pct"), F.sum("pop").alias("dan")))

    pa = [  # (ma, mo ta, dinh dang, phan vung, df, duong dan de do dung luong)
        ("P1", "Parquet chia thư mục theo năm, đọc thẳng", "Parquet", True, fact, p_pq),
        ("P2", "CSV một thư mục phẳng (như vùng landing)", "CSV", False, fact_csv, p_csv),
        ("P3", "CSV chia thư mục theo năm", "CSV", True, fact_csvp, p_csvp),
        ("P4", "Parquet chia thư mục theo năm, nhưng cache() trước khi lọc", "Parquet", True, None, p_pq),
    ]

    n_sau_loc = fact.filter((F.col("year") == BASE_YEAR) & (F.col("pop") > 0)).count()
    gold = pd.read_csv(TAB / "kpi_theo_nhom_spark.csv", encoding="utf-8-sig").set_index("nhom")["ty_le_trong_3km_pct"]
    plans, rows, kq = [], [], {}
    n_cache = None
    for ma, mota, dd, pv, df, path in pa:
        if df is None:                                            # P4: nap bo nho dem - lan nap quet ca 10 phan vung
            df = doc_parquet(p_pq).cache()
            n_cache = df.count()
        q = truy_van(df)
        plan = ke_hoach(q)                                        # ke hoach voi cau hinh mac dinh (AQE bat)
        plans.append(f"{'=' * 100}\n# {ma} - {mota}\n{'=' * 100}\n{plan}")
        # chi so quet thuc te: tat AQE de doc metric tren ke hoach vat ly cuoi cung
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        q2 = truy_van(df)
        res = q2.toPandas()
        leaves = q2._jdf.queryExecution().executedPlan().collectLeaves()
        so_file = byte_q = dong_q = None
        for i in range(leaves.size()):
            nd = leaves.apply(i)
            m = nd.metrics()
            ten = nd.nodeName()
            try:                                                  # bo qua nut quet bang nho dim_xa
                if "dim_xa" in str(nd.metadata()):
                    continue
            except Exception:                                     # InMemoryTableScan khong co metadata()
                pass

            def g(k):
                return int(m.apply(k).value()) if m.contains(k) else None
            if ten.startswith("Scan") or ten.startswith("InMemoryTableScan"):
                so_file, byte_q, dong_q = g("numFiles"), g("filesSize"), g("numOutputRows")
                break
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        tl = res.set_index("nhom")["ty_le_trong_3km_pct"]
        kq[ma] = tl
        dl, nf = dung_luong(path)
        lech_gold = float((tl - gold.reindex(tl.index)).abs().max())
        row = dict(phuong_an=ma, mo_ta=mota, dinh_dang=dd, chia_thu_muc_theo_nam=pv,
                   dung_luong_ca_10_nam_byte=dl, so_file_ca_10_nam=nf,
                   so_file_quet=so_file, dung_luong_file_quet_byte=byte_q, so_dong_ra_tu_buoc_quet=dong_q,
                   so_dong_sau_loc=n_sau_loc, **dac_trung_ke_hoach(plan))
        for nhom, cot in COT_NHOM.items():
            row[cot] = round(float(tl.get(nhom, float("nan"))), 6)
        row["lech_voi_kpi_theo_nhom_spark"] = round(lech_gold, 6)
        row["khop_kpi_theo_nhom_spark"] = lech_gold <= TOL_GOLD
        rows.append(row)

    if n_cache is not None:
        df.unpersist()
    out = pd.DataFrame(rows)
    for c in ("so_file_quet", "dung_luong_file_quet_byte", "so_dong_ra_tu_buoc_quet"):
        out[c] = out[c].astype("Int64")                           # P4 doc tu bo nho dem: khong co chi so file
    b1 = out.loc[out.phuong_an == "P1", "dung_luong_file_quet_byte"].iloc[0]
    out["byte_quet_gap_bao_nhieu_lan_P1"] = (out.dung_luong_file_quet_byte / b1).round(2)
    out["ghi_chu"] = ""
    out.loc[out.phuong_an == "P4", "ghi_chu"] = (f"Đọc từ bộ nhớ đệm; lần nạp cache() trước đó đã quét cả "
                                                 f"{int(out.loc[out.phuong_an == 'P1', 'so_file_ca_10_nam'].iloc[0])} file, {n_cache} dòng")
    lech_giua = max(float((kq[m] - kq["P1"].reindex(kq[m].index)).abs().max()) for m in kq)
    trang_thai = "ĐẠT" if (lech_giua <= TOL_GIUA and out.khop_kpi_theo_nhom_spark.all()) else "LỆCH"
    save_csv(out, TAB / "so_sanh_parquet_csv.csv", S,
             note=f"cung truy van KPI 3 km theo nhom nam {BASE_YEAR} tren 4 cach luu; {trang_thai}")

    f = LOGS / f"plan_pruning_{BASE_YEAR}.txt"
    f.write_text(f"# Ke hoach thuc thi cua cung mot truy van KPI tren 4 cach luu (buoc 16), Spark {spark.version}\n"
                 f"# Truy van: ty le dan trong 3 km theo nhom xa, nam {BASE_YEAR}; broadcast bang dim_xa\n\n"
                 + "\n".join(plans), encoding="utf-8")
    register(f, S, rows=sum(p.count("\n") for p in plans), note="explain formatted cua 4 phuong an luu tru")

    for r in rows:
        log.info("%s %-58s | file quet %s | byte %s | dong ra tu buoc quet %s | PartitionFilters=%s | join=%s | shuffle=%d",
                 r["phuong_an"], r["mo_ta"], r["so_file_quet"], r["dung_luong_file_quet_byte"], r["so_dong_ra_tu_buoc_quet"],
                 "co" if r["partition_filters"] else "KHONG", r["loai_join"], r["so_exchange"])
    log.info("Lech lon nhat giua 4 phuong an: %.2e diem %%; khop kpi_theo_nhom_spark.csv: %s -> %s",
             lech_giua, bool(out.khop_kpi_theo_nhom_spark.all()), trang_thai)
    if trang_thai != "ĐẠT":
        log.warning("LECH: ket qua 4 phuong an khong trung nhau hoac khong khop bang gold - xem %s", TAB / "so_sanh_parquet_csv.csv")
    spark.stop()


if __name__ == "__main__":
    main()
