# -*- coding: utf-8 -*-
"""Khung dùng chung cho pipeline Spark 5 bước: tạo SparkSession, đọc Bronze, ghi/đọc Parquet an toàn, ghi kế hoạch
thực thi, ghi phép đối soát, đo thời gian từng bước và gom kết quả để Bước 5 ghi một lần.
Mọi bước (b1_doc … b5_ghi) nhận một đối tượng `NguCanh` (ngữ cảnh) tạo ở đây.

Hai cách chạy:
  - Trên máy (mặc định, chạy được bằng F5 trong VS Code): Spark local[*], hồ dữ liệu ở data/lake/.
  - Trên cụm Docker của học phần: đặt HDFS_URI=hdfs://namenode:9000/mis5007 -> bronze/silver/gold nằm trên HDFS;
    thêm SPARK_MASTER=yarn để executor chạy trên YARN (cần chạy yarn_python_env.sh một lần trước).
Biến môi trường tuỳ chọn: SPARK_DRIVER_MEMORY (mặc định 4g), SPARK_SHUFFLE_PARTITIONS (16), SPARK_UI (false),
HDFS_REPLICATION (1), SPARK_PARTITION_OVERWRITE (static), SPARK_EXECUTOR_INSTANCES/CORES/MEMORY, YARN_PY_ARCHIVE.
"""
import contextlib  # chuyển hướng tạm thời những gì in ra màn hình (để bắt kế hoạch thực thi)
import glob  # tìm file theo mẫu
import io  # bộ đệm chuỗi trong bộ nhớ
import os  # biến môi trường
import sys  # đường dẫn tìm module, trình thông dịch Python đang chạy
import time  # đo thời gian, lấy giờ
from pathlib import Path  # đường dẫn

import pandas as pd  # bảng dữ liệu trên driver (hàng chờ bảng kết quả)

SCRIPTS = str(Path(__file__).resolve().parent)  # thư mục scripts/2_spark_5_buoc/ (các module Spark)
CHUNG = str(Path(__file__).resolve().parents[1] / "chung")  # thư mục scripts/chung/ (common.py, hinh_hoc.py)
for _d in (SCRIPTS, CHUNG):  # hai thư mục chứa module của dự án
    if _d not in sys.path:  # nếu chưa có trong đường dẫn tìm module
        sys.path.insert(0, _d)  # thêm vào để import được
DUONG_MODULE = CHUNG + os.pathsep + SCRIPTS  # chuỗi đường dẫn module truyền cho tiến trình Python con của Spark
from common import ROOT, LAKE, LOGS, get_logger, set_java_home  # noqa: E402  gốc dự án, hồ dữ liệu, thư mục log, hàm tiện ích


def _dam_bao_pyspark():  # bảo đảm import được pyspark (trong container jupyter, pyspark nằm ở $SPARK_HOME/python)
    try:  # thử import
        import pyspark  # noqa: F401  có sẵn (ví dụ trong .venv trên máy)
    except ImportError:  # không có -> tự thêm đường dẫn của Spark
        sh = os.environ.get("SPARK_HOME", "")  # thư mục cài Spark
        paths = [f"{sh}/python"] + glob.glob(f"{sh}/python/lib/py4j-*-src.zip")  # thư viện Python của Spark và py4j
        sys.path[:0] = paths  # thêm lên đầu đường dẫn tìm module
        os.environ["PYTHONPATH"] = os.pathsep.join(paths + [os.environ.get("PYTHONPATH", "")])  # cho cả tiến trình con


class NguCanh:  # "ngữ cảnh" của một lần chạy pipeline: SparkSession + các hàm đọc/ghi + sổ đối soát
    def __init__(self, ten: str):  # khởi tạo; `ten` là tên bước dùng cho log
        self.log = get_logger(ten)  # bộ ghi log
        self.t0 = time.time()  # mốc thời gian bắt đầu
        set_java_home()  # tìm Java 17 nếu chưa đặt JAVA_HOME
        _dam_bao_pyspark()  # bảo đảm import được pyspark
        os.environ["PYTHONPATH"] = DUONG_MODULE + os.pathsep + os.environ.get("PYTHONPATH", "")  # tiến trình Python con của Spark cũng import được module dự án
        self.hdfs = os.environ.get("HDFS_URI")  # địa chỉ HDFS (rỗng = chạy trên máy)
        if self.hdfs:  # dùng HDFS
            os.environ.setdefault("HADOOP_USER_NAME", "root")  # cụm HDFS của lớp bật kiểm tra quyền -> ghi bằng user root
        from pyspark.sql import SparkSession, Window, functions as F, types as T  # các thành phần Spark SQL

        self.F, self.T, self.Window = F, T, Window  # lưu để các giai đoạn dùng: F = hàm cột, T = kiểu dữ liệu, Window = cửa sổ
        master = os.environ.get("SPARK_MASTER", "local[*]")  # nơi chạy executor: local[*] = mọi nhân của máy này
        self.tren_yarn = master.startswith("yarn")  # có chạy trên YARN không
        if self.tren_yarn and not self.hdfs:  # YARN mà không có HDFS
            raise SystemExit("SPARK_MASTER=yarn can HDFS_URI: executor tren NodeManager khong doc duoc file tren may nay.")  # dừng với thông báo rõ
        if self.tren_yarn:  # chuẩn bị cho YARN
            conf_dir = os.environ.setdefault("HADOOP_CONF_DIR", str(ROOT / "docker" / "hadoop-conf"))  # cấu hình cụm
            os.environ.setdefault("YARN_CONF_DIR", conf_dir)  # cấu hình YARN
            os.environ["PYSPARK_PYTHON"] = "./pyenv/bin/python"  # executor dùng Python trong gói gửi kèm (yarn_python_env.sh)
        else:  # chạy trên máy
            os.environ["PYSPARK_PYTHON"] = sys.executable  # tiến trình con dùng ĐÚNG Python đang chạy (.venv), tránh lệch phiên bản
            os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable  # driver cũng vậy
        b = (SparkSession.builder.appName("mis5007_tram_y_te")  # bắt đầu cấu hình phiên Spark, đặt tên ứng dụng
             .master(master)  # nơi chạy
             .config("spark.sql.session.timeZone", "UTC")  # múi giờ UTC cho mọi phép tính ngày giờ
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g" if self.tren_yarn else "4g"))  # bộ nhớ driver
             .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))  # số phân vùng sau shuffle = 16 (thiết kế), tránh nhiều file nhỏ
             .config("spark.sql.maxPlanStringLength", "2000")  # rút gọn chuỗi mô tả kế hoạch trong log
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # dùng Apache Arrow để chuyển bảng pandas <-> Spark nhanh
             .config("spark.ui.enabled", os.environ.get("SPARK_UI", "false"))  # tắt giao diện web mặc định (bật bằng SPARK_UI=true)
             .config("spark.ui.showConsoleProgress", "false")  # không in thanh tiến độ [Stage ...] làm rối log
             .config("spark.ui.retainedJobs", "100000")  # nhớ đủ mọi job để đếm số job Spark của từng bước (bảng thoi_gian_5_buoc.csv)
             .config("spark.hadoop.dfs.replication", os.environ.get("HDFS_REPLICATION", "1"))  # cụm chỉ có 1 DataNode -> hệ số nhân bản 1
             .config("spark.sql.sources.partitionOverwriteMode", os.environ.get("SPARK_PARTITION_OVERWRITE", "static")))  # ghi đè cả bảng (an toàn khi phạm vi năm đổi)
        if self.tren_yarn:  # cấu hình riêng cho YARN
            b = (b.config("spark.archives", os.environ.get("YARN_PY_ARCHIVE", "hdfs://namenode:9000/mis5007/apps/pyspark_env.tar.gz#pyenv"))  # gói Python cho executor
                 .config("spark.executor.instances", os.environ.get("SPARK_EXECUTOR_INSTANCES", "2"))  # số executor
                 .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))  # số nhân mỗi executor
                 .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))  # bộ nhớ mỗi executor: 1g (1536m làm máy ảo Docker 7,7 GB hết RAM, executor bị giết – đo 26/11/2026)
                 .config("spark.yarn.am.memory", "512m"))  # bộ nhớ ApplicationMaster
        else:  # cấu hình riêng khi chạy trên máy
            b = (b.config("spark.driver.host", "127.0.0.1")  # gắn driver vào địa chỉ nội bộ (tránh lỗi phân giải tên máy trên macOS)
                 .config("spark.driver.bindAddress", "127.0.0.1")  # (tiếp)
                 .config("spark.executorEnv.PYTHONPATH", DUONG_MODULE))  # tiến trình con import được module của dự án
        self.spark = b.getOrCreate()  # tạo (hoặc lấy) phiên Spark
        self.spark.sparkContext.setLogLevel("ERROR")  # chỉ in lỗi của Spark, bớt rối màn hình
        if self.tren_yarn:  # trên YARN: gửi module dự án cho executor
            for f in (Path(CHUNG) / "common.py", Path(CHUNG) / "hinh_hoc.py", Path(SCRIPTS) / "b4_du_bao.py"):  # module chứa hàm chạy trên executor (UDF, applyInPandas)
                self.spark.sparkContext.addPyFile(str(f))  # gửi kèm
        self.goc = self.hdfs.rstrip("/") if self.hdfs else "file://" + str(LAKE)  # gốc hồ dữ liệu: HDFS hoặc thư mục data/lake
        jvm = self.spark.sparkContext._jvm  # cầu nối tới máy ảo Java của Spark
        self._Path = jvm.org.apache.hadoop.fs.Path  # lớp Path của Hadoop (dùng chung cho file máy và HDFS)
        self._hconf = self.spark.sparkContext._jsc.hadoopConfiguration()  # cấu hình Hadoop hiện hành
        self.da_ghi = []  # danh sách mọi bảng Parquet đã ghi trong lần chạy (để kiểm tra _SUCCESS)
        self.doi_soat = []  # danh sách các phép đối soát
        self.qa = []  # danh sách kiểm tra chất lượng (qa_summary.csv)
        self.so_dong = []  # số dòng thô / hợp lệ / loại bỏ theo từng nguồn
        self.loai_bo = []  # các DataFrame dòng bị loại (quarantine + excluded) của từng nguồn
        self.csv_cho = {}  # hàng chờ bảng CSV: tên file -> (bảng pandas, script tạo ra, ghi chú); Bước 5 mới ghi
        self.gold_cho = []  # hàng chờ bảng Gold: (tên bảng, DataFrame, cột phân vùng); Bước 5 mới ghi
        self.thoi_gian = []  # thời gian và số bảng của từng bước (bảng thoi_gian_5_buoc.csv)
        self.log.info("Spark %s, master=%s | ho du lieu=%s", self.spark.version, self.spark.sparkContext.master, self.goc)  # in thông tin

    # ------------------------- đường dẫn và hệ thống file -------------------------
    def _fs(self, p: str):  # lấy hệ thống file (máy hoặc HDFS) ứng với một đường dẫn
        return self._Path(p).getFileSystem(self._hconf)  # Hadoop tự chọn theo tiền tố file:// hay hdfs://

    def ton_tai(self, p: str) -> bool:  # đường dẫn có tồn tại không
        return bool(self._fs(p).exists(self._Path(p)))  # hỏi hệ thống file

    def bronze(self, nguon: str, bang: str) -> str:  # đường dẫn file Bronze của lần nạp mới nhất
        thu_muc = f"{self.goc}/bronze/source_system={nguon}"  # thư mục của nguồn
        st = self._fs(thu_muc).listStatus(self._Path(thu_muc))  # liệt kê các thư mục con ingestion_date=...
        ngay = sorted(str(s.getPath().getName()) for s in st if str(s.getPath().getName()).startswith("ingestion_date="))  # các lần nạp, sắp tăng
        if not ngay:  # chưa nạp
            raise FileNotFoundError(f"Chua co Bronze {thu_muc} - chay 01b/01c truoc")  # báo lỗi
        return f"{thu_muc}/{ngay[-1]}/{bang}"  # lần nạp mới nhất

    # ------------------------- ghi / đọc Parquet an toàn -------------------------
    def ghi(self, df, bang: str, part=None, che_do=None):  # ghi DataFrame ra Parquet trong hồ dữ liệu (ghi đè)
        w = df.write.mode("overwrite")  # chế độ ghi đè
        if che_do:  # nếu chỉ định cách ghi đè phân vùng
            w = w.option("partitionOverwriteMode", che_do)  # áp dụng (static/dynamic)
        if part:  # có cột phân vùng
            w = w.partitionBy(*([part] if isinstance(part, str) else part))  # chia thư mục theo cột (ví dụ year=2024/ma_quan=hai_chau)
        w.option("compression", "snappy").parquet(f"{self.goc}/{bang}")  # ghi Parquet nén Snappy
        self.da_ghi.append(f"{self.goc}/{bang}")  # nhớ để kiểm tra _SUCCESS

    def co_success(self, p: str) -> bool:  # Parquet không có giao dịch: file _SUCCESS chỉ có khi ghi xong trọn vẹn
        return self.ton_tai(f"{p}/_SUCCESS")  # kiểm tra file cờ

    def doc(self, bang: str, schema=None):  # đọc một bảng Parquet đã ghi, dừng nếu thiếu _SUCCESS
        full = f"{self.goc}/{bang}"  # đường dẫn đầy đủ
        if not self.co_success(full):  # chưa ghi xong / bị ngắt giữa chừng
            raise RuntimeError(f"Thieu {full}/_SUCCESS: lan ghi chua xong - khong doc du lieu do dang.")  # dừng ngay
        r = self.spark.read if schema is None else self.spark.read.schema(schema)  # bộ đọc (có schema nếu chỉ định)
        return r.parquet(full)  # đọc Parquet

    def luu(self, df, bang: str, part=None):  # ghi rồi ĐỌC LẠI: cắt chuỗi phụ thuộc, bước sau tính từ dữ liệu đã lưu
        self.ghi(df, bang, part)  # ghi
        return self.doc(bang).cache()  # đọc lại và giữ trong bộ nhớ đệm

    def tao_df(self, pdf):  # đưa một bảng pandas (nhỏ) lên Spark
        pdf = pdf.copy()  # bản sao để không sửa bảng gốc
        for c in pdf.columns:  # duyệt từng cột
            if str(pdf[c].dtype) in ("str", "string"):  # cột chuỗi kiểu mới của pandas 3 (Arrow đọc lỗi với PySpark 3.5)
                pdf[c] = pdf[c].astype(object)  # đổi về kiểu object truyền thống
        return self.spark.createDataFrame(pdf)  # tạo DataFrame Spark (dùng Arrow cho nhanh)

    # ------------------------- khung 5 bước: đo thời gian, hàng chờ kết quả -------------------------
    @contextlib.contextmanager  # biến hàm thành khối "with": chạy phần trước yield, rồi nội dung khối, rồi phần sau yield
    def buoc(self, so: int, ten: str, viec: str):  # một bước của khung 5 bước: in tiêu đề, đo thời gian, đếm bảng gửi vào hàng chờ
        self.log.info("===== BUOC %d - %s: %s =====", so, ten, viec)  # tiêu đề trong log (không dấu cho dễ đọc ở mọi Terminal)
        t = time.time()  # mốc bắt đầu bước
        n_csv, n_gold, n_ghi = len(self.csv_cho), len(self.gold_cho), len(self.da_ghi)  # số bảng trước khi vào bước
        n_job = self.so_job()  # số job Spark đã chạy trước khi vào bước
        yield  # chạy nội dung của bước (phần code bên trong khối with)
        giay = time.time() - t  # số giây bước đã chạy
        self.thoi_gian.append(dict(buoc=so, ten_buoc=ten, viec_chinh=viec, thoi_gian_giay=round(giay, 1),  # dòng thời gian của bước
                                   so_bang_csv_vao_hang_cho=len(self.csv_cho) - n_csv,  # số bảng CSV bước này tạo ra
                                   so_bang_gold_vao_hang_cho=len(self.gold_cho) - n_gold,  # số bảng Gold bước này tạo ra
                                   so_bang_parquet_da_ghi=len(self.da_ghi) - n_ghi,  # số bảng Parquet bước này đã ghi xuống đĩa/HDFS
                                   so_job_spark=self.so_job() - n_job))  # số job Spark của bước (mỗi hành động như count, ghi, toPandas sinh ít nhất 1 job)
        self.log.info("----- xong buoc %d (%s): %.1f giay", so, ten, giay)  # dòng kết thúc bước

    def so_job(self) -> int:  # tổng số job Spark đã chạy từ đầu phiên (chỉ số khối lượng công việc, không phụ thuộc máy bận hay rảnh)
        return len(self.spark.sparkContext.statusTracker().getJobIdsForGroup())  # các job không thuộc nhóm nào = mọi job của pipeline

    def xuat(self, ten: str, pdf, script: str, note: str = ""):  # Bước 1–4 KHÔNG ghi file CSV: gửi bảng vào hàng chờ, Bước 5 ghi
        self.csv_cho[ten] = (pdf.copy(), script, note)  # lưu bản sao (code phía sau có sửa bảng gốc cũng không ảnh hưởng)

    def gold(self, df, bang: str, part=None):  # gửi một bảng Gold vào hàng chờ, Bước 5 ghi (cùng thứ tự tham số với ghi)
        self.gold_cho.append((df, bang, part))  # DataFrame Spark, tên bảng, cột phân vùng (nếu có)

    def doc_cho(self, ten: str):  # đọc một bảng đang chờ ghi Y NHƯ đọc từ file CSV (để đối soát trước khi ghi)
        return pd.read_csv(io.StringIO(self.csv_cho[ten][0].to_csv(index=False)))  # ghi ra chuỗi CSV rồi đọc lại -> cùng kiểu dữ liệu với đọc file

    # ------------------------- kế hoạch thực thi, đối soát, kiểm tra -------------------------
    def ke_hoach(self, df, ten_file: str, mo_ta: str):  # ghi kế hoạch thực thi (explain formatted) của một truy vấn vào logs/
        cu = self.spark.conf.get("spark.sql.maxPlanStringLength")  # giá trị cấu hình hiện tại
        self.spark.conf.set("spark.sql.maxPlanStringLength", "100000000")  # tạm cho phép chuỗi dài để không bị cắt
        buf = io.StringIO()  # bộ đệm hứng nội dung in ra
        with contextlib.redirect_stdout(buf):  # chuyển hướng màn hình vào bộ đệm
            df.explain("formatted")  # Spark in kế hoạch thực thi
        self.spark.conf.set("spark.sql.maxPlanStringLength", cu)  # trả lại cấu hình cũ
        f = LOGS / ten_file  # file đích
        f.write_text(f"# {mo_ta}\n# Spark {self.spark.version}, ghi luc {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n" + buf.getvalue(), encoding="utf-8")  # ghi file
        self.log.info("Ke hoach thuc thi cua %s: %s", mo_ta, f.relative_to(LOGS.parent))  # in đường dẫn
        return buf.getvalue()  # trả nội dung kế hoạch

    def kiem(self, phep: str, gia_tri, doi_chieu, dat: bool, ghi_chu: str = ""):  # ghi một phép đối soát
        self.doi_soat.append(dict(phep_doi_soat=phep, spark=gia_tri, pandas_hoac_nguon=doi_chieu,  # tên phép, giá trị Spark, giá trị đối chiếu (giữ tên cột cũ)
                                  ket_qua="ĐẠT" if dat else "LỆCH", ghi_chu=ghi_chu))  # kết quả và ghi chú
        if not dat:  # nếu lệch
            self.log.warning("LECH: %s | spark=%s doi_chieu=%s", phep, gia_tri, doi_chieu)  # cảnh báo ngay

    def qa_them(self, nhom: str, kiem_tra: str, ket_qua, dat: bool, ghi_chu: str = ""):  # ghi một kiểm tra chất lượng dữ liệu
        self.qa.append(dict(nhom=nhom, kiem_tra=kiem_tra, ket_qua=ket_qua, dat="ĐẠT" if dat else "CẦN XEM", ghi_chu=ghi_chu))  # thêm dòng

    def dung(self):  # kết thúc phiên Spark
        self.log.info("Thoi gian chay: %.0f giay; %d job Spark", time.time() - self.t0, self.so_job())  # in tổng thời gian và số job
        self.spark.stop()  # dừng Spark, giải phóng tài nguyên
